/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.linkedin.kafka.cruisecontrol.model.Replica;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test fails for
 * (a) self healing if there are replicas on dead brokers after self healing.
 * (b) adding a new broker causes the replicas to move between old brokers.
 * (c) rebalance if rebalance causes a worse goal state. See {@link #executeGoalsFor} for details of pass / fail status.
 * <p>
 * Test is called from:
 * (1) {@link RandomClusterTest} with different clusters (fixed goals).
 * (2) {@link RandomGoalTest} with different goals (fixed cluster).
 * (3) {@link DeterministicClusterTest} with different balancing constraints with deterministic clusters.
 * (4) {@link RandomSelfHealingTest} with dead brokers.
 */
class OptimizationVerifier {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizationVerifier.class);

  private OptimizationVerifier() {

  }

  /**
   * Execute given goals in the given cluster enforcing the given constraint. Return pass / fail status of a test.
   * A test fails if:
   * 1) Rebalance: During the optimization process, optimization of a goal leads to a worse cluster state (in terms of
   * the requirements of the same goal) than the cluster state just before starting the optimization.
   * 2) Self Healing: There are replicas on dead brokers after self healing.
   * 3) Adding a new broker causes the replicas to move among old brokers.
   *
   * @param constraint         Balancing constraint for the given cluster.
   * @param clusterModel       The state of the cluster.
   * @param goalNameByPriority Name of goals by the order of execution priority.
   * @return Pass / fail status of a test.
   * @throws ModelInputException
   * @throws AnalysisInputException
   */
  static boolean executeGoalsFor(BalancingConstraint constraint,
                                 ClusterModel clusterModel,
                                 Map<Integer, String> goalNameByPriority)
      throws Exception {
    // Get the initial stats from the cluster.
    ClusterModelStats preOptimizedStats = clusterModel.getClusterStats(constraint);

    // Set goals by their priority.
    SortedMap<Integer, Goal> goalByPriority = new TreeMap<>();
    for (Map.Entry<Integer, String> goalEntry : goalNameByPriority.entrySet()) {
      Integer priority = goalEntry.getKey();
      String goalClassName = goalEntry.getValue();

      Class<? extends Goal> goalClass = (Class<? extends Goal>) Class.forName(goalClassName);
      try {
        Constructor<? extends Goal> constructor = goalClass.getDeclaredConstructor(BalancingConstraint.class);
        constructor.setAccessible(true);
        goalByPriority.put(priority, constructor.newInstance(constraint));
      } catch (NoSuchMethodException badConstructor) {
        //Try default constructor
        goalByPriority.put(priority, goalClass.newInstance());
      }
    }

    // Generate the goalOptimizer and optimize given goals.
    long startTime = System.currentTimeMillis();
    Properties props = CruiseControlUnitTestUtils.getCruiseControlProperties();
    GoalOptimizer goalOptimizer = new GoalOptimizer(new KafkaCruiseControlConfig(constraint.setProps(props)),
                                                    null,
                                                    new SystemTime(),
                                                    new MetricRegistry());
    GoalOptimizer.OptimizerResult optimizerResult = goalOptimizer.optimizations(clusterModel, goalByPriority);
    LOG.trace("Took {} ms to execute {} to generate {} proposals.", System.currentTimeMillis() - startTime,
              goalByPriority, optimizerResult.goalProposals().size());

    // Check whether test has failed for self healing: fails if there are replicas on dead brokers after self healing.
    if (!clusterModel.selfHealingEligibleReplicas().isEmpty()) {
      Set<Broker> deadBrokers = clusterModel.brokers();
      deadBrokers.removeAll(clusterModel.healthyBrokers());
      for (Broker deadBroker : deadBrokers) {
        if (deadBroker.replicas().size() > 0) {
          LOG.error("Failed to move {} replicas on dead broker {} to other brokers.", deadBroker.replicas().size(),
                    deadBroker.id());
          return false;
        }
      }
    } else {
      // Check whether test has failed for rebalance: fails if rebalance caused a worse goal state after rebalance.
      Map<Goal, ClusterModelStats> clusterStatsByPriority = optimizerResult.statsByGoalPriority();
      for (Map.Entry<Goal, ClusterModelStats> entry : clusterStatsByPriority.entrySet()) {
        Goal.ClusterModelStatsComparator comparator = entry.getKey().clusterModelStatsComparator();
        boolean success = comparator.compare(entry.getValue(), preOptimizedStats) >= 0;
        if (!success) {
          LOG.error("Failed goal comparison " + entry.getKey().name() + ". " + comparator.explainLastComparison());
          return false;
        }
        preOptimizedStats = entry.getValue();
      }
    }

    // When there are new brokers, ensure replicas are only moved from the old brokers to the new broker.
    if (!clusterModel.newBrokers().isEmpty()) {
      for (Broker broker : clusterModel.healthyBrokers()) {
        if (!broker.isNew()) {
          for (Replica replica : broker.replicas()) {
            if (replica.originalBroker() != broker) {
              LOG.error("Broker {} is not a new broker but has received new replicas", broker.id());
              return false;
            }
          }
        }
      }
      for (Broker broker : clusterModel.newBrokers()) {
        // We can only check the first resource.
        Resource r = constraint.resources().get(0);
        double utilizationLowerThreshold =
            clusterModel.load().expectedUtilizationFor(r) / clusterModel.capacityFor(r) * (2 - constraint.balancePercentage(r));
        double brokerUtilization = broker.load().expectedUtilizationFor(r) / broker.capacityFor(r);
        if (brokerUtilization < utilizationLowerThreshold) {
          LOG.error("Broker {} is still under utilized for resource {}. Broker utilization is {}, the "
                        + "lower threshold is {}", broker, r, brokerUtilization, utilizationLowerThreshold);
          return false;
        }
      }
    }

    return true;
  }
}
