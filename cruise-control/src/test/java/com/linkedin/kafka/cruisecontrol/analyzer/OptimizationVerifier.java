/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;

import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.easymock.EasyMock;
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
   * @param verifications      The verifications to make after the optimization.
   * @return Pass / fail status of a test.
   */
  static boolean executeGoalsFor(BalancingConstraint constraint,
                                 ClusterModel clusterModel,
                                 List<String> goalNameByPriority,
                                 List<Verification> verifications) throws Exception {
    return executeGoalsFor(constraint, clusterModel, goalNameByPriority, Collections.emptySet(), verifications);
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
   * @param excludedTopics     The excluded topics.
   * @param verifications      The verifications to make after the optimization.
   * @return Pass / fail status of a test.
   */
  @SuppressWarnings("unchecked")
  static boolean executeGoalsFor(BalancingConstraint constraint,
                                 ClusterModel clusterModel,
                                 List<String> goalNameByPriority,
                                 Collection<String> excludedTopics,
                                 List<Verification> verifications) throws Exception {
    // Get the initial stats from the cluster.
    ClusterModelStats preOptimizedStats = clusterModel.getClusterStats(constraint);

    // Set goals by their priority.
    List<Goal> goalByPriority = new ArrayList<>(goalNameByPriority.size());
    for (String goalClassName : goalNameByPriority) {
      Class<? extends Goal> goalClass = (Class<? extends Goal>) Class.forName(goalClassName);
      try {
        Constructor<? extends Goal> constructor = goalClass.getDeclaredConstructor(BalancingConstraint.class);
        constructor.setAccessible(true);
        goalByPriority.add(constructor.newInstance(constraint));
      } catch (NoSuchMethodException badConstructor) {
        //Try default constructor
        goalByPriority.add(goalClass.newInstance());
      }
    }

    // Generate the goalOptimizer and optimize given goals.
    long startTime = System.currentTimeMillis();
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    StringJoiner stringJoiner = new StringJoiner(",");
    excludedTopics.forEach(stringJoiner::add);
    props.setProperty(KafkaCruiseControlConfig.TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG, stringJoiner.toString());
    GoalOptimizer goalOptimizer = new GoalOptimizer(new KafkaCruiseControlConfig(constraint.setProps(props)),
                                                    null,
                                                    new SystemTime(),
                                                    new MetricRegistry(),
                                                    EasyMock.mock(Executor.class));
    // First optimize with all hard goals if there is any.
    List<Goal> hardGoals =  goalByPriority.stream().filter(Goal::isHardGoal).collect(Collectors.toList());
    GoalOptimizer.OptimizerResult optimizerResult1 = hardGoals.isEmpty() ? null
                                                                         : goalOptimizer.optimizations(clusterModel,
                                                                                                       hardGoals,
                                                                                                       new OperationProgress());

    // If provided goal list contains any soft goal, then optimize with all goals.
    // In this iteration, all the hard goals will not generate any new proposal since they are already satisfied in previous
    // iteration (otherwise an exception has already been thrown). The optimize result of this iteration will only contains
    // proposals generated by soft goals.
    List<Goal> softGoals = goalByPriority.stream().filter(g -> !g.isHardGoal()).collect(Collectors.toList());
    GoalOptimizer.OptimizerResult optimizerResult2 = softGoals.isEmpty() ? null
                                                                         : goalOptimizer.optimizations(clusterModel,
                                                                                                       goalByPriority,
                                                                                                       new OperationProgress());
    if (LOG.isTraceEnabled()) {
      LOG.trace("Took {} ms to execute {} to generate {} proposals.", System.currentTimeMillis() - startTime,
                goalByPriority,
                (optimizerResult1 == null ? 0 : optimizerResult1.goalProposals().size()) +
                (optimizerResult2 == null ? 0 : optimizerResult2.goalProposals().size()));
    }

    for (Verification verification : verifications) {
      switch (verification) {
        case GOAL_VIOLATION:
          if ((optimizerResult1 != null && !verifyGoalViolations(optimizerResult1))
              || (optimizerResult2 != null && !verifyGoalViolations(optimizerResult2))) {
            return false;
          }
          break;
        case NEW_BROKERS:
          if (!clusterModel.newBrokers().isEmpty() && !verifyNewBrokers(clusterModel, constraint)) {
            return false;
          }
          break;
        case DEAD_BROKERS:
          SortedSet<Broker> deadBrokers = clusterModel.deadBrokers();
          if (!clusterModel.deadBrokers().isEmpty()) {
            if (!verifyDeadBrokers(clusterModel)) {
              return false;
            }
            // Ensure that soft goals only move offline replicas or immigrant replicas.
            if (optimizerResult2 != null
                && !verifySoftGoalReplicaMovements(optimizerResult1, optimizerResult2, deadBrokers, softGoals)) {
              return false;
            }
          }
          break;
        case REGRESSION:
          if (clusterModel.selfHealingEligibleReplicas().isEmpty()
              && ((optimizerResult1 != null && !verifyRegression(optimizerResult1, preOptimizedStats))
              || (optimizerResult2 != null && !verifyRegression(optimizerResult2, preOptimizedStats)))) {
            return false;
          }
          break;
        default:
          throw new IllegalStateException("Invalid verification " + verification);
      }
    }
    return true;
  }

  private static boolean verifyGoalViolations(GoalOptimizer.OptimizerResult optimizerResult) {
    // Check if there are still goals violated after the optimization.
    if (!optimizerResult.violatedGoalsAfterOptimization().isEmpty()) {
      LOG.error("Failed to optimize goal {}", optimizerResult.violatedGoalsAfterOptimization());
      System.out.println(optimizerResult.clusterModelStats().toString());
      return false;
    } else {
      return true;
    }
  }

  private static boolean verifyDeadBrokers(ClusterModel clusterModel) {
    Set<Broker> deadBrokers = clusterModel.brokers();
    deadBrokers.removeAll(clusterModel.aliveBrokers());
    for (Broker deadBroker : deadBrokers) {
      if (deadBroker.replicas().size() > 0) {
        LOG.error("Failed to move {} replicas on dead broker {} to other brokers.", deadBroker.replicas().size(),
                  deadBroker.id());
        return false;
      }
    }
    return true;
  }

  private static boolean verifySoftGoalReplicaMovements(GoalOptimizer.OptimizerResult optimizerResultOfHardGoals,
                                                        GoalOptimizer.OptimizerResult optimizerResultOfSoftGoals,
                                                        SortedSet<Broker> deadBrokers,
                                                        List<Goal> softGoals) {
    // Populate dead brokers.
    Set<Integer> deadBrokerIds = new HashSet<>(deadBrokers.size());
    deadBrokers.forEach(b -> deadBrokerIds.add(b.id()));

    // Populate replicas moved by hard goals, these replicas will be treated as immigrant replicas when soft goals calculating
    // execution proposals.
    Map<TopicPartition, Set<Integer>> replicasMovedByHardGoals = new HashMap<>();
    if (optimizerResultOfHardGoals != null) {
      optimizerResultOfHardGoals.goalProposals().forEach(p -> replicasMovedByHardGoals.put(p.topicPartition(), p.replicasToAdd()));
    }

    for (ExecutionProposal p : optimizerResultOfSoftGoals.goalProposals()) {
      for (Integer r : p.replicasToRemove()) {
        // Soft goals should only generate proposals that
        // (1) move offline replicas from dead brokers
        // (2) move immigrant replicas from alive brokers
        if (!deadBrokerIds.contains(r) && !replicasMovedByHardGoals.get(p.topicPartition()).contains(r)) {
          LOG.error("Self-healing replica movement generated by soft goals must be limited to only moving offline or immigrant replicas."
                    + " Proposal violated: {}, goals used: {}).", p, softGoals);
          return false;
        }
      }
    }
    return true;
  }

  private static boolean verifyNewBrokers(ClusterModel clusterModel, BalancingConstraint constraint) {
    for (Broker broker : clusterModel.aliveBrokers()) {
      if (!broker.isNew()) {
        for (Replica replica : broker.replicas()) {
          if (replica.originalBroker() != broker) {
            LOG.error("Broker {} is not a new broker but has received new replicas", broker.id());
            return false;
          }
        }
      }
    }
    // Since the first distribution goal is ReplicaDistributionGoal, we can check the replica count on new brokers are
    // in valid range.
    int replicaCountLowerThreshold = (int) Math.floor((double) clusterModel.numReplicas() / clusterModel.aliveBrokers().size()
                                                      * Math.max(0, (1 - constraint.replicaBalancePercentage())));
    for (Broker broker : clusterModel.newBrokers()) {
      if (broker.replicas().size() < replicaCountLowerThreshold) {
        LOG.error(" New Broker {} has only {} replicas, which is smaller than lower threshold is {}.",
                  broker, broker.replicas().size(), replicaCountLowerThreshold);
        return false;
      }
    }
    return true;
  }

  private static boolean verifyRegression(GoalOptimizer.OptimizerResult optimizerResult,
                                          ClusterModelStats preOptimizationStats) {
    // Check whether test has failed for rebalance: fails if rebalance caused a worse goal state after rebalance.
    Map<String, ClusterModelStats> statsByGoalName = optimizerResult.statsByGoalName();
    Map<String, Goal.ClusterModelStatsComparator> clusterModelStatsComparatorByGoalName
        = optimizerResult.clusterModelStatsComparatorByGoalName();
    ClusterModelStats preStats = preOptimizationStats;
    for (Map.Entry<String, ClusterModelStats> entry : statsByGoalName.entrySet()) {
      Goal.ClusterModelStatsComparator comparator = clusterModelStatsComparatorByGoalName.get(entry.getKey());
      boolean success = comparator.compare(entry.getValue(), preStats) >= 0;
      if (!success) {
        LOG.error("Failed goal comparison " + entry.getKey() + ". " + comparator.explainLastComparison());
        return false;
      }
      preStats = entry.getValue();
    }
    return true;
  }

  enum Verification {
    GOAL_VIOLATION, DEAD_BROKERS, NEW_BROKERS, REGRESSION,
  }
}
