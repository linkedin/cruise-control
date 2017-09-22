/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import com.linkedin.kafka.cruisecontrol.model.RawAndDerivedResource;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.apache.kafka.common.TopicPartition;


/**
 * A util class for Analyzer.
 */
public class AnalyzerUtils {
  public final static double EPSILON = 1E-5;

  private AnalyzerUtils() {

  }

  /**
   * Get the diff represented by the set of balancing proposals to move from initial to final distribution.
   *
   * @param initialDistribution Initial distribution of replicas over the cluster.
   * @param optimizedClusterModel The optimized cluster model.
   * @return The diff represented by the set of balancing proposals to move from initial to final distribution.
   * @throws AnalysisInputException
   */
  public static Set<BalancingProposal> getDiff(Map<TopicPartition, List<Integer>> initialDistribution,
                                               ClusterModel optimizedClusterModel)
      throws AnalysisInputException, ModelInputException {
    Map<TopicPartition, List<Integer>> finalDistribution = optimizedClusterModel.getReplicaDistribution();
    // Sanity check to make sure that given distributions contain the same replicas.
    if (!initialDistribution.keySet().equals(finalDistribution.keySet())) {
      throw new AnalysisInputException("Attempt to diff distributions with different partitions.");
    }
    for (Map.Entry<TopicPartition, List<Integer>> entry : initialDistribution.entrySet()) {
      TopicPartition tp = entry.getKey();
      List<Integer> initialReplicas = entry.getValue();
      if (finalDistribution.get(tp).size() != initialReplicas.size()) {
        throw new AnalysisInputException("Attempt to diff distributions with modified replication factor.");
      }
    }

    // Generate a set of balancing proposals to represent the diff between initial and final distribution.
    Set<BalancingProposal> diff = new HashSet<>();
    for (Map.Entry<TopicPartition, List<Integer>> entry : initialDistribution.entrySet()) {
      TopicPartition tp = entry.getKey();
      List<Integer> initialBrokerIds = entry.getValue();
      List<Integer> finalBrokerIds = finalDistribution.get(tp);

      // Get order preserving difference of broker ids.
      List<Integer> initialBrokerIdDiff = new ArrayList<>();
      List<Integer> finalBrokerIdDiff = new ArrayList<>(finalBrokerIds);
      initialBrokerIdDiff.addAll(initialBrokerIds.stream()
          .filter(brokerId -> !finalBrokerIdDiff.remove(brokerId))
          .collect(Collectors.toList()));

      // Generate replica movement proposals (if needed).
      for (int i = 0; i < initialBrokerIdDiff.size(); i++) {
        Double partitionSize =
            optimizedClusterModel.partition(tp).leader().load().expectedUtilizationFor(Resource.DISK);
        BalancingProposal replicaMovementProposal =
            new BalancingProposal(tp, initialBrokerIdDiff.get(i), finalBrokerIdDiff.get(i),
                                  BalancingAction.REPLICA_MOVEMENT, partitionSize.intValue());
        diff.add(replicaMovementProposal);
      }

      // Get the initial leader and final leader broker ids.
      int initialLeaderBrokerId = initialBrokerIds.get(0);
      int finalLeaderBrokerId = finalBrokerIds.get(0);

      // Generate leadership movement proposal (if needed).
      if (initialLeaderBrokerId != finalLeaderBrokerId && !initialBrokerIdDiff.contains(initialLeaderBrokerId)) {
        BalancingProposal leadershipMovementProposal =
            new BalancingProposal(tp, initialLeaderBrokerId, finalLeaderBrokerId, BalancingAction.LEADERSHIP_MOVEMENT);
        diff.add(leadershipMovementProposal);
      }
    }
    return diff;
  }

  /**
   * Check whether the given proposal is acceptable for all of the given optimized goals.
   *
   * @param optimizedGoals Optimized goals to check whether they accept the given proposal.
   * @param proposal       Proposal to be checked for acceptance.
   * @param clusterModel   The state of the cluster.
   * @return True if the the given proposal is acceptable for all of the given optimized goals, false otherwise.
   */
  public static boolean isProposalAcceptableForOptimizedGoals(Set<Goal> optimizedGoals,
                                                              BalancingProposal proposal,
                                                              ClusterModel clusterModel) {
    for (Goal optimizedGoal : optimizedGoals) {
      if (!optimizedGoal.isProposalAcceptable(proposal, clusterModel)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks the replicas that are supposed to be moved away from the dead brokers. If there are still replicas
   * on the dead broker, throw exception.
   * @param clusterModel the cluster model to check.
   * @throws AnalysisInputException when there are still replicas on the dead broker.
   */
  public static void ensureNoReplicaOnDeadBrokers(ClusterModel clusterModel) throws AnalysisInputException {
    // Sanity check: No self-healing eligible replica should remain at a decommissioned broker.
    for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
      if (!replica.broker().isAlive()) {
        throw new AnalysisInputException(String.format(
            "Self healing failed to move the replica away from decommissioned broker %d for goal",
            replica.broker().id()));
      }
    }
  }

  /**
   * Compare the given values. Return 1 if first > second, -1 if first < second, 0 otherwise.
   *
   * @param d1 The first {@code double} to compare.
   * @param d2 The second {@code double} to compare.
   * @param resource the resource the current comparison is for.
   * @return 1 if first > second, -1 if first < second, 0 otherwise.
   */
  public static int compare(double d1, double d2, Resource resource) {
    double epsilon = resource.epsilon(d1, d2);
    return compare(d1, d2, epsilon);
  }

  /**
   * Compare the given values. Return 1 if first > second, -1 if first < second, 0 otherwise.
   *
   * @param d1 The first {@code double} to compare.
   * @param d2 The second {@code double} to compare.
   * @return 1 if first > second, -1 if first < second, 0 otherwise.
   */
  public static int compare(double d1, double d2, double epsilon) {
    if (d2 - d1 > epsilon) {
      return -1;  // Second value is larger than the first value.
    }
    if (d1 - d2 > epsilon) {
      return 1;   // First value is larger than the second value.
    }
    // Given values are approximately equal.
    return 0;
  }

  /**
   * Get a priority to goal mapping. This is a default mapping.
   */
  public static SortedMap<Integer, Goal> getGoalMapByPriority(KafkaCruiseControlConfig config) {
    List<Goal> goals = config.getConfiguredInstances(KafkaCruiseControlConfig.GOALS_CONFIG, Goal.class);
    SortedMap<Integer, Goal> orderedGoals = new TreeMap<>();
    int i = 0;
    for (Goal goal: goals) {
      orderedGoals.put(i++, goal);
    }
    return orderedGoals;
  }

  /**
   * Get a goal map with goal name as the keys.
   */
  public static Map<String, Goal> getGoalsMapByName(KafkaCruiseControlConfig config) {
    List<Goal> goals = config.getConfiguredInstances(KafkaCruiseControlConfig.GOALS_CONFIG, Goal.class);
    Map<String, Goal> goalsByName = new HashMap<>();
    for (Goal goal: goals) {
      goalsByName.put(goal.name(), goal);
    }
    return goalsByName;
  }

 /**
   * Test if two clusters are significantly different in the metrics we look at for balancing.
   *
   * @param orig the utilization matrix from the original cluster
   * @param optimized the utilization matrix from the optimized cluster
   * @return The P value that the various derived resources come from the same probability distribution.  The probability
   * that the null hypothesis is correct.
   */
  public static double[] testDifference(double[][] orig, double[][] optimized) {
    int nResources = RawAndDerivedResource.values().length;
    if (orig.length != nResources) {
      throw new IllegalArgumentException("orig must have number of rows equal to RawAndDerivedResource.");
    }
    if (optimized.length != nResources) {
      throw new IllegalArgumentException("optimized must have number of rows equal to RawAndDerivedResource.");
    }
    if (orig[0].length != optimized[0].length) {
      throw new IllegalArgumentException("The number of brokers must be the same.");
    }

    double[] pValues = new double[orig.length];

    //TODO:  For small N we want to do statistical bootstrapping (not the same as bootstrapping data).
    for (int resourceIndex = 0; resourceIndex < nResources; resourceIndex++) {
      RandomGenerator rng = new MersenneTwister(0x5d11121018463324L);
      KolmogorovSmirnovTest kolmogorovSmirnovTest = new KolmogorovSmirnovTest(rng);
      pValues[resourceIndex] =
          kolmogorovSmirnovTest.kolmogorovSmirnovTest(orig[resourceIndex], optimized[resourceIndex]);
    }

    return pValues;
  }

}
