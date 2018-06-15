/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;

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

import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;


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
   * @param initialReplicaDistribution Initial distribution of replicas over the cluster.
   * @param initialLeaderDistribution Initial distribution of the leaders.
   * @param optimizedClusterModel The optimized cluster model.
   * @return The diff represented by the set of balancing proposals to move from initial to final distribution.
   */
  public static Set<ExecutionProposal> getDiff(Map<TopicPartition, List<Integer>> initialReplicaDistribution,
                                               Map<TopicPartition, Integer> initialLeaderDistribution,
                                               ClusterModel optimizedClusterModel) {
    Map<TopicPartition, List<Integer>> finalDistribution = optimizedClusterModel.getReplicaDistribution();
    // Sanity check to make sure that given distributions contain the same replicas.
    if (!initialReplicaDistribution.keySet().equals(finalDistribution.keySet())) {
      throw new IllegalArgumentException("Attempt to diff distributions with different partitions.");
    }
    for (Map.Entry<TopicPartition, List<Integer>> entry : initialReplicaDistribution.entrySet()) {
      TopicPartition tp = entry.getKey();
      List<Integer> initialReplicas = entry.getValue();
      if (finalDistribution.get(tp).size() != initialReplicas.size()) {
        throw new IllegalArgumentException("Attempt to diff distributions with modified replication factor.");
      }
    }

    // Generate a set of execution proposals to represent the diff between initial and final distribution.
    Set<ExecutionProposal> diff = new HashSet<>();
    for (Map.Entry<TopicPartition, List<Integer>> entry : initialReplicaDistribution.entrySet()) {
      TopicPartition tp = entry.getKey();
      List<Integer> initialReplicas = entry.getValue();
      List<Integer> finalReplicas = finalDistribution.get(tp);
      int finalLeaderId = optimizedClusterModel.partition(tp).leader().broker().id();
      // The partition has no change.
      if (finalReplicas.equals(initialReplicas) && finalLeaderId == initialLeaderDistribution.get(tp)) {
        continue;
      }
      // We need to adjust the final broker list order to ensure the final leader is the first replica.
      if (finalLeaderId != finalReplicas.get(0)) {
        int leaderPos = finalReplicas.indexOf(finalLeaderId);
        finalReplicas.set(leaderPos, finalReplicas.get(0));
        finalReplicas.set(0, finalLeaderId);
      }
      Double partitionSize = optimizedClusterModel.partition(tp).leader().load().expectedUtilizationFor(Resource.DISK);
      diff.add(new ExecutionProposal(tp, partitionSize.intValue(), initialLeaderDistribution.get(tp),
                                     initialReplicas, finalReplicas));
    }
    return diff;
  }

  /**
   * Check whether the given proposal is acceptable for all of the given optimized goals.
   *
   * @param optimizedGoals Optimized goals to check whether they accept the given proposal.
   * @param proposal       Proposal to be checked for acceptance.
   * @param clusterModel   The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the given proposal is acceptable for all the given optimized goals, the
   * reject flag (e.g. {@link ActionAcceptance#REPLICA_REJECT}, {@link ActionAcceptance#BROKER_REJECT}) otherwise.
   */
  public static ActionAcceptance isProposalAcceptableForOptimizedGoals(Set<Goal> optimizedGoals,
                                                                       BalancingAction proposal,
                                                                       ClusterModel clusterModel) {
    for (Goal optimizedGoal : optimizedGoals) {
      ActionAcceptance actionAcceptance = optimizedGoal.actionAcceptance(proposal, clusterModel);
      if (actionAcceptance != ACCEPT) {
        return actionAcceptance;
      }
    }
    return ACCEPT;
  }

  /**
   * Checks the replicas that are supposed to be moved away from the dead brokers. If there are still replicas
   * on the dead broker, throw exception.
   * @param clusterModel the cluster model to check.
   * @throws OptimizationFailureException when there are still replicas on the dead broker.
   */
  public static void ensureNoReplicaOnDeadBrokers(ClusterModel clusterModel) throws OptimizationFailureException {
    // Sanity check: No self-healing eligible replica should remain at a decommissioned broker.
    for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
      if (!replica.broker().isAlive()) {
        throw new OptimizationFailureException(String.format(
            "Self healing failed to move the replica %s away from decommissioned broker %d for goal. There are still "
                + "%d replicas on the broker.",
            replica, replica.broker().id(), replica.broker().replicas().size()));
      }
    }
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   *
   * @param clusterModelStats Cluster model stats.
   */
  public static Map<String, Object> getJsonStructure(ClusterModelStats clusterModelStats) {
    Map<String, Object> clusterStatsMap = new HashMap<>();

    clusterStatsMap.put("brokers", clusterModelStats.numBrokers());
    clusterStatsMap.put("replicas", clusterModelStats.numReplicasInCluster());
    clusterStatsMap.put("topics", clusterModelStats.numTopics());

    Map<Statistic, Map<Resource, Double>> resourceUtilizationStats = clusterModelStats.resourceUtilizationStats();
    Map<Statistic, Double> nwOutUtilizationStats = clusterModelStats.potentialNwOutUtilizationStats();
    Map<Statistic, Number> replicaStats = clusterModelStats.replicaStats();
    Map<Statistic, Number> topicReplicaStats = clusterModelStats.topicReplicaStats();

    Map<String, Object> statisticMap = new HashMap<>();

    for (Statistic stat : Statistic.values()) {
      Map<String, Double> resourceMap = new HashMap<>();

      for (Resource resource : Resource.cachedValues()) {
        resourceMap.put(resource.resource(), resourceUtilizationStats.get(stat).get(resource));
      }

      resourceMap.put("potentialNwOut", nwOutUtilizationStats.get(stat));
      resourceMap.put("replicas", replicaStats.get(stat).doubleValue());
      resourceMap.put("topicReplicas",  topicReplicaStats.get(stat).doubleValue());

      statisticMap.put(stat.stat(), resourceMap);
    }

    clusterStatsMap.put("statistics", statisticMap);

    return clusterStatsMap;
  }

  /**
   * Compare the given values. Return 1 if first &gt; second, -1 if first &lt; second, 0 otherwise.
   *
   * @param d1 The first {@code double} to compare.
   * @param d2 The second {@code double} to compare.
   * @param resource the resource the current comparison is for.
   * @return 1 if first &gt; second, -1 if first &lt; second, 0 otherwise.
   */
  public static int compare(double d1, double d2, Resource resource) {
    double epsilon = resource.epsilon(d1, d2);
    return compare(d1, d2, epsilon);
  }

  /**
   * Compare the given values. Return 1 if first &gt; second, -1 if first &lt; second, 0 otherwise.
   *
   * @param d1 The first {@code double} to compare.
   * @param d2 The second {@code double} to compare.
   * @return 1 if first &gt; second, -1 if first &lt; second, 0 otherwise.
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
    List<String> defaultGoalsConfig = config.getList(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG);
    List<Goal> goals;
    if (defaultGoalsConfig == null || defaultGoalsConfig.isEmpty()) {
      // Default goals config not set or it is empty, use all the goals.
      goals = config.getConfiguredInstances(KafkaCruiseControlConfig.GOALS_CONFIG, Goal.class);
    } else {
      // Use the provided default goals config.
      goals = config.getConfiguredInstances(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG, Goal.class);
    }
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
  public static Map<String, Goal> getCaseInsensitiveGoalsByName(KafkaCruiseControlConfig config) {
    List<Goal> goals = config.getConfiguredInstances(KafkaCruiseControlConfig.GOALS_CONFIG, Goal.class);
    Map<String, Goal> caseInsensitiveGoalsByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (Goal goal: goals) {
      caseInsensitiveGoalsByName.put(goal.name(), goal);
    }
    return caseInsensitiveGoalsByName;
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

  /*
   * JSON does not support literal NaN value
   * round it to zero when Java Math sees a NaN
   */
  public static double nanToZero(double v) {
      if (Double.isNaN(v)) {
          return 0.0;
      } else {
          return v;
      }
  }

  /**
   * Calculate the factorial of the given non-negative value.
   *
   * @param v The value for which the factorial will be calculated.
   * @return the factorial of the given non-negative value.
   */
  public static int factorial(int v) {
    if (v < 0) {
      throw new IllegalArgumentException("Attempt to calculate factorial of a non-negative value.");
    } else {
      int res = 1;
      for (int i = 2; i <= v; i++) {
        res *= i;
      }
      return res;
    }
  }

  /**
   * Get all permutations of the given list of goals to permute.
   *
   * @param toPermute List of goals to permute.
   * @return A set containing all possible permutations of the given list of goals to permute.
   */
  public static Set<List<Goal>> getPermutations(List<Goal> toPermute) {
    Set<List<Goal>> allPermutations = new HashSet<>();
    // Handle the case with single goal to permute.
    if (toPermute.size() == 1) {
      allPermutations.add(toPermute);
      return allPermutations;
    }

    for (int i = 0; i < toPermute.size(); i++) {
      // Copy the original list and remove the goal that we will prepend to the permutations of the remaining goals.
      List<Goal> remainingToPermute = new ArrayList<>(toPermute);
      Goal goal = toPermute.get(i);
      remainingToPermute.remove(i);

      // Prepend the goal to permutations of the remaining goals.
      for (List<Goal> permutedRemaining: getPermutations(remainingToPermute)) {
        permutedRemaining.add(0, goal);
        allPermutations.add(permutedRemaining);
      }
    }

    return allPermutations;
  }
}
