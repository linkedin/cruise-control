/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoals;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.ClusterProperty;
import com.linkedin.kafka.cruisecontrol.common.RandomCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;


/**
 * Unit test for testing with different goals and fixed cluster properties.
 */
@RunWith(Parameterized.class)
public class RandomGoalTest {
  private static final Logger LOG = LoggerFactory.getLogger(RandomGoalTest.class);

  private final static Random RANDOM = new Random(34534534);

  /**
   * Populate parameters for the {@link OptimizationVerifier}. All brokers are alive.
   *
   * @return Parameters for the {@link OptimizationVerifier}.
   */
  @Parameters
  public static Collection<Object[]> data() throws Exception {
    int goalRepetition = 4;
    Collection<Object[]> params = new ArrayList<>();

    Map<Integer, String> goalNameByPriority = new HashMap<>();
    goalNameByPriority.put(1, RackAwareCapacityGoal.class.getName());
    goalNameByPriority.put(2, PotentialNwOutGoal.class.getName());
    goalNameByPriority.put(3, TopicReplicaDistributionGoal.class.getName());
    goalNameByPriority.put(4, DiskUsageDistributionGoal.class.getName());
    goalNameByPriority.put(5, NetworkInboundUsageDistributionGoal.class.getName());
    goalNameByPriority.put(6, NetworkOutboundUsageDistributionGoal.class.getName());
    goalNameByPriority.put(7, CpuUsageDistributionGoal.class.getName());
    goalNameByPriority.put(8, LeaderBytesInDistributionGoals.class.getName());
    goalNameByPriority.put(9, ReplicaDistributionGoal.class.getName());

    KafkaCruiseControlConfig config =
        new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    BalancingConstraint balancingConstraint = new BalancingConstraint(config);
    balancingConstraint.setBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    // Test: Single goal at a time.
    for (Map.Entry<Integer, String> entry: goalNameByPriority.entrySet()) {
      Map<Integer, String> singletonGoalNameByPriority = Collections.singletonMap(entry.getKey(), entry.getValue());
      Object[] singleGoalParams = {Collections.emptyMap(), singletonGoalNameByPriority, balancingConstraint};
      params.add(singleGoalParams);
    }

    // Test: Consecutive repetition of the same goal (goalRepetition times each).
    int goalPriority = 1;
    for (String goalName : goalNameByPriority.values()) {
      Map<Integer, String> repeatedGoalNamesByPriority = new HashMap<>();
      for (int i = 0; i < goalRepetition; i++) {
        repeatedGoalNamesByPriority.put(goalPriority, goalName);
        goalPriority++;
      }
      Object[] consecutiveRepetitionParams = {Collections.emptyMap(), repeatedGoalNamesByPriority, balancingConstraint};
      params.add(consecutiveRepetitionParams);
    }
    // Test: Nested repetition of the same goal (goalRepetition times each).
    goalPriority = 1;
    Map<Integer, String> nonRepetitiveGoalNamesByPriority = new HashMap<>();
    for (int i = 0; i < goalRepetition; i++) {
      for (String goalName : goalNameByPriority.values()) {
        nonRepetitiveGoalNamesByPriority.put(goalPriority, goalName);
        goalPriority++;
      }
    }
    Object[] nestedRepetitionParams = {Collections.emptyMap(), nonRepetitiveGoalNamesByPriority, balancingConstraint};
    params.add(nestedRepetitionParams);

    // Test: No goal.
    Object[] noGoalParams = {Collections.emptyMap(), Collections.emptyMap(), balancingConstraint};
    params.add(noGoalParams);

    // Test shuffled soft goals.
    List<String> shuffledSoftGoalNames = new ArrayList<>(goalNameByPriority.values());
    shuffledSoftGoalNames.remove(RackAwareCapacityGoal.class.getName());    // Remove the hard goal.
    Collections.shuffle(shuffledSoftGoalNames, RANDOM);

    int priority = 1;
    Map<Integer, String> randomOrderedSoftGoalsByPriority = new HashMap<>();
    for (String goalName : shuffledSoftGoalNames) {
      randomOrderedSoftGoalsByPriority.put(priority, goalName);
      priority++;
    }
    Object[] randomOrderedSoftGoalsParams = {Collections.emptyMap(), randomOrderedSoftGoalsByPriority, balancingConstraint};
    params.add(randomOrderedSoftGoalsParams);

    return params;
  }

  private Map<ClusterProperty, Number> _modifiedProperties;
  private Map<Integer, String> _goalNameByPriority;
  private BalancingConstraint _balancingConstraint;

  /**
   * Constructor of Random Goal Test.
   *
   * @param modifiedProperties Modified cluster properties over the {@link TestConstants#BASE_PROPERTIES}.
   * @param goalNameByPriority Goal name by priority.
   */
  public RandomGoalTest(Map<ClusterProperty, Number> modifiedProperties,
                        Map<Integer, String> goalNameByPriority,
                        BalancingConstraint balancingConstraint) {
    _modifiedProperties = modifiedProperties;
    _goalNameByPriority = goalNameByPriority;
    _balancingConstraint = balancingConstraint;
  }

  @Test
  public void test() throws Exception {
    // Create cluster properties by applying modified properties to base properties.
    Map<ClusterProperty, Number> clusterProperties = new HashMap<>(TestConstants.BASE_PROPERTIES);
    clusterProperties.putAll(_modifiedProperties);

    LOG.debug("Replica distribution: {}.", TestConstants.Distribution.EXPONENTIAL);
    ClusterModel clusterModel = RandomCluster.generate(clusterProperties);
    RandomCluster.populate(clusterModel, clusterProperties, TestConstants.Distribution.EXPONENTIAL);

    assertTrue("Random Goal Test failed to improve the existing state.",
        OptimizationVerifier.executeGoalsFor(_balancingConstraint, clusterModel, _goalNameByPriority));
  }
}
