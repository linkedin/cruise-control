/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
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
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.is;


@RunWith(Parameterized.class)
public class RandomSelfHealingTest {
  private static final Logger LOG = LoggerFactory.getLogger(RandomSelfHealingTest.class);

  /**
   * Populate parameters for the {@link OptimizationVerifier}. All brokers are alive.
   *
   * @return Parameters for the {@link OptimizationVerifier}.
   */
  @Parameters
  public static Collection<Object[]> data() throws AnalysisInputException {
    Collection<Object[]> params = new ArrayList<>();

    Map<Integer, String> hardGoalNameByPriority = new HashMap<>();
    hardGoalNameByPriority.put(1, RackAwareCapacityGoal.class.getName());

    Map<Integer, String> softGoalNameByPriority = new HashMap<>();
    softGoalNameByPriority.put(2, PotentialNwOutGoal.class.getName());
    softGoalNameByPriority.put(3, DiskUsageDistributionGoal.class.getName());
    softGoalNameByPriority.put(4, NetworkInboundUsageDistributionGoal.class.getName());
    softGoalNameByPriority.put(5, NetworkOutboundUsageDistributionGoal.class.getName());
    softGoalNameByPriority.put(6, CpuUsageDistributionGoal.class.getName());
    softGoalNameByPriority.put(7, TopicReplicaDistributionGoal.class.getName());
    softGoalNameByPriority.put(8, ReplicaDistributionGoal.class.getName());

    KafkaCruiseControlConfig config =
        new KafkaCruiseControlConfig(CruiseControlUnitTestUtils.getCruiseControlProperties());
    BalancingConstraint balancingConstraint = new BalancingConstraint(config);
    balancingConstraint.setBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    Map<ClusterProperty, Number> modifiedProperties = new HashMap<>();

    // -- TEST DECK #1: SINGLE DEAD BROKER.
    // Test: Single Soft Goal.
    modifiedProperties.put(ClusterProperty.NUM_DEAD_BROKERS, 1);
    for (Map.Entry<Integer, String> entry : softGoalNameByPriority.entrySet()) {
      Object[] singleDeadSingleSoftParams = {modifiedProperties, Collections.singletonMap(entry.getKey(),
          entry.getValue()), balancingConstraint};
      params.add(singleDeadSingleSoftParams);
    }
    // Test: All Soft Goals.
    Object[] singleDeadMultiSoftParams = {modifiedProperties, softGoalNameByPriority, balancingConstraint};
    params.add(singleDeadMultiSoftParams);
    // Test: Hard Goal.
    Object[] singleDeadSingleHardParams = {modifiedProperties, hardGoalNameByPriority, balancingConstraint};
    params.add(singleDeadSingleHardParams);

    // -- TEST DECK #2: MULTIPLE DEAD BROKERS.
    // Test: Single Soft Goal.
    modifiedProperties.put(ClusterProperty.NUM_DEAD_BROKERS, 10);
    for (Map.Entry<Integer, String> entry : softGoalNameByPriority.entrySet()) {
      Object[] multiDeadSingleSoftParams = {modifiedProperties, Collections.singletonMap(entry.getKey(),
                                                                                         entry.getValue()),
          balancingConstraint};
      params.add(multiDeadSingleSoftParams);
    }
    // Test: All Soft Goals.
    Object[] multiDeadMultiSoftParams = {modifiedProperties, softGoalNameByPriority, balancingConstraint};
    params.add(multiDeadMultiSoftParams);
    // Test: Hard Goal.
    Object[] multiDeadSingleHardParams = {modifiedProperties, hardGoalNameByPriority, balancingConstraint};
    params.add(multiDeadSingleHardParams);

    return params;
  }

  private Map<ClusterProperty, Number> _modifiedProperties;
  private Map<Integer, String> _goalNameByPriority;
  private BalancingConstraint _balancingConstraint;

  /**
   * Constructor of Self Healing Test.
   *
   * @param modifiedProperties Modified cluster properties over the {@link TestConstants#BASE_PROPERTIES}.
   * @param goalNameByPriority Goal name by priority.
   */
  public RandomSelfHealingTest(Map<ClusterProperty, Number> modifiedProperties,
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

    try {
      assertTrue("Self Healing Test failed to improve the existing state.",
          OptimizationVerifier.executeGoalsFor(_balancingConstraint, clusterModel, _goalNameByPriority));
    } catch (AnalysisInputException analysisInputException) {
      analysisInputException.printStackTrace();
      // This exception is thrown if self healing fails due to healthy brokers failing due to healthy brokers
      // violating rack awareness.
      assertThat(analysisInputException.getMessage(), is("Healthy brokers fail to satisfy rack-awareness."));
    }
  }
}