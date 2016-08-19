/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

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
import com.linkedin.kafka.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Load;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

import java.util.Properties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertTrue;


/**
 * Unit test for testing with various balancing percentages and capacity thresholds using various deterministic clusters.
 */
@RunWith(Parameterized.class)
public class DeterministicClusterTest {
  private BalancingConstraint _balancingConstraint;
  private ClusterModel _cluster;
  private Map<Integer, String> _goalNameByPriority;

  /**
   * Constructor for Deterministic Cluster Test.
   *
   * @param balancingConstraint Balancing constraint.
   * @param cluster             The state of the cluster.
   * @param goalNameByPriority  Name of goals by the order of execution priority.
   */
  public DeterministicClusterTest(BalancingConstraint balancingConstraint, ClusterModel cluster,
                                  Map<Integer, String> goalNameByPriority) {
    _balancingConstraint = balancingConstraint;
    _cluster = cluster;
    _goalNameByPriority = goalNameByPriority;
  }

  /**
   * Populate parameters for the {@link OptimizationVerifier}. All brokers are alive.
   *
   * @return Parameters for the {@link OptimizationVerifier}.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data()
      throws AnalysisInputException, ModelInputException {
    Collection<Object[]> params = new ArrayList<>();

    int numSnapshots = 1;
    if (!Load.initialized()) {
      Properties props = CruiseControlUnitTestUtils.getCruiseControlProperties();
      props.setProperty(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG, Integer.toString(numSnapshots));
      Load.init(new KafkaCruiseControlConfig(props));
    }

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

    BalancingConstraint balancingConstraint =
        new BalancingConstraint(new KafkaCruiseControlConfig(CruiseControlUnitTestUtils.getCruiseControlProperties()));

    // ----------##TEST: BALANCE PERCENTAGES.
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);
    List<Double> balancePercentages = new ArrayList<>();
    balancePercentages.add(TestConstants.HIGH_BALANCE_PERCENTAGE);
    balancePercentages.add(TestConstants.MEDIUM_BALANCE_PERCENTAGE);
    balancePercentages.add(TestConstants.LOW_BALANCE_PERCENTAGE);

    // -- TEST DECK #1: SMALL CLUSTER.
    for (Double balancePercentage : balancePercentages) {
      balancingConstraint.setBalancePercentage(balancePercentage);
      Object[] testParams = {balancingConstraint,
          DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY), goalNameByPriority};
      params.add(testParams);
    }
    // -- TEST DECK #2: MEDIUM CLUSTER.
    for (Double balancePercentage : balancePercentages) {
      balancingConstraint.setBalancePercentage(balancePercentage);
      Object[] testParams = {balancingConstraint,
          DeterministicCluster.mediumClusterModel(TestConstants.BROKER_CAPACITY), goalNameByPriority};
      params.add(testParams);
    }

    // ----------##TEST: CAPACITY THRESHOLD.
    balancingConstraint.setBalancePercentage(TestConstants.MEDIUM_BALANCE_PERCENTAGE);
    List<Double> capacityThresholds = new ArrayList<>();
    capacityThresholds.add(TestConstants.HIGH_CAPACITY_THRESHOLD);
    capacityThresholds.add(TestConstants.MEDIUM_CAPACITY_THRESHOLD);
    capacityThresholds.add(TestConstants.LOW_CAPACITY_THRESHOLD);

    // -- TEST DECK #3: SMALL CLUSTER.
    for (Double capacityThreshold : capacityThresholds) {
      balancingConstraint.setCapacityThreshold(capacityThreshold);
      Object[] testParams = {balancingConstraint,
          DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY), goalNameByPriority};
      params.add(testParams);
    }
    // -- TEST DECK #4: MEDIUM CLUSTER.
    for (Double capacityThreshold : capacityThresholds) {
      balancingConstraint.setCapacityThreshold(capacityThreshold);
      Object[] testParams = {balancingConstraint,
          DeterministicCluster.mediumClusterModel(TestConstants.BROKER_CAPACITY), goalNameByPriority};
      params.add(testParams);
    }

    // ----------##TEST: BROKER CAPACITY.
    List<Double> brokerCapacities = new ArrayList<>();
    brokerCapacities.add(TestConstants.LARGE_BROKER_CAPACITY);
    brokerCapacities.add(TestConstants.MEDIUM_BROKER_CAPACITY);
    brokerCapacities.add(TestConstants.SMALL_BROKER_CAPACITY);

    // -- TEST DECK #5: SMALL AND MEDIUM CLUSTERS.
    for (Double capacity : brokerCapacities) {
      Map<Resource, Double> testBrokerCapacity = new HashMap<>();
      testBrokerCapacity.put(Resource.CPU, capacity);
      testBrokerCapacity.put(Resource.DISK, capacity);
      testBrokerCapacity.put(Resource.NW_IN, capacity);
      testBrokerCapacity.put(Resource.NW_OUT, capacity);

      Object[] smallTestParams = {balancingConstraint,
          DeterministicCluster.smallClusterModel(testBrokerCapacity), goalNameByPriority};
      params.add(smallTestParams);
      Object[] testParams = {balancingConstraint,
          DeterministicCluster.mediumClusterModel(testBrokerCapacity), goalNameByPriority};
      params.add(testParams);
    }

    return params;
  }

  @Test
  public void test() throws Exception {
    try {
      assertTrue("Deterministic Cluster Test failed to improve the existing state.",
          OptimizationVerifier.executeGoalsFor(_balancingConstraint, _cluster, _goalNameByPriority));
    } catch (AnalysisInputException analysisInputException) {
      // This exception is thrown if rebalance fails due to healthy brokers having insufficient capacity.
      if (!analysisInputException.getMessage().contains("Insufficient healthy cluster capacity for resource")) {
        throw analysisInputException;
      }
    }
  }
}
