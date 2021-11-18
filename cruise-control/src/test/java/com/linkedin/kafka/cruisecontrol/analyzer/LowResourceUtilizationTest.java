/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUnitTestUtils.goal;

/**
 * Unit test for testing low utilization threshold taking effect to prevent rebalance for resource distribution goals
 */
@RunWith(Parameterized.class)
public class LowResourceUtilizationTest {

  private static final Map<Resource, Double> BROKER_CAPACITY;
  static {
    BROKER_CAPACITY = Map.of(Resource.CPU, 200.0, Resource.DISK, 1000.0, Resource.NW_IN, 2000.0, Resource.NW_OUT, 2000.0);
  }

  private final ResourceDistributionGoal _resourceDistributionGoal;
  private final boolean _expectRebalance;

  public LowResourceUtilizationTest(ResourceDistributionGoal resourceDistributionGoal, boolean expectRebalance) {
    _resourceDistributionGoal = resourceDistributionGoal;
    _expectRebalance = expectRebalance;
  }

  /**
   * Populate parameters to test rebalance with low utilization threshold. All brokers are alive.
   *
   * @return Parameters to test rebalance with low utilization threshold.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() throws Exception {
    Collection<Object[]> p = new ArrayList<>();
    final double maxCpuUtilizationRatio = 0.3475;
    final double maxDiskUtilizationRatio = 0.28;
    final double maxNetworkInUtilizationRatio = 0.13;
    final double maxNetworkOutUtilizationRatio = 0.1475;
    final double balanceMargin = 0.9;

    // Expect rebalance on CPU usage distribution goal since one broker's CPU usage is above the low CPU utilization threshold
    Properties configOverrides = new Properties();
    configOverrides.put(AnalyzerConfig.CPU_LOW_UTILIZATION_THRESHOLD_CONFIG, onePercentSmaller(maxCpuUtilizationRatio));
    p.add(params(goal(CpuUsageDistributionGoal.class, configOverrides), true));

    // Expect no rebalance on CPU usage distribution goal since all brokers' CPU usage is below the low CPU utilization threshold
    configOverrides.put(AnalyzerConfig.CPU_LOW_UTILIZATION_THRESHOLD_CONFIG,
        onePercentGreater(maxCpuUtilizationRatio / balanceMargin));
    p.add(params(goal(CpuUsageDistributionGoal.class, configOverrides), false));

    // Expect rebalance on disk usage distribution goal since one broker's disk usage is above the low disk utilization threshold
    configOverrides.put(AnalyzerConfig.DISK_LOW_UTILIZATION_THRESHOLD_CONFIG, onePercentSmaller(maxDiskUtilizationRatio));
    p.add(params(goal(DiskUsageDistributionGoal.class, configOverrides), true));

    // Expect no rebalance on disk usage distribution goal since all brokers' disk usage is below the low disk utilization threshold
    configOverrides.put(AnalyzerConfig.DISK_LOW_UTILIZATION_THRESHOLD_CONFIG,
        onePercentGreater(maxDiskUtilizationRatio / balanceMargin));
    p.add(params(goal(DiskUsageDistributionGoal.class, configOverrides), false));

    // Expect rebalance on network inbound usage distribution goal since one broker's network inbound usage is
    // above the low network inbound utilization threshold
    configOverrides.put(AnalyzerConfig.NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG,
        onePercentSmaller(maxNetworkInUtilizationRatio));
    p.add(params(goal(NetworkInboundUsageDistributionGoal.class, configOverrides), true));

    // Expect no rebalance on network inbound usage distribution goal since all brokers' network inbound usage is
    // below the low network inbound utilization threshold
    configOverrides.put(AnalyzerConfig.NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG,
        onePercentGreater(maxNetworkInUtilizationRatio / balanceMargin));
    p.add(params(goal(NetworkInboundUsageDistributionGoal.class, configOverrides), false));

    // Expect rebalance on network outbound usage distribution goal since one broker's network outbound usage is
    // above the low network outbound utilization threshold
    configOverrides.put(AnalyzerConfig.NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG, onePercentSmaller(maxNetworkOutUtilizationRatio));
    p.add(params(goal(NetworkOutboundUsageDistributionGoal.class, configOverrides), true));

    // Expect no rebalance on network outbound usage distribution goal since all brokers' network outbound usage is
    // below the low network outbound utilization threshold
    configOverrides.put(AnalyzerConfig.NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG,
        onePercentGreater(maxNetworkOutUtilizationRatio / balanceMargin));
    p.add(params(goal(NetworkOutboundUsageDistributionGoal.class, configOverrides), false));

    return p;
  }

  private static String onePercentGreater(double value) {
    return Double.toString(value * 1.01);
  }

  private static String onePercentSmaller(double value) {
    return Double.toString(value * 0.99);
  }

  /**
   * Creates a small cluster model with broker resource utilization percentage shown below:
   *    Broker-0:
   *      CPU:    34.75%
   *      Disk:   28%
   *      NW_IN:  13%
   *      NW_OUT: 14.75%
   *
   *    Broker-1:
   *      CPU:    14%
   *      Disk:   15.5%
   *      NW_IN:  7%
   *      NW_OUT: 5.8%
   *
   *    Broker-2:
   *      CPU:    9.75%
   *      Disk:   13.5%
   *      NW_IN:  6.5%
   *      NW_OUT: 0%
   *
   * @return cluster model
   */
  private static ClusterModel createSmallClusterModel() {
    return DeterministicCluster.smallClusterModel(BROKER_CAPACITY);
  }

  private static Object[] params(Goal resourceDistributionGoal, Boolean expectRebalance) {
    return new Object[]{resourceDistributionGoal, expectRebalance};
  }

  @Test
  public void test() throws OptimizationFailureException {
    ClusterModel clusterModel = createSmallClusterModel();
    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = clusterModel.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initLeaderDistribution = clusterModel.getLeaderDistribution();

    // Before the optimization, goals are expected to be undecided wrt their provision status.
    assertEquals(ProvisionStatus.UNDECIDED, _resourceDistributionGoal.provisionResponse().status());
    assertTrue("Failed to optimize " + _resourceDistributionGoal.name(),
        _resourceDistributionGoal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet(),
                                                                                                         Collections.emptySet(),
                                                                                                         Collections.emptySet())));
    // Since all optimizations succeed, it is guaranteed that all brokers have resource utilization under low utilization threshold.
    assertEquals(ProvisionStatus.OVER_PROVISIONED, _resourceDistributionGoal.provisionResponse().status());

    boolean hasDiff = AnalyzerUtils.hasDiff(initReplicaDistribution, initLeaderDistribution, clusterModel);

    if (_expectRebalance) {
      assertTrue(hasDiff);
    } else {
      assertFalse(hasDiff);
    }
  }
}
