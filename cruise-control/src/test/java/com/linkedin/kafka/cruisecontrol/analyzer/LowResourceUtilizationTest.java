/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

/**
 * Unit test for testing low utilization threshold taking effect to prevent rebalance for resource distribution goals
 */
@RunWith(Parameterized.class)
public class LowResourceUtilizationTest {

  private static final Map<Resource, Double> BROKER_CAPACITY = new HashMap<Resource, Double>() {{
    put(Resource.CPU, 200.0);
    put(Resource.DISK, 1000.0);
    put(Resource.NW_IN, 2000.0);
    put(Resource.NW_OUT, 2000.0);
  }};

  private final ClusterModel _clusterModel;
  private final ResourceDistributionGoal _resourceDistributionGoal;
  private final Map<String, Object> _goalConfigs;
  private final Boolean _expectRebalance;

  public LowResourceUtilizationTest(ClusterModel clusterModel,
                                    ResourceDistributionGoal resourceDistributionGoal,
                                    Map<String, Object> goalConfigs,
                                    Boolean expectRebalance) {

    _clusterModel = clusterModel;
    _resourceDistributionGoal = resourceDistributionGoal;
    _goalConfigs = goalConfigs;
    _expectRebalance = expectRebalance;
  }

  /**
   * Populate parameters to test rebalance with low utilization threshold. All brokers are alive.
   *
   * @return Parameters to test rebalance with low utilization threshold.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> p = new ArrayList<>();

    // Expect rebalance on CPU usage distribution goal since one broker's CPU usage is above the low CPU utilization threshold
    ClusterModel clusterModel = createSmallClusterModel();
    Map<String, Object> goalConfigs = getDefaultGoalConfigs();
    goalConfigs.put(AnalyzerConfig.CPU_LOW_UTILIZATION_THRESHOLD_CONFIG, 0.3474);
    p.add(params(clusterModel, new CpuUsageDistributionGoal(), goalConfigs, true));

    // Expect no rebalance on CPU usage distribution goal since all brokers' CPU usage is below the low CPU utilization threshold
    clusterModel = createSmallClusterModel();
    goalConfigs = getDefaultGoalConfigs();
    goalConfigs.put(AnalyzerConfig.CPU_LOW_UTILIZATION_THRESHOLD_CONFIG, 0.3476);
    p.add(params(clusterModel, new CpuUsageDistributionGoal(), goalConfigs, false));

    // Expect rebalance on disk usage distribution goal since one broker's disk usage is above the low disk utilization threshold
    clusterModel = createSmallClusterModel();
    goalConfigs = getDefaultGoalConfigs();
    goalConfigs.put(AnalyzerConfig.DISK_LOW_UTILIZATION_THRESHOLD_CONFIG, 0.27);
    p.add(params(clusterModel, new DiskUsageDistributionGoal(), goalConfigs, true));

    // Expect no rebalance on disk usage distribution goal since all brokers' disk usage is below the low disk utilization threshold
    clusterModel = createSmallClusterModel();
    goalConfigs = getDefaultGoalConfigs();
    goalConfigs.put(AnalyzerConfig.DISK_LOW_UTILIZATION_THRESHOLD_CONFIG, 0.29);
    p.add(params(clusterModel, new DiskUsageDistributionGoal(), goalConfigs, false));

    // Expect rebalance on network inbound usage distribution goal since one broker's network inbound usage is
    // above the low network inbound utilization threshold
    clusterModel = createSmallClusterModel();
    goalConfigs = getDefaultGoalConfigs();
    goalConfigs.put(AnalyzerConfig.NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG, 0.12);
    p.add(params(clusterModel, new NetworkInboundUsageDistributionGoal(), goalConfigs, true));

    // Expect no rebalance on network inbound usage distribution goal since all brokers' network inbound usage is
    // below the low network inbound utilization threshold
    clusterModel = createSmallClusterModel();
    goalConfigs = getDefaultGoalConfigs();
    goalConfigs.put(AnalyzerConfig.NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG, 0.14);
    p.add(params(clusterModel, new NetworkInboundUsageDistributionGoal(), goalConfigs, false));


    // Expect rebalance on network outbound usage distribution goal since one broker's network outbound usage is
    // above the low network outbound utilization threshold
    clusterModel = createSmallClusterModel();
    goalConfigs = getDefaultGoalConfigs();
    goalConfigs.put(AnalyzerConfig.NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG, 0.14);
    p.add(params(clusterModel, new NetworkOutboundUsageDistributionGoal(), goalConfigs, true));

    // Expect no rebalance on network outbound usage distribution goal since all brokers' network outbound usage is
    // below the low network outbound utilization threshold
    clusterModel = createSmallClusterModel();
    goalConfigs = getDefaultGoalConfigs();
    goalConfigs.put(AnalyzerConfig.NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG, 0.15);
    p.add(params(clusterModel, new NetworkOutboundUsageDistributionGoal(), goalConfigs, false));

    return p;
  }

  private static Map<String, Object> getDefaultGoalConfigs() {
    return new HashMap<String, Object>() {{
      put(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "bootstrap.servers");
      put(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG, "connect:1234");
    }};
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

  private static Object[] params(ClusterModel clusterModel,
                                 ResourceDistributionGoal resourceDistributionGoal,
                                 Map<String, Object> goalConfigs,
                                 Boolean expectRebalance) {
    return new Object[]{clusterModel, resourceDistributionGoal, goalConfigs, expectRebalance};
  }

  @Test
  public void test() throws OptimizationFailureException {
    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = _clusterModel.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initLeaderDistribution = _clusterModel.getLeaderDistribution();

    _resourceDistributionGoal.configure(_goalConfigs);
    assertTrue("Failed to optimize " + _resourceDistributionGoal.name(),
        _resourceDistributionGoal.optimize(_clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet())));

    Set<ExecutionProposal> goalProposals =
        AnalyzerUtils.getDiff(initReplicaDistribution, initLeaderDistribution, _clusterModel);

    if (_expectRebalance) {
      assertFalse(goalProposals.isEmpty());
    } else {
      assertTrue(goalProposals.isEmpty());
    }
  }
}
