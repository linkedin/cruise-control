/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoals;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.ClusterProperty;
import com.linkedin.kafka.cruisecontrol.common.RandomCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;


/**
 * Unit test for testing with different clusters properties and fixed goals.
 */
public class RandomClusterTest {
  private static final Logger LOG = LoggerFactory.getLogger(RandomClusterTest.class);

  /**
   * Populate parameters for the {@link OptimizationVerifier}. All brokers are alive.
   *
   * @return Parameters for the {@link OptimizationVerifier}.
   */
  public static Collection<Object[]> data(TestConstants.Distribution distribution)
      throws AnalysisInputException {
    Collection<Object[]> params = new ArrayList<>();

    Map<Integer, String> goalNameByPriority = new HashMap<>();
    goalNameByPriority.put(1, RackAwareCapacityGoal.class.getName());
    goalNameByPriority.put(2, PotentialNwOutGoal.class.getName());
    goalNameByPriority.put(3, TopicReplicaDistributionGoal.class.getName());
    goalNameByPriority.put(4, ResourceDistributionGoal.class.getName());
    goalNameByPriority.put(5, LeaderBytesInDistributionGoals.class.getName());
    goalNameByPriority.put(6, ReplicaDistributionGoal.class.getName());

    KafkaCruiseControlConfig config =
        new KafkaCruiseControlConfig(CruiseControlUnitTestUtils.getCruiseControlProperties());
    BalancingConstraint balancingConstraint = new BalancingConstraint(config);
    balancingConstraint.setBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    Map<ClusterProperty, Number> modifiedProperties;
    // Test: Increase Broker Count
    for (int i = 1; i <= 6; i++) {
      modifiedProperties = new HashMap<>();
      modifiedProperties.put(ClusterProperty.NUM_BROKERS, 20 + i * 20);
      Object[] brokerCountParams = {modifiedProperties, goalNameByPriority, distribution, balancingConstraint};
      params.add(brokerCountParams);
    }
    // Test: Increase Replica Count
    for (int i = 7; i <= 12; i++) {
      modifiedProperties = new HashMap<>();
      modifiedProperties.put(ClusterProperty.NUM_REPLICAS, 50001 + (i - 7) * 5001);
      Object[] replicaCountParams = {modifiedProperties, goalNameByPriority, distribution, balancingConstraint};
      params.add(replicaCountParams);
    }
    // Test: Increase Topic Count
    for (int i = 13; i <= 18; i++) {
      modifiedProperties = new HashMap<>();
      modifiedProperties.put(ClusterProperty.NUM_TOPICS, 3000 + (i - 13) * 1000);
      Object[] topicCountParams = {modifiedProperties, goalNameByPriority, distribution, balancingConstraint};
      params.add(topicCountParams);
    }
    // Test: Increase Replication Count
    for (int i = 19; i <= 24; i++) {
      modifiedProperties = new HashMap<>();
      modifiedProperties.put(ClusterProperty.NUM_REPLICAS, 50000 - (50000 % (i - 16)));
      modifiedProperties.put(ClusterProperty.MIN_REPLICATION, (i - 16));
      modifiedProperties.put(ClusterProperty.MAX_REPLICATION, (i - 16));
      Object[] replicationCountParams = {modifiedProperties, goalNameByPriority, distribution, balancingConstraint};
      params.add(replicationCountParams);
    }

    return params;
  }

  private Map<ClusterProperty, Number> _modifiedProperties;
  private Map<Integer, String> _goalNameByPriority;
  private TestConstants.Distribution _replicaDistribution;
  private BalancingConstraint _balancingConstraint;

  /**
   * Constructor of Random Cluster Test.
   *
   * @param modifiedProperties  Modified cluster properties over the {@link TestConstants#BASE_PROPERTIES}.
   * @param goalNameByPriority  Goal name by priority.
   * @param replicaDistribution Distribution of replicas in the test cluster.
   * @param balancingConstraint The balancing constraints.
   */
  public RandomClusterTest(Map<ClusterProperty, Number> modifiedProperties,
                           Map<Integer, String> goalNameByPriority,
                           TestConstants.Distribution replicaDistribution,
                           BalancingConstraint balancingConstraint) {
    _modifiedProperties = modifiedProperties;
    _goalNameByPriority = goalNameByPriority;
    _replicaDistribution = replicaDistribution;
    _balancingConstraint = balancingConstraint;
  }

  public void testRebalance() throws Exception {
    // Create cluster properties by applying modified properties to base properties.
    Map<ClusterProperty, Number> clusterProperties = new HashMap<>(TestConstants.BASE_PROPERTIES);
    clusterProperties.putAll(_modifiedProperties);

    LOG.debug("Replica distribution: {}.", _replicaDistribution);
    ClusterModel clusterModel = RandomCluster.generate(clusterProperties);
    RandomCluster.populate(clusterModel, clusterProperties, _replicaDistribution);

    assertTrue("Random Cluster Test failed to improve the existing state.",
        OptimizationVerifier.executeGoalsFor(_balancingConstraint, clusterModel, _goalNameByPriority));
  }

  /**
   * This test first creates a random cluster, balance it. Then add two new brokers, balance the cluster again.
   */
  public void testNewBrokers() throws Exception {
    // Create cluster properties by applying modified properties to base properties.
    Map<ClusterProperty, Number> clusterProperties = new HashMap<>(TestConstants.BASE_PROPERTIES);
    clusterProperties.putAll(_modifiedProperties);

    LOG.debug("Replica distribution: {}.", _replicaDistribution);
    ClusterModel clusterModel = RandomCluster.generate(clusterProperties);
    RandomCluster.populate(clusterModel, clusterProperties, _replicaDistribution);

    assertTrue("Random Cluster Test failed to improve the existing state.",
               OptimizationVerifier.executeGoalsFor(_balancingConstraint, clusterModel, _goalNameByPriority));

    ClusterModel clusterWithNewBroker = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    for (Broker b : clusterModel.brokers()) {
      clusterWithNewBroker.createRack(b.rack().id());
      clusterWithNewBroker.createBroker(b.rack().id(), Integer.toString(b.id()), b.id(), TestConstants.BROKER_CAPACITY);
      for (Replica replica : b.replicas()) {
        clusterWithNewBroker.createReplica(b.rack().id(), b.id(), replica.topicPartition(), replica.isLeader());
      }
    }

    for (Broker b : clusterModel.brokers()) {
      for (Replica replica : b.replicas()) {
        List<Snapshot> snapshots =
            clusterModel.broker(b.id()).replica(replica.topicPartition()).load().snapshotsByTime();
        for (Snapshot snapshot : snapshots) {
          clusterWithNewBroker.pushLatestSnapshot(b.rack().id(), b.id(), replica.topicPartition(), snapshot);
        }
      }
    }

    for (int i = 1; i < 3; i++) {
      clusterWithNewBroker.createBroker(Integer.toString(i),
                                        Integer.toString(i + clusterModel.brokers().size()),
                                        i + clusterModel.brokers().size(),
                                        TestConstants.BROKER_CAPACITY);
      clusterWithNewBroker.setBrokerState(i + clusterModel.brokers().size(), Broker.State.NEW);
    }

    assertTrue("Random Cluster Test failed to improve the existing state.",
               OptimizationVerifier.executeGoalsFor(_balancingConstraint, clusterWithNewBroker, _goalNameByPriority));
  }
}