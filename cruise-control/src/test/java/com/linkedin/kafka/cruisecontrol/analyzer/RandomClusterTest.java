/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.MinTopicLeadersPerBrokerGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.ClusterProperty;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.model.RandomCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.NEW_BROKERS;
import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.BROKEN_BROKERS;
import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.GOAL_VIOLATION;
import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.REGRESSION;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for testing with different cluster properties and fixed goals.
 */
public class RandomClusterTest {
  private static final Logger LOG = LoggerFactory.getLogger(RandomClusterTest.class);
  private final Map<ClusterProperty, Number> _modifiedProperties;
  private final List<String> _goalNameByPriority;
  private final TestConstants.Distribution _replicaDistribution;
  private final BalancingConstraint _balancingConstraint;
  private final List<OptimizationVerifier.Verification> _verifications;

  /**
   * Constructor of Random Cluster Test.
   *
   * @param modifiedProperties  Modified cluster properties over the {@link TestConstants#BASE_PROPERTIES}.
   * @param goalNameByPriority  Goal name by priority.
   * @param replicaDistribution Distribution of replicas in the test cluster.
   * @param balancingConstraint The balancing constraints.
   * @param verifications       The verifications to make.
   */
  public RandomClusterTest(Map<ClusterProperty, Number> modifiedProperties,
                           List<String> goalNameByPriority,
                           TestConstants.Distribution replicaDistribution,
                           BalancingConstraint balancingConstraint,
                           List<OptimizationVerifier.Verification> verifications) {
    _modifiedProperties = modifiedProperties;
    _goalNameByPriority = goalNameByPriority;
    _replicaDistribution = replicaDistribution;
    _balancingConstraint = balancingConstraint;
    _verifications = verifications;
  }

  /**
   * Populate parameters for the {@link OptimizationVerifier}. All brokers are alive.
   *
   * @param distribution Load distribution type.
   * @return Parameters for the {@link OptimizationVerifier}.
   */
  public static Collection<Object[]> data(TestConstants.Distribution distribution) {
    Collection<Object[]> p = new ArrayList<>();

    // Sorted by priority.
    List<String> goalNameByPriority = Arrays.asList(RackAwareGoal.class.getName(),
                                                    RackAwareDistributionGoal.class.getName(),
                                                    MinTopicLeadersPerBrokerGoal.class.getName(),
                                                    ReplicaCapacityGoal.class.getName(),
                                                    DiskCapacityGoal.class.getName(),
                                                    NetworkInboundCapacityGoal.class.getName(),
                                                    NetworkOutboundCapacityGoal.class.getName(),
                                                    CpuCapacityGoal.class.getName(),
                                                    ReplicaDistributionGoal.class.getName(),
                                                    PotentialNwOutGoal.class.getName(),
                                                    DiskUsageDistributionGoal.class.getName(),
                                                    NetworkInboundUsageDistributionGoal.class.getName(),
                                                    NetworkOutboundUsageDistributionGoal.class.getName(),
                                                    CpuUsageDistributionGoal.class.getName(),
                                                    LeaderReplicaDistributionGoal.class.getName(),
                                                    LeaderBytesInDistributionGoal.class.getName(),
                                                    TopicReplicaDistributionGoal.class.getName(),
                                                    PreferredLeaderElectionGoal.class.getName());

    List<String> kafkaAssignerGoals = Arrays.asList(KafkaAssignerEvenRackAwareGoal.class.getName(),
                                                    KafkaAssignerDiskUsageDistributionGoal.class.getName());

    List<OptimizationVerifier.Verification> verifications = Arrays.asList(NEW_BROKERS, BROKEN_BROKERS, REGRESSION);
    List<OptimizationVerifier.Verification> kafkaAssignerVerifications =
        Arrays.asList(GOAL_VIOLATION, BROKEN_BROKERS, REGRESSION);

    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(1500L));
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    Map<ClusterProperty, Number> modifiedProperties;
    // Test: Increase Broker Count
    for (int i = 1; i <= 2; i++) {
      modifiedProperties = new HashMap<>();
      modifiedProperties.put(ClusterProperty.NUM_BROKERS, 20 + i * 60);
      p.add(params(modifiedProperties, goalNameByPriority, distribution, balancingConstraint, verifications));
      p.add(params(modifiedProperties, kafkaAssignerGoals, distribution, balancingConstraint, kafkaAssignerVerifications));
    }

    // Test: Increase Replica Count
    props.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(3000L));
    balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);
    for (int i = 3; i <= 5; i++) {
      modifiedProperties = new HashMap<>();
      modifiedProperties.put(ClusterProperty.NUM_REPLICAS, 50001 + i * 5001);
      p.add(params(modifiedProperties, goalNameByPriority, distribution, balancingConstraint, verifications));
      p.add(params(modifiedProperties, kafkaAssignerGoals, distribution, balancingConstraint, kafkaAssignerVerifications));
    }
    // Test: Increase Topic Count
    for (int i = 6; i <= 7; i++) {
      modifiedProperties = new HashMap<>();
      modifiedProperties.put(ClusterProperty.NUM_TOPICS, 3000 + (i - 2) * 1000);
      p.add(params(modifiedProperties, goalNameByPriority, distribution, balancingConstraint, verifications));
      p.add(params(modifiedProperties, kafkaAssignerGoals, distribution, balancingConstraint, kafkaAssignerVerifications));
    }
    // Test: Increase Replication Count
    for (int i = 8; i <= 9; i++) {
      modifiedProperties = new HashMap<>();
      modifiedProperties.put(ClusterProperty.NUM_REPLICAS, 50000 - (50000 % (i - 4)));
      modifiedProperties.put(ClusterProperty.MIN_REPLICATION, (i - 4));
      modifiedProperties.put(ClusterProperty.MAX_REPLICATION, (i - 4));
      p.add(params(modifiedProperties, goalNameByPriority, distribution, balancingConstraint, verifications));
      p.add(params(modifiedProperties, kafkaAssignerGoals, distribution, balancingConstraint, kafkaAssignerVerifications));
    }

    return p;
  }

  private ClusterModel rebalance() throws Exception {
    // Create cluster properties by applying modified properties to base properties.
    Map<ClusterProperty, Number> clusterProperties = new HashMap<>(TestConstants.BASE_PROPERTIES);
    clusterProperties.putAll(_modifiedProperties);

    LOG.debug("Replica distribution: {}.", _replicaDistribution);
    ClusterModel clusterModel = RandomCluster.generate(clusterProperties);
    RandomCluster.populate(clusterModel, clusterProperties, _replicaDistribution);

    assertTrue("Random Cluster Test failed to improve the existing state.",
        OptimizationVerifier.executeGoalsFor(_balancingConstraint, clusterModel, _goalNameByPriority, _verifications));
    return clusterModel;
  }

  /**
   * This test first creates a random cluster, balance it. Then add two new brokers, balance the cluster again.
   */
  public void testNewBrokers() throws Exception {
    ClusterModel clusterModel = rebalance();

    ClusterModel clusterWithNewBroker = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    for (Broker b : clusterModel.brokers()) {
      clusterWithNewBroker.createRack(b.rack().id());
      Map<Resource, Double> brokerCapacity = new HashMap<>();
      for (Resource r : Resource.cachedValues()) {
        brokerCapacity.put(r, b.capacityFor(r));
      }

      BrokerCapacityInfo brokerCapacityInfo = new BrokerCapacityInfo(brokerCapacity);
      clusterWithNewBroker.createBroker(b.rack().id(), Integer.toString(b.id()), b.id(), brokerCapacityInfo, false);
    }

    for (Map.Entry<String, List<Partition>> entry : clusterModel.getPartitionsByTopic().entrySet()) {
      for (Partition p : entry.getValue()) {
        int index = 0;
        for (Replica r : p.replicas()) {
          clusterWithNewBroker.createReplica(r.broker().rack().id(), r.broker().id(), p.topicPartition(), index++, r.isLeader());
        }
      }
    }

    for (Broker b : clusterModel.brokers()) {
      for (Replica replica : b.replicas()) {
        AggregatedMetricValues aggregatedMetricValues =
            clusterModel.broker(b.id()).replica(replica.topicPartition()).load().loadByWindows();
        clusterWithNewBroker.setReplicaLoad(b.rack().id(), b.id(), replica.topicPartition(), aggregatedMetricValues,
                                            clusterModel.load().windows());
      }
    }

    BrokerCapacityInfo commonBrokerCapacityInfo = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY);
    for (int i = 1; i < 3; i++) {
      clusterWithNewBroker.createBroker(Integer.toString(i),
                                        Integer.toString(i + clusterModel.brokers().size() - 1),
                                        i + clusterModel.brokers().size() - 1,
                                        commonBrokerCapacityInfo,
                                        false);
      clusterWithNewBroker.setBrokerState(i + clusterModel.brokers().size() - 1, Broker.State.NEW);
    }

    assertTrue("Random Cluster Test failed to improve the existing state with new brokers.",
               OptimizationVerifier.executeGoalsFor(_balancingConstraint, clusterWithNewBroker, _goalNameByPriority,
                                                    _verifications));
  }

  private static Object[] params(Map<ClusterProperty, Number> modifiedProperties,
                                 List<String> goalNameByPriority,
                                 TestConstants.Distribution replicaDistribution,
                                 BalancingConstraint balancingConstraint,
                                 List<OptimizationVerifier.Verification> verifications) {
    return new Object[]{modifiedProperties, goalNameByPriority, replicaDistribution, balancingConstraint, verifications};
  }
}
