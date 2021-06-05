/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.BrokerAndSortedReplicas;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.TreeSet;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.Resource.DISK;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC2;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;


public class KafkaAssignerDiskUsageDistributionGoalTest {
  private static final TopicPartition T0P0 = new TopicPartition(TOPIC0, 0);
  private static final TopicPartition T0P1 = new TopicPartition(TOPIC0, 1);
  private static final TopicPartition T0P2 = new TopicPartition(TOPIC0, 2);

  private static final TopicPartition T1P0 = new TopicPartition(TOPIC1, 0);
  private static final TopicPartition T1P1 = new TopicPartition(TOPIC1, 1);
  private static final TopicPartition T1P2 = new TopicPartition(TOPIC1, 2);

  private static final TopicPartition T2P0 = new TopicPartition(TOPIC2, 0);
  private static final TopicPartition T2P1 = new TopicPartition(TOPIC2, 1);
  private static final TopicPartition T2P2 = new TopicPartition(TOPIC2, 2);

  @Test
  public void testCanSwap() {
    KafkaAssignerDiskUsageDistributionGoal goal = new KafkaAssignerDiskUsageDistributionGoal();
    ClusterModel clusterModel = createClusterModel();

    Replica r1 = clusterModel.broker(0).replica(T0P0);
    Replica r2 = clusterModel.broker(1).replica(T2P0);
    assertTrue("Replicas in the same rack should be good to swap", goal.canSwap(r1, r2, clusterModel));
    assertTrue("Replicas in the same rack should be good to swap", goal.canSwap(r2, r1, clusterModel));

    r2 = clusterModel.broker(1).replica(T1P0);
    assertFalse("Should not be able to swap replica with different roles.", goal.canSwap(r1, r2, clusterModel));
    assertFalse("Should not be able to swap replica with different roles.", goal.canSwap(r2, r1, clusterModel));

    r2 = clusterModel.broker(2).replica(T2P1);
    assertFalse("Should not be able to put two replicas in the same broker", goal.canSwap(r1, r2, clusterModel));
    assertFalse("Should not be able to put two replicas in the same broker", goal.canSwap(r2, r1, clusterModel));

    r2 = clusterModel.broker(3).replica(T2P2);
    assertFalse("Should not be able to put two replicas in the same rack", goal.canSwap(r1, r2, clusterModel));
    assertFalse("Should not be able to put two replicas in the same rack", goal.canSwap(r2, r1, clusterModel));

    r1 = clusterModel.broker(3).replica(T0P2);
    r2 = clusterModel.broker(4).replica(T1P2);
    assertTrue("Should be able to swap", goal.canSwap(r1, r2, clusterModel));
    assertTrue("Should be able to swap", goal.canSwap(r2, r1, clusterModel));
  }

  @Test
  public void testFindReplicaToSwapWith() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(10L));
    props.setProperty(AnalyzerConfig.OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(10L));
    props.setProperty(AnalyzerConfig.DISK_BALANCE_THRESHOLD_CONFIG, "1.05");
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    KafkaAssignerDiskUsageDistributionGoal goal = new KafkaAssignerDiskUsageDistributionGoal(balancingConstraint);
    ClusterModel clusterModel = createClusterModel();

    Broker b2 = clusterModel.broker(2);
    Replica r = b2.replica(T0P1);
    assertNull(goal.findReplicaToSwapWith(r, sortedReplicaAscend(clusterModel.broker(1)),
                                          30, 10, 90, clusterModel));

    // The replicas on broker 3 are of the following sizes
    // T0P0: 10
    // T0P2: 20
    // T1P1: 30
    // T2P2: 50
    // T2P1: 60
    // T1P0: 80
    // Only T0P0 and T1P1 are eligible to swap with r.
    findReplicaToSwapWithAndVerify(Arrays.asList(-1.0, 5.0, 10.0, 20.0, 21.0, 60.0, 100.0),
                                   Arrays.asList(T0P0, T0P0, T0P0, T0P0, T1P1, T1P1, T1P1),
                                   9, 90, r, 3, clusterModel, goal);

    findReplicaToSwapWithAndVerify(Arrays.asList(-1.0, 5.0, 10.0, 20.0, 21.0, 60.0, 100.0),
                                   Arrays.asList(T1P1, T1P1, T1P1, T1P1, T1P1, T1P1, T1P1),
                                   10, 31, r, 3, clusterModel, goal);

    findReplicaToSwapWithAndVerify(Arrays.asList(-1.0, 5.0, 10.0, 20.0, 21.0, 60.0, 100.0),
                                   Arrays.asList(T0P0, T0P0, T0P0, T0P0, T0P0, T0P0, T0P0),
                                   9, 30, r, 3, clusterModel, goal);

    findReplicaToSwapWithAndVerify(Arrays.asList(-1.0, 5.0, 10.0, 20.0, 21.0, 60.0, 100.0),
                                   Arrays.asList(null, null, null, null, null, null, null),
                                   10, 30, r, 3, clusterModel, goal);

  }

  @Test
  public void testSwapReplicas() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(10L));
    props.setProperty(AnalyzerConfig.OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(10L));
    props.setProperty(AnalyzerConfig.DISK_BALANCE_THRESHOLD_CONFIG, "1.05");
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    KafkaAssignerDiskUsageDistributionGoal goal = new KafkaAssignerDiskUsageDistributionGoal(balancingConstraint);
    ClusterModel clusterModel = createClusterModel();

    Comparator<Replica> replicaComparator =
        Comparator.comparingDouble((Replica r) -> r.load().expectedUtilizationFor(DISK))
                  .thenComparing(r -> r);

    double meanDiskUsage = clusterModel.load().expectedUtilizationFor(DISK) / clusterModel.capacityFor(DISK);
    assertTrue(goal.swapReplicas(new BrokerAndSortedReplicas(clusterModel.broker(0), replicaComparator),
                                 new BrokerAndSortedReplicas(clusterModel.broker(1), replicaComparator),
                                 meanDiskUsage,
                                 clusterModel,
                                 Collections.emptySet()));

    assertFalse(goal.swapReplicas(new BrokerAndSortedReplicas(clusterModel.broker(0), replicaComparator),
                                  new BrokerAndSortedReplicas(clusterModel.broker(2), replicaComparator),
                                  meanDiskUsage,
                                  clusterModel,
                                  Collections.emptySet()));

    assertTrue(goal.swapReplicas(new BrokerAndSortedReplicas(clusterModel.broker(2), replicaComparator),
                                 new BrokerAndSortedReplicas(clusterModel.broker(3), replicaComparator),
                                 meanDiskUsage,
                                 clusterModel,
                                 Collections.emptySet()));
  }

  private void findReplicaToSwapWithAndVerify(List<Double> targetSizes,
                                              List<TopicPartition> expectedResults,
                                              double minSize,
                                              double maxSize,
                                              Replica replica,
                                              int brokerIdToSwapWith,
                                              ClusterModel clusterModel,
                                              KafkaAssignerDiskUsageDistributionGoal goal) {
    for (int i = 0; i < targetSizes.size(); i++) {
      Replica toSwapWith =
          goal.findReplicaToSwapWith(replica, sortedReplicaAscend(clusterModel.broker(brokerIdToSwapWith)),
                                     targetSizes.get(i), minSize, maxSize, clusterModel);
      assertEquals(String.format("Wrong answer for targetSize = %f. Expected %s, but the result was %s",
                                 targetSizes.get(i), expectedResults.get(i), toSwapWith),
                   expectedResults.get(i), toSwapWith == null ? null : toSwapWith.topicPartition());

    }
  }

  private NavigableSet<KafkaAssignerDiskUsageDistributionGoal.ReplicaWrapper> sortedReplicaAscend(Broker broker) {
    NavigableSet<KafkaAssignerDiskUsageDistributionGoal.ReplicaWrapper> sortedReplicas = new TreeSet<>();
    for (Replica r : broker.replicas()) {
      sortedReplicas.add(new KafkaAssignerDiskUsageDistributionGoal.ReplicaWrapper(r, r.load().expectedUtilizationFor(DISK)));
    }
    return sortedReplicas;
  }

  /**
   * The replica distribution is as below.
   *
   * L - Leader
   * F - Follower
   * S - Secondary Follower
   *
   *         r0             r1         r2          r3
   *        /  \             |          |           |
   *      b0    b1          b2         b3          b4
   *   T0P0(L)  T0P1(L)   T0P2(L)    T1P0(L)     T1P1(L)
   *   T1P2(L)  T2P0(L)   T2P1(L)    T2P2(L)     T0P0(F)
   *   T0P2(F)  T1P0(F)   T0P1(F)    T1P1(F)     T1P2(F)
   *   T2P1(F)  T2P2(F)   T2P0(F)    T0P0(S)     T0P1(S)
   *   T1P1(S)            T1P0(S)    T0P2(S)     T2P0(S)
   *                      T1P2(S)    T2P1(S)     T2P2(S)
   * The sizes of each broker are:
   * b0: 190
   * b1: 260
   * b2: 360
   * b3: 250
   * b4: 290
   *
   * The average broker size should be: 270
   * @return Cluster model with the documented properties for testing.
   */
  private ClusterModel createClusterModel() {
    Map<TopicPartition, Float> partitionSize = new HashMap<>();

    partitionSize.put(T0P0, 10f);
    partitionSize.put(T0P1, 90f);
    partitionSize.put(T0P2, 20f);
    partitionSize.put(T1P0, 80f);
    partitionSize.put(T1P1, 30f);
    partitionSize.put(T1P2, 70f);
    partitionSize.put(T2P0, 40f);
    partitionSize.put(T2P1, 60f);
    partitionSize.put(T2P2, 50f);

    final int numRacks = 4;
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0),
                                                 1.0);
    for (int i = 0; i < numRacks; i++) {
      clusterModel.createRack("r" + i);
    }

    BrokerCapacityInfo commonBrokerCapacityInfo = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY);
    int i = 0;
    for (; i < 2; i++) {
      clusterModel.createBroker("r0", "h" + i, i, commonBrokerCapacityInfo, false);
    }
    for (int j = 1; j < numRacks; j++, i++) {
      clusterModel.createBroker("r" + j, "h" + i, i, commonBrokerCapacityInfo, false);
    }

    clusterModel.createReplica("r0", 0, T0P0, 0, true);
    clusterModel.createReplica("r0", 0, T1P2, 0, true);
    clusterModel.createReplica("r0", 1, T0P1, 0, true);
    clusterModel.createReplica("r0", 1, T2P0, 0, true);
    clusterModel.createReplica("r1", 2, T0P2, 0, true);
    clusterModel.createReplica("r1", 2, T2P1, 0, true);
    clusterModel.createReplica("r2", 3, T1P0, 0, true);
    clusterModel.createReplica("r2", 3, T2P2, 0, true);
    clusterModel.createReplica("r3", 4, T1P1, 0, true);

    clusterModel.createReplica("r0", 0, T0P2, 1, false);
    clusterModel.createReplica("r0", 0, T2P1, 1, false);
    clusterModel.createReplica("r0", 1, T1P0, 1, false);
    clusterModel.createReplica("r0", 1, T2P2, 1, false);
    clusterModel.createReplica("r1", 2, T0P1, 1, false);
    clusterModel.createReplica("r1", 2, T2P0, 1, false);
    clusterModel.createReplica("r2", 3, T1P1, 1, false);
    clusterModel.createReplica("r3", 4, T0P0, 1, false);
    clusterModel.createReplica("r3", 4, T1P2, 1, false);

    clusterModel.createReplica("r0", 0, T1P1, 2, false);
    clusterModel.createReplica("r1", 2, T1P0, 2, false);
    clusterModel.createReplica("r1", 2, T1P2, 2, false);
    clusterModel.createReplica("r2", 3, T0P0, 2, false);
    clusterModel.createReplica("r2", 3, T0P2, 2, false);
    clusterModel.createReplica("r2", 3, T2P1, 2, false);
    clusterModel.createReplica("r3", 4, T0P1, 2, false);
    clusterModel.createReplica("r3", 4, T2P0, 2, false);
    clusterModel.createReplica("r3", 4, T2P2, 2, false);

    List<Long> windows = Collections.singletonList(1L);
    clusterModel.setReplicaLoad("r0", 0, T0P0, getAggregatedMetricValues(partitionSize.get(T0P0)), windows);
    clusterModel.setReplicaLoad("r0", 0, T1P2, getAggregatedMetricValues(partitionSize.get(T1P2)), windows);
    clusterModel.setReplicaLoad("r0", 0, T0P2, getAggregatedMetricValues(partitionSize.get(T0P2)), windows);
    clusterModel.setReplicaLoad("r0", 0, T2P1, getAggregatedMetricValues(partitionSize.get(T2P1)), windows);
    clusterModel.setReplicaLoad("r0", 0, T1P1, getAggregatedMetricValues(partitionSize.get(T1P1)), windows);

    clusterModel.setReplicaLoad("r0", 1, T0P1, getAggregatedMetricValues(partitionSize.get(T0P1)), windows);
    clusterModel.setReplicaLoad("r0", 1, T2P0, getAggregatedMetricValues(partitionSize.get(T2P0)), windows);
    clusterModel.setReplicaLoad("r0", 1, T1P0, getAggregatedMetricValues(partitionSize.get(T1P0)), windows);
    clusterModel.setReplicaLoad("r0", 1, T2P2, getAggregatedMetricValues(partitionSize.get(T2P2)), windows);

    clusterModel.setReplicaLoad("r1", 2, T0P2, getAggregatedMetricValues(partitionSize.get(T0P2)), windows);
    clusterModel.setReplicaLoad("r1", 2, T2P1, getAggregatedMetricValues(partitionSize.get(T2P1)), windows);
    clusterModel.setReplicaLoad("r1", 2, T0P1, getAggregatedMetricValues(partitionSize.get(T0P1)), windows);
    clusterModel.setReplicaLoad("r1", 2, T2P0, getAggregatedMetricValues(partitionSize.get(T2P0)), windows);
    clusterModel.setReplicaLoad("r1", 2, T1P0, getAggregatedMetricValues(partitionSize.get(T1P0)), windows);
    clusterModel.setReplicaLoad("r1", 2, T1P2, getAggregatedMetricValues(partitionSize.get(T1P2)), windows);

    clusterModel.setReplicaLoad("r2", 3, T1P0, getAggregatedMetricValues(partitionSize.get(T1P0)), windows);
    clusterModel.setReplicaLoad("r2", 3, T2P2, getAggregatedMetricValues(partitionSize.get(T2P2)), windows);
    clusterModel.setReplicaLoad("r2", 3, T1P1, getAggregatedMetricValues(partitionSize.get(T1P1)), windows);
    clusterModel.setReplicaLoad("r2", 3, T0P0, getAggregatedMetricValues(partitionSize.get(T0P0)), windows);
    clusterModel.setReplicaLoad("r2", 3, T0P2, getAggregatedMetricValues(partitionSize.get(T0P2)), windows);
    clusterModel.setReplicaLoad("r2", 3, T2P1, getAggregatedMetricValues(partitionSize.get(T2P1)), windows);

    clusterModel.setReplicaLoad("r3", 4, T1P1, getAggregatedMetricValues(partitionSize.get(T1P1)), windows);
    clusterModel.setReplicaLoad("r3", 4, T0P0, getAggregatedMetricValues(partitionSize.get(T0P0)), windows);
    clusterModel.setReplicaLoad("r3", 4, T1P2, getAggregatedMetricValues(partitionSize.get(T1P2)), windows);
    clusterModel.setReplicaLoad("r3", 4, T0P1, getAggregatedMetricValues(partitionSize.get(T0P1)), windows);
    clusterModel.setReplicaLoad("r3", 4, T2P0, getAggregatedMetricValues(partitionSize.get(T2P0)), windows);
    clusterModel.setReplicaLoad("r3", 4, T2P2, getAggregatedMetricValues(partitionSize.get(T2P2)), windows);
    return clusterModel;
  }

  private AggregatedMetricValues getAggregatedMetricValues(double value) {
    return KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues(0, 0, 0, value);
  }
}
