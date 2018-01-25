/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner;

import com.linkedin.kafka.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Load;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.Snapshot;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.Resource.DISK;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;


public class KafkaAssignerDiskUsageDistributionGoalTest {
  private static final String TOPIC0 = "topic0";
  private static final String TOPIC1 = "topic1";
  private static final String TOPIC2 = "topic2";

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
  public void testCanSwap() throws ModelInputException {
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
  public void testFindReplicaToSwapWith() throws KafkaCruiseControlException {
    Properties props = CruiseControlUnitTestUtils.getCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(10L));
    props.setProperty(KafkaCruiseControlConfig.DISK_BALANCE_THRESHOLD_CONFIG, "1.05");
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
  public void testSwapReplicas() throws ModelInputException, AnalysisInputException {
    Properties props = CruiseControlUnitTestUtils.getCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(10L));
    props.setProperty(KafkaCruiseControlConfig.DISK_BALANCE_THRESHOLD_CONFIG, "1.05");
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    KafkaAssignerDiskUsageDistributionGoal goal = new KafkaAssignerDiskUsageDistributionGoal(balancingConstraint);
    ClusterModel clusterModel = createClusterModel();

    double meanDiskUsage = clusterModel.load().expectedUtilizationFor(DISK) / clusterModel.capacityFor(DISK);
    assertTrue(goal.swapReplicas(clusterModel.broker(0),
                                 clusterModel.broker(1),
                                 meanDiskUsage,
                                 clusterModel,
                                 Collections.emptySet()));

    assertFalse(goal.swapReplicas(clusterModel.broker(0),
                                  clusterModel.broker(2),
                                  meanDiskUsage,
                                  clusterModel,
                                  Collections.emptySet()));

    assertTrue(goal.swapReplicas(clusterModel.broker(2),
                                 clusterModel.broker(3),
                                 meanDiskUsage,
                                 clusterModel,
                                 Collections.emptySet()));
  }

  @Test
  public void test() throws KafkaCruiseControlException {
    Properties props = CruiseControlUnitTestUtils.getCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(10L));
    props.setProperty(KafkaCruiseControlConfig.DISK_BALANCE_THRESHOLD_CONFIG, "1.05");
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    KafkaAssignerDiskUsageDistributionGoal goal = new KafkaAssignerDiskUsageDistributionGoal(balancingConstraint);
    ClusterModel clusterModel = createClusterModel();

    goal.optimize(clusterModel, Collections.emptySet(), Collections.emptySet());
    for (Broker b : clusterModel.brokers()) {
      System.out.println("Broker " + b.id() + " = " + b.load().expectedUtilizationFor(DISK));
    }
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

  private List<KafkaAssignerDiskUsageDistributionGoal.ReplicaWrapper> sortedReplicaAscend(Broker broker) {
    List<KafkaAssignerDiskUsageDistributionGoal.ReplicaWrapper> sortedReplicas = new ArrayList<>();
    List<Replica> replicasInDesc = broker.sortedReplicas(DISK);
    for (int i = replicasInDesc.size() - 1; i >= 0; i--) {
      Replica r = replicasInDesc.get(i);
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
   */
  private ClusterModel createClusterModel() throws ModelInputException {
    int numSnapshots = 2;
    if (!Load.initialized()) {
      Properties props = CruiseControlUnitTestUtils.getCruiseControlProperties();
      props.setProperty(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG, Integer.toString(numSnapshots));
      Load.init(new KafkaCruiseControlConfig(props));
    }

    Map<TopicPartition, Snapshot> snapshots = new HashMap<>();
    snapshots.put(T0P0, new Snapshot(0, 0.0, 0.0, 0.0, 10));
    snapshots.put(T0P1, new Snapshot(0, 0.0, 0.0, 0.0, 90));
    snapshots.put(T0P2, new Snapshot(0, 0.0, 0.0, 0.0, 20));
    snapshots.put(T1P0, new Snapshot(0, 0.0, 0.0, 0.0, 80));
    snapshots.put(T1P1, new Snapshot(0, 0.0, 0.0, 0.0, 30));
    snapshots.put(T1P2, new Snapshot(0, 0.0, 0.0, 0.0, 70));
    snapshots.put(T2P0, new Snapshot(0, 0.0, 0.0, 0.0, 40));
    snapshots.put(T2P1, new Snapshot(0, 0.0, 0.0, 0.0, 60));
    snapshots.put(T2P2, new Snapshot(0, 0.0, 0.0, 0.0, 50));

    final int numRacks = 4;
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0),
                                                 1.0);
    for (int i = 0; i < numRacks; i++) {
      clusterModel.createRack("r" + i);
    }

    int i = 0;
    for (; i < 2; i++) {
      clusterModel.createBroker("r0", "h" + i, i, TestConstants.BROKER_CAPACITY);
    }
    for (int j = 1; j < numRacks; j++, i++) {
      clusterModel.createBroker("r" + j, "h" + i, i, TestConstants.BROKER_CAPACITY);
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


    clusterModel.pushLatestSnapshot("r0", 0, T0P0, snapshots.get(T0P0).duplicate());
    clusterModel.pushLatestSnapshot("r0", 0, T1P2, snapshots.get(T1P2).duplicate());
    clusterModel.pushLatestSnapshot("r0", 0, T0P2, snapshots.get(T0P2).duplicate());
    clusterModel.pushLatestSnapshot("r0", 0, T2P1, snapshots.get(T2P1).duplicate());
    clusterModel.pushLatestSnapshot("r0", 0, T1P1, snapshots.get(T1P1).duplicate());

    clusterModel.pushLatestSnapshot("r0", 1, T0P1, snapshots.get(T0P1).duplicate());
    clusterModel.pushLatestSnapshot("r0", 1, T2P0, snapshots.get(T2P0).duplicate());
    clusterModel.pushLatestSnapshot("r0", 1, T1P0, snapshots.get(T1P0).duplicate());
    clusterModel.pushLatestSnapshot("r0", 1, T2P2, snapshots.get(T2P2).duplicate());

    clusterModel.pushLatestSnapshot("r1", 2, T0P2, snapshots.get(T0P2).duplicate());
    clusterModel.pushLatestSnapshot("r1", 2, T2P1, snapshots.get(T2P1).duplicate());
    clusterModel.pushLatestSnapshot("r1", 2, T0P1, snapshots.get(T0P1).duplicate());
    clusterModel.pushLatestSnapshot("r1", 2, T2P0, snapshots.get(T2P0).duplicate());
    clusterModel.pushLatestSnapshot("r1", 2, T1P0, snapshots.get(T1P0).duplicate());
    clusterModel.pushLatestSnapshot("r1", 2, T1P2, snapshots.get(T1P2).duplicate());

    clusterModel.pushLatestSnapshot("r2", 3, T1P0, snapshots.get(T1P0).duplicate());
    clusterModel.pushLatestSnapshot("r2", 3, T2P2, snapshots.get(T2P2).duplicate());
    clusterModel.pushLatestSnapshot("r2", 3, T1P1, snapshots.get(T1P1).duplicate());
    clusterModel.pushLatestSnapshot("r2", 3, T0P0, snapshots.get(T0P0).duplicate());
    clusterModel.pushLatestSnapshot("r2", 3, T0P2, snapshots.get(T0P2).duplicate());
    clusterModel.pushLatestSnapshot("r2", 3, T2P1, snapshots.get(T2P1).duplicate());

    clusterModel.pushLatestSnapshot("r3", 4, T1P1, snapshots.get(T1P1).duplicate());
    clusterModel.pushLatestSnapshot("r3", 4, T0P0, snapshots.get(T0P0).duplicate());
    clusterModel.pushLatestSnapshot("r3", 4, T1P2, snapshots.get(T1P2).duplicate());
    clusterModel.pushLatestSnapshot("r3", 4, T0P1, snapshots.get(T0P1).duplicate());
    clusterModel.pushLatestSnapshot("r3", 4, T2P0, snapshots.get(T2P0).duplicate());
    clusterModel.pushLatestSnapshot("r3", 4, T2P2, snapshots.get(T2P2).duplicate());
    return clusterModel;
  }
}
