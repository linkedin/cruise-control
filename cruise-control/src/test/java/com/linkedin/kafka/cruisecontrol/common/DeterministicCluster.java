/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;


public class DeterministicCluster {

  private DeterministicCluster() {

  }

  // Two racks, three brokers, one partition, two replicas
  public static ClusterModel rackAwareSatisfiable() {
    List<Integer> orderedRackIdsOfBrokers = Arrays.asList(0, 0, 1);
    ClusterModel cluster = DeterministicCluster.getHomogeneousDeterministicCluster(2, orderedRackIdsOfBrokers,
                                                                                   TestConstants.BROKER_CAPACITY);

    // Create topic partition.
    TopicPartition pInfoT10 = new TopicPartition("T1", 0);

    // Create replicas for topic: T1.
    cluster.createReplica("0", 0, pInfoT10, 0, true);
    cluster.createReplica("0", 1, pInfoT10, 1, false);

    // Create snapshots and push them to the cluster.
    List<Long> windows = Collections.singletonList(1L);
    cluster.setReplicaLoad("0", 0, pInfoT10,
                           KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues(40.0, 100.0, 130.0, 75.0),
                           windows);
    cluster.setReplicaLoad("0", 1, pInfoT10,
                           KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues(5.0, 100.0, 0.0, 75.0),
                           windows);

    return cluster;
  }

  // two racks, three brokers, one partition, three replicas.
  public static ClusterModel rackAwareUnsatisfiable() {
    ClusterModel cluster = rackAwareSatisfiable();
    TopicPartition pInfoT10 = new TopicPartition("T1", 0);

    cluster.createReplica("1", 2, pInfoT10, 2, false);
    cluster.setReplicaLoad("1", 2, pInfoT10,
                           KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues(60.0, 100.0, 130.0, 75.0),
                           Collections.singletonList(1L));

    return cluster;
  }

  /**
   * Generates a small scale cluster.
   * <p>
   * <li>Number of Partitions: 10.</li>
   * <li>Topics: T1, T2</li>
   * <li>Replication factor/Topic: T1: 2, T2: 2</li>
   * <li>Partitions/Topic: T1: 6, T2: 4</li>
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T1_P1_leader, T1_P2_follower, T2_P3_leader, T2_P2_leader</li>
   * <li>B1: T1_P2_leader, T2_P1_leader, T2_P3_follower</li>
   * <li>B2: T2_P2_follower, T1_P1_follower, T2_P1_follower</li>
   * <p>
   * <h3>Load on Partitions</h3>
   * <p>
   * <ul>
   * <li>T1_P1_leader:</li>
   * <ul>
   * <li>CPU: 100.0</li>
   * <li>DISK: 75.0</li>
   * <li>INBOUND NW: 100.0</li>
   * <li>OUTBOUND NW: 130.0</li>
   * </ul>
   * <li>T1_P1_follower:</li>
   * <ul>
   * <li>CPU: 5.0</li>
   * <li>DISK: 75.0</li>
   * <li>INBOUND NW: 100.0</li>
   * <li>OUTBOUND NW: 0.0</li>
   * </ul>
   * <li>T1_P2_leader:</li>
   * <ul>
   * <li>CPU: 40.5</li>
   * <li>DISK: 55.0</li>
   * <li>INBOUND NW: 90.0</li>
   * <li>OUTBOUND NW: 110.0</li>
   * </ul>
   * <li>T1_P2_follower:</li>
   * <ul>
   * <li>CPU: 80.5</li>
   * <li>DISK: 55.0</li>
   * <li>INBOUND NW: 90.0</li>
   * <li>OUTBOUND NW: 0.0</li>
   * </ul>
   * <li>T2_P1_leader:</li>
   * <ul>
   * <li>CPU: 5.0</li>
   * <li>DISK: 5.0</li>
   * <li>INBOUND NW: 5.0</li>
   * <li>OUTBOUND NW: 6.0</li>
   * </ul>
   * <li>T2_P1_follower:</li>
   * <ul>
   * <li>CPU: 4.0</li>
   * <li>DISK: 5.0</li>
   * <li>INBOUND NW: 5.0</li>
   * <li>OUTBOUND NW: 0.0</li>
   * </ul>
   * <li>T2_P2_leader:</li>
   * <ul>
   * <li>CPU: 100.0</li>
   * <li>DISK: 55.0</li>
   * <li>INBOUND NW: 25.0</li>
   * <li>OUTBOUND NW: 45.0</li>
   * </ul>
   * <li>T2_P2_follower:</li>
   * <ul>
   * <li>CPU: 20.5</li>
   * <li>DISK: 55.0</li>
   * <li>INBOUND NW: 25.0</li>
   * <li>OUTBOUND NW: 0.0</li>
   * </ul>
   * <li>T2_P3_leader:</li>
   * <ul>
   * <li>CPU: 85.0</li>
   * <li>DISK: 95.0</li>
   * <li>INBOUND NW: 45.0</li>
   * <li>OUTBOUND NW: 120.0</li>
   * </ul>
   * <li>T2_P3_follower:</li>
   * <ul>
   * <li>CPU: 55.0</li>
   * <li>DISK: 95.0</li>
   * <li>INBOUND NW: 45.0</li>
   * <li>OUTBOUND NW: 0.0</li>
   * </ul>
   * </ul>
   *
   * @return Small scale cluster.
   */
  public static ClusterModel smallClusterModel(Map<Resource, Double> brokerCapacity) {
    List<Integer> orderedRackIdsOfBrokers = Arrays.asList(0, 0, 1);
    ClusterModel cluster = getHomogeneousDeterministicCluster(2, orderedRackIdsOfBrokers,
        brokerCapacity);

    // Create topic partition.
    TopicPartition pInfoT10 = new TopicPartition("T1", 0);
    TopicPartition pInfoT11 = new TopicPartition("T1", 1);
    TopicPartition pInfoT20 = new TopicPartition("T2", 0);
    TopicPartition pInfoT21 = new TopicPartition("T2", 1);
    TopicPartition pInfoT22 = new TopicPartition("T2", 2);
    // Create replicas for topic: T1.
    cluster.createReplica("0", 0, pInfoT10, 0, true);
    cluster.createReplica("1", 2, pInfoT10, 1, false);
    cluster.createReplica("0", 1, pInfoT11, 0, true);
    cluster.createReplica("0", 0, pInfoT11, 1, false);
    // Create replicas for topic: T2.
    cluster.createReplica("0", 1, pInfoT20, 0, true);
    cluster.createReplica("1", 2, pInfoT20, 1, false);
    cluster.createReplica("0", 0, pInfoT21, 0, true);
    cluster.createReplica("1", 2, pInfoT21, 1, false);
    cluster.createReplica("0", 0, pInfoT22, 0, true);
    cluster.createReplica("0", 1, pInfoT22, 1, false);

    // Create snapshots and push them to the cluster.
    List<Long> windows = Collections.singletonList(1L);
    cluster.setReplicaLoad("0", 0, pInfoT10, createLoad(100.0, 100.0, 130.0, 75.0), windows);
    cluster.setReplicaLoad("1", 2, pInfoT10, createLoad(5.0, 100.0, 0.0, 75.0), windows);
    cluster.setReplicaLoad("0", 1, pInfoT11, createLoad(40.5, 90.0, 110.0, 55.0), windows);
    cluster.setReplicaLoad("0", 0, pInfoT11, createLoad(80.5, 90.0, 0.0, 55.0), windows);
    cluster.setReplicaLoad("0", 1, pInfoT20, createLoad(5.0, 5.0, 6.0, 5.0), windows);
    cluster.setReplicaLoad("1", 2, pInfoT20, createLoad(4.0, 5.0, 0.0, 5.0), windows);
    cluster.setReplicaLoad("0", 0, pInfoT21, createLoad(100.0, 25.0, 45.0, 55.0), windows);
    cluster.setReplicaLoad("1", 2, pInfoT21, createLoad(20.5, 25.0, 0.0, 55.0), windows);
    cluster.setReplicaLoad("0", 0, pInfoT22, createLoad(85.0, 45.0, 120.0, 95.0), windows);
    cluster.setReplicaLoad("0", 1, pInfoT22, createLoad(55.0, 45.0, 0.0, 95.0), windows);

    return cluster;
  }

  /**
   * Generates a medium test cluster.
   * <p>
   * <li>Number of Partitions: 12.</li>
   * <li>Topics: A, B, C, D</li>
   * <li>Replication factor/Topic: A:2, B:2, C:2, D:2</li>
   * <li>Partitions/Topic: A: 6, B:2, C:2, D:2</li>
   *
   * @return A medium test cluster.
   */
  public static ClusterModel mediumClusterModel(Map<Resource, Double> brokerCapacity) {
    List<Integer> orderedRackIdsOfBrokers = Arrays.asList(0, 0, 1);
    ClusterModel cluster = getHomogeneousDeterministicCluster(2, orderedRackIdsOfBrokers,
        brokerCapacity);
    // Create topic partition.
    TopicPartition pInfoA0 = new TopicPartition("A", 0);
    TopicPartition pInfoA1 = new TopicPartition("A", 1);
    TopicPartition pInfoA2 = new TopicPartition("A", 2);
    TopicPartition pInfoB0 = new TopicPartition("B", 0);
    TopicPartition pInfoC0 = new TopicPartition("C", 0);
    TopicPartition pInfoD0 = new TopicPartition("D", 0);

    // Create replicas for TopicA.
    cluster.createReplica("0", 1, pInfoA0, 0, true);
    cluster.createReplica("0", 0, pInfoA0, 1, false);
    cluster.createReplica("0", 0, pInfoA1, 0, true);
    cluster.createReplica("1", 2, pInfoA1, 1, false);
    cluster.createReplica("0", 0, pInfoA2, 0, true);
    cluster.createReplica("1", 2, pInfoA2, 1, false);
    // Create replicas for TopicB.
    cluster.createReplica("0", 1, pInfoB0, 0, true);
    cluster.createReplica("1", 2, pInfoB0, 1, false);
    // Create replicas for TopicC.
    cluster.createReplica("1", 2, pInfoC0, 0, true);
    cluster.createReplica("0", 1, pInfoC0, 1, false);
    // Create replicas for TopicD.
    cluster.createReplica("0", 1, pInfoD0, 0, true);
    cluster.createReplica("1", 2, pInfoD0, 1, false);

    // Create snapshots and push them to the cluster.
    List<Long> windows = Collections.singletonList(1L);
    cluster.setReplicaLoad("0", 0, pInfoA0, createLoad(5.0, 5.0, 0.0, 4.0), windows);
    cluster.setReplicaLoad("0", 0, pInfoA1, createLoad(5.0, 3.0, 10.0, 8.0), windows);
    cluster.setReplicaLoad("0", 0, pInfoA2, createLoad(5.0, 2.0, 10.0, 6.0), windows);
    cluster.setReplicaLoad("0", 1, pInfoB0, createLoad(5.0, 4.0, 10.0, 7.0), windows);
    cluster.setReplicaLoad("0", 1, pInfoC0, createLoad(5.0, 6.0, 0.0, 4.0), windows);
    cluster.setReplicaLoad("0", 1, pInfoD0, createLoad(5.0, 5.0, 10.0, 6.0), windows);
    cluster.setReplicaLoad("0", 1, pInfoA0, createLoad(5.0, 4.0, 10.0, 10.0), windows);
    cluster.setReplicaLoad("1", 2, pInfoB0, createLoad(2.0, 2.0, 0.0, 5.0), windows);
    cluster.setReplicaLoad("1", 2, pInfoC0, createLoad(1.0, 8.0, 10.0, 4.0), windows);
    cluster.setReplicaLoad("1", 2, pInfoD0, createLoad(2.0, 8.0, 0.0, 7.0), windows);
    cluster.setReplicaLoad("1", 2, pInfoA1, createLoad(3.0, 4.0, 0.0, 6.0), windows);
    cluster.setReplicaLoad("1", 2, pInfoA2, createLoad(4.0, 5.0, 0.0, 3.0), windows);

    return cluster;
  }

  private static AggregatedMetricValues createLoad(double cpu, double networkIn, double networkOut, double disk) {
    return KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues(cpu, networkIn, networkOut, disk);
  }

  /**
   * Creates a deterministic cluster with the given number of racks and the broker distribution.
   *
   * @param numRacks                Number of racks in ToR architecture.
   * @param orderedRackIdsOfBrokers Specifies the rack id for each broker.
   * @param brokerCapacity          Healthy broker capacity.
   * @return Cluster with the specified number of racks and broker distribution.
   */
  public static ClusterModel getHomogeneousDeterministicCluster(int numRacks,
                                                                List<Integer> orderedRackIdsOfBrokers,
                                                                Map<Resource, Double> brokerCapacity) {
    int numBrokers = orderedRackIdsOfBrokers.size();
    // Sanity checks.
    if (numRacks > numBrokers || numBrokers <= 0 || numRacks <= 0 ||
        brokerCapacity.get(Resource.CPU) < 0 ||
        brokerCapacity.get(Resource.DISK) < 0 ||
        brokerCapacity.get(Resource.NW_IN) < 0 ||
        brokerCapacity.get(Resource.NW_OUT) < 0) {
      throw new IllegalArgumentException("Deterministic cluster generation failed due to bad input.");
    }

    // Create cluster.
    ClusterModel cluster = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    // Create racks and add them to cluster.
    for (int i = 0; i < numRacks; i++) {
      cluster.createRack(Integer.toString(i));
    }

    BrokerCapacityInfo commonBrokerCapacityInfo = new BrokerCapacityInfo(brokerCapacity);
    // Create brokers and assign a broker to each rack.
    int brokerId = 0;
    for (Integer rackId : orderedRackIdsOfBrokers) {
      cluster.createBroker(rackId.toString(), Integer.toString(brokerId), brokerId, commonBrokerCapacityInfo);
      brokerId++;
    }
    return cluster;
  }
}
