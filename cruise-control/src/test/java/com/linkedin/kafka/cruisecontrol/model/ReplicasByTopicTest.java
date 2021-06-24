/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * Unit tests for verifying the consistency of number of replicas by topic in the cluster
 */
public class ReplicasByTopicTest {
  private static final String NEW_TOPIC = "NEW_TOPIC";
  private static final Map<Resource, Double> BROKER_CAPACITY = Map.of(Resource.CPU, TestConstants.LARGE_BROKER_CAPACITY,
                                                                      Resource.DISK, TestConstants.LARGE_BROKER_CAPACITY,
                                                                      Resource.NW_IN, TestConstants.LARGE_BROKER_CAPACITY,
                                                                      Resource.NW_OUT, TestConstants.MEDIUM_BROKER_CAPACITY);

  @Test
  public void testRelocateReplica() {
    ClusterModel smallReplicaMoveClusterModel = DeterministicCluster.smallClusterModel(BROKER_CAPACITY);
    TopicPartition pInfoT10 = new TopicPartition(DeterministicCluster.T1, 0);

    // Relocating a replica from a topic must not change its number of replicas in the cluster.
    int numTopicReplicasBefore = smallReplicaMoveClusterModel.numTopicReplicas(DeterministicCluster.T1);
    smallReplicaMoveClusterModel.relocateReplica(pInfoT10, 0, 1);
    int numTopicReplicasAfter = smallReplicaMoveClusterModel.numTopicReplicas(DeterministicCluster.T1);
    assertEquals(numTopicReplicasBefore, numTopicReplicasAfter);
  }

  @Test
  public void testRemoveReplica() {
    ClusterModel smallReplicaMoveClusterModel = DeterministicCluster.smallClusterModel(BROKER_CAPACITY);
    TopicPartition pInfoT10 = new TopicPartition(DeterministicCluster.T1, 0);

    int numTopicReplicasBefore = smallReplicaMoveClusterModel.numTopicReplicas(DeterministicCluster.T1);
    smallReplicaMoveClusterModel.removeReplica(0, pInfoT10);
    int numTopicReplicasAfter = smallReplicaMoveClusterModel.numTopicReplicas(DeterministicCluster.T1);
    assertEquals(numTopicReplicasBefore - 1, numTopicReplicasAfter);
  }

  @Test
  public void testCreateReplica() {
    ClusterModel smallReplicaMoveClusterModel = DeterministicCluster.smallClusterModel(BROKER_CAPACITY);

    TopicPartition pInfoNewTopic0 = new TopicPartition(NEW_TOPIC, 0);
    int numTopicReplicasBefore = smallReplicaMoveClusterModel.numTopicReplicas(NEW_TOPIC);
    smallReplicaMoveClusterModel.createReplica("0", 0, pInfoNewTopic0, 0, true);
    int numTopicReplicasAfter = smallReplicaMoveClusterModel.numTopicReplicas(NEW_TOPIC);
    assertEquals(numTopicReplicasBefore + 1, numTopicReplicasAfter);
  }

  @Test
  public void testDeleteReplica() {
    ClusterModel smallReplicaMoveClusterModel = DeterministicCluster.smallClusterModel(BROKER_CAPACITY);
    TopicPartition pInfoT10 = new TopicPartition(DeterministicCluster.T1, 0);

    int numTopicReplicasBefore = smallReplicaMoveClusterModel.numTopicReplicas(DeterministicCluster.T1);
    smallReplicaMoveClusterModel.deleteReplica(pInfoT10, 0);
    int numTopicReplicasAfter = smallReplicaMoveClusterModel.numTopicReplicas(DeterministicCluster.T1);
    assertEquals(numTopicReplicasBefore - 1, numTopicReplicasAfter);
  }
}
