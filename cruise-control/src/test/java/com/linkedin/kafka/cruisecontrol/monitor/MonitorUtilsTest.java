/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC2;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.getCluster;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.NODE_0;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.NODE_1;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.NODE_2;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.nodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Unit tests for MonitorUtils.
 */
public class MonitorUtilsTest {
  @Test
  public void testMetadataChanged() {
    Node[] nodesWithOrder1 = {NODE_0, NODE_1};
    Node[] nodesWithOrder2 = {NODE_1, NODE_0};
    Node[] nodes2 = {NODE_0, NODE_2};
    // Cluster 1 just has one partition
    PartitionInfo t0p0 = new PartitionInfo(TOPIC0, 0, NODE_0, nodesWithOrder1, nodesWithOrder2);
    PartitionInfo t0p1 = new PartitionInfo(TOPIC0, 1, NODE_1, nodesWithOrder1, nodesWithOrder2);
    PartitionInfo t1p0 = new PartitionInfo(TOPIC1, 0, NODE_2, nodesWithOrder1, nodesWithOrder2);
    PartitionInfo t1p1 = new PartitionInfo(TOPIC1, 1, NODE_0, nodesWithOrder1, nodesWithOrder2);
    Set<PartitionInfo> partitions1 = new HashSet<>(Arrays.asList(t0p0, t0p1, t1p0, t1p1));
    Cluster cluster1 = new Cluster("cluster", Arrays.asList(NODE_0, NODE_1, NODE_2), partitions1,
                                   Collections.emptySet(), Collections.emptySet());

    Set<Integer> brokersWithReplicas = new HashSet<>();
    brokersWithReplicas.add(0);
    brokersWithReplicas.add(1);

    // Verify number of replicas and brokers with replicas in the cluster
    assertEquals(8, MonitorUtils.numReplicas(cluster1));
    assertEquals(brokersWithReplicas, MonitorUtils.brokersWithReplicas(cluster1));

    // Cluster2 has a new topic
    PartitionInfo t2p0 = new PartitionInfo(TOPIC2, 0, NODE_1, nodesWithOrder1, nodesWithOrder2);
    Cluster cluster2 = cluster1.withPartitions(Collections.singletonMap(new TopicPartition(TOPIC2, 0), t2p0));

    // Verify number of replicas and brokers with replicas in the cluster
    assertEquals(10, MonitorUtils.numReplicas(cluster2));
    assertEquals(brokersWithReplicas, MonitorUtils.brokersWithReplicas(cluster2));

    // A new partition.
    PartitionInfo t0p2 = new PartitionInfo(TOPIC0, 2, NODE_1, nodesWithOrder1, nodesWithOrder2);
    Cluster cluster3 = cluster1.withPartitions(Collections.singletonMap(new TopicPartition(TOPIC2, 2), t0p2));

    // Verify number of replicas and brokers with replicas in the cluster
    assertEquals(10, MonitorUtils.numReplicas(cluster3));
    assertEquals(brokersWithReplicas, MonitorUtils.brokersWithReplicas(cluster3));

    // An existing partition with different replica orders
    PartitionInfo t0p0DifferentOrder = new PartitionInfo(TOPIC0, 0, NODE_0, nodesWithOrder2, nodesWithOrder2);
    Cluster cluster4 = cluster1.withPartitions(Collections.singletonMap(new TopicPartition(TOPIC0, 0), t0p0DifferentOrder));

    // Verify number of replicas and brokers with replicas in the cluster
    assertEquals(8, MonitorUtils.numReplicas(cluster4));
    assertEquals(brokersWithReplicas, MonitorUtils.brokersWithReplicas(cluster4));

    // An existing partition with a different replica assignment
    PartitionInfo t0p0DifferentAssignment = new PartitionInfo(TOPIC0, 0, NODE_0, nodes2, nodesWithOrder2);
    Cluster cluster5 = cluster1.withPartitions(Collections.singletonMap(new TopicPartition(TOPIC0, 0), t0p0DifferentAssignment));

    // Verify number of replicas and brokers with replicas in the cluster
    brokersWithReplicas.add(2);
    assertEquals(8, MonitorUtils.numReplicas(cluster5));
    assertEquals(brokersWithReplicas, MonitorUtils.brokersWithReplicas(cluster5));

    // An existing partition with a different leader
    PartitionInfo t0p0DifferentLeader = new PartitionInfo(TOPIC0, 0, NODE_1, nodesWithOrder1, nodesWithOrder2);
    Cluster cluster6 = cluster1.withPartitions(Collections.singletonMap(new TopicPartition(TOPIC0, 0), t0p0DifferentLeader));

    // Verify number of replicas and brokers with replicas in the cluster
    brokersWithReplicas.remove(2);
    assertEquals(8, MonitorUtils.numReplicas(cluster6));
    assertEquals(brokersWithReplicas, MonitorUtils.brokersWithReplicas(cluster6));

    // An existing partition with the same cluster but different ISR
    PartitionInfo t0p0DifferentIsr = new PartitionInfo(TOPIC0, 0, NODE_0, nodesWithOrder1, new Node[]{NODE_0});
    Cluster cluster7 = cluster1.withPartitions(Collections.singletonMap(new TopicPartition(TOPIC0, 0), t0p0DifferentIsr));

    // Verify number of replicas and brokers with replicas in the cluster
    assertEquals(8, MonitorUtils.numReplicas(cluster7));
    assertEquals(brokersWithReplicas, MonitorUtils.brokersWithReplicas(cluster7));

    assertTrue(MonitorUtils.metadataChanged(cluster1, cluster2));
    assertTrue(MonitorUtils.metadataChanged(cluster1, cluster3));
    assertTrue(MonitorUtils.metadataChanged(cluster1, cluster4));
    assertTrue(MonitorUtils.metadataChanged(cluster1, cluster5));
    assertTrue(MonitorUtils.metadataChanged(cluster1, cluster6));
    assertFalse(MonitorUtils.metadataChanged(cluster1, cluster7));
  }

  @Test
  public void testHasPartitionsWithIsrGreaterThanReplicas() {
    Cluster cluster = getCluster(Arrays.asList(new TopicPartition(TOPIC0, 0), new TopicPartition(TOPIC0, 1)));

    // Verify: No signal when the cluster has all replicas in sync.
    assertFalse(MonitorUtils.hasPartitionsWithIsrGreaterThanReplicas(cluster));

    // Verify: No signal when the cluster has an URP.
    Node[] singletonNode0 = {NODE_0};
    PartitionInfo urp = new PartitionInfo(TOPIC1, 0, NODE_0, nodes(), singletonNode0);
    cluster = cluster.withPartitions(Collections.singletonMap(new TopicPartition(TOPIC1, 0), urp));
    assertFalse(MonitorUtils.hasPartitionsWithIsrGreaterThanReplicas(cluster));

    // Verify: Expect signal when the cluster has partitions with ISR greater than replicas.
    PartitionInfo badPartition = new PartitionInfo(TOPIC1, 1, NODE_0, singletonNode0, nodes());
    cluster = cluster.withPartitions(Collections.singletonMap(new TopicPartition(TOPIC1, 1), badPartition));
    assertTrue(MonitorUtils.hasPartitionsWithIsrGreaterThanReplicas(cluster));
  }
}
