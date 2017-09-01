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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Unit tests for MonitorUtils.
 */
public class MonitorUtilsTest {
  @Test
  public void testMetadataChanged() {
    Node node0 = new Node(0, "localhost", 100, "rack0");
    Node node1 = new Node(1, "localhost", 100, "rack1");
    Node node2 = new Node(2, "localhost", 100, "rack2");
    Node[] nodesWithOrder1 = {node0, node1};
    Node[] nodesWithOrder2 = {node1, node0};
    Node[] nodes2 = {node0, node2};
    String topic0 = "topic0";
    String topic1 = "topic1";
    String topic2 = "topic2";
    // Cluster 1 just has one partition
    PartitionInfo t0p0 = new PartitionInfo(topic0, 0, node0, nodesWithOrder1, nodesWithOrder2);
    PartitionInfo t0p1 = new PartitionInfo(topic0, 1, node1, nodesWithOrder1, nodesWithOrder2);
    PartitionInfo t1p0 = new PartitionInfo(topic1, 0, node2, nodesWithOrder1, nodesWithOrder2);
    PartitionInfo t1p1 = new PartitionInfo(topic1, 1, node0, nodesWithOrder1, nodesWithOrder2);
    Set<PartitionInfo> partitions1 = new HashSet<>();
    partitions1.addAll(Arrays.asList(t0p0, t0p1, t1p0, t1p1));
    Cluster cluster1 = new Cluster("cluster", Arrays.asList(node0, node1, node2), partitions1,
                                   Collections.emptySet(), Collections.emptySet());

    // Cluster2 has a new topic
    PartitionInfo t2p0 = new PartitionInfo(topic2, 0, node1, nodesWithOrder1, nodesWithOrder2);
    Cluster cluster2 = cluster1.withPartitions(Collections.singletonMap(new TopicPartition(topic2, 0), t2p0));

    // A new partition.
    PartitionInfo t0p2 = new PartitionInfo(topic0, 2, node1, nodesWithOrder1, nodesWithOrder2);
    Cluster cluster3 = cluster1.withPartitions(Collections.singletonMap(new TopicPartition(topic2, 2), t0p2));

    // A partition with different replica orders
    PartitionInfo t0p0DifferentOrder = new PartitionInfo(topic0, 0, node0, nodesWithOrder2, nodesWithOrder2);
    Cluster cluster4 = cluster1.withPartitions(Collections.singletonMap(new TopicPartition(topic0, 0), t0p0DifferentOrder));

    // A different replica assignment
    PartitionInfo t0p0DifferentAssignment = new PartitionInfo(topic0, 0, node0, nodes2, nodesWithOrder2);
    Cluster cluster5 = cluster1.withPartitions(Collections.singletonMap(new TopicPartition(topic0, 0), t0p0DifferentAssignment));

    // A different leader
    PartitionInfo t0p0DifferentLeader = new PartitionInfo(topic0, 0, node1, nodesWithOrder1, nodesWithOrder2);
    Cluster cluster6 = cluster1.withPartitions(Collections.singletonMap(new TopicPartition(topic0, 0), t0p0DifferentLeader));

    // The same cluster but different ISR
    PartitionInfo t0p0DifferentIsr = new PartitionInfo(topic0, 0, node0, nodesWithOrder1, new Node[]{node0});
    Cluster cluster7 = cluster1.withPartitions(Collections.singletonMap(new TopicPartition(topic0, 0), t0p0DifferentIsr));

    assertTrue(MonitorUtils.metadataChanged(cluster1, cluster2));
    assertTrue(MonitorUtils.metadataChanged(cluster1, cluster3));
    assertTrue(MonitorUtils.metadataChanged(cluster1, cluster4));
    assertTrue(MonitorUtils.metadataChanged(cluster1, cluster5));
    assertTrue(MonitorUtils.metadataChanged(cluster1, cluster6));
    assertFalse(MonitorUtils.metadataChanged(cluster1, cluster7));

  }
}
