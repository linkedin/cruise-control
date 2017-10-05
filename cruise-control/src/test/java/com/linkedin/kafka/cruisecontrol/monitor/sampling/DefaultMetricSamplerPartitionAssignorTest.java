/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link DefaultMetricSamplerPartitionAssignor}
 */
public class DefaultMetricSamplerPartitionAssignorTest {
  private static final String TOPIC_PREFIX = "topic-";
  private static final int NUM_TOPICS = 100;
  private static final int NUM_FETCHERS = 4;
  private final Random _random = new Random();

  /**
   * This is a pretty loose test because the default assignor is sort of doing a best effort job.
   */
  @Test
  public void testAssignment() {
    int maxNumPartitionsForTopic = -1;
    int totalNumPartitions = 0;
    // Prepare the metadata
    Node node0 = new Node(0, "localhost", 100, "rack0");
    Node node1 = new Node(1, "localhost", 100, "rack1");
    Node[] nodes = {node0, node1};
    Set<Node> allNodes = new HashSet<>();
    allNodes.add(node0);
    allNodes.add(node1);
    Set<PartitionInfo> partitions = new HashSet<>();
    for (int i = 0; i < NUM_TOPICS; i++) {
      // Random number of partitions ranging from 4 to 400
      int randomNumPartitions = 4 * (_random.nextInt(100) + 1);
      maxNumPartitionsForTopic = Math.max(randomNumPartitions, maxNumPartitionsForTopic);
      totalNumPartitions += randomNumPartitions;
      for (int j = 0; j < randomNumPartitions; j++) {
        partitions.add(new PartitionInfo(TOPIC_PREFIX + i, j, node0, nodes, nodes));
      }
    }
    Cluster cluster = new Cluster("clusterId", allNodes, partitions, Collections.emptySet(), Collections.emptySet());
    Metadata metadata = new Metadata();
    metadata.update(cluster, Collections.emptySet(), 0);

    MetricSamplerPartitionAssignor assignor = new DefaultMetricSamplerPartitionAssignor();
    List<Set<TopicPartition>> assignments = assignor.assignPartitions(metadata.fetch(), NUM_FETCHERS);

    int maxAssignedNumPartitionsForFetcher = -1;
    int minAssignedNumPartitionsForFetcher = Integer.MAX_VALUE;
    int totalAssignedNumPartitions = 0;
    Set<TopicPartition> uniqueAssignedPartitions = new HashSet<>();
    for (Set<TopicPartition> assignment : assignments) {
      maxAssignedNumPartitionsForFetcher = Math.max(maxAssignedNumPartitionsForFetcher, assignment.size());
      minAssignedNumPartitionsForFetcher = Math.min(minAssignedNumPartitionsForFetcher, assignment.size());
      uniqueAssignedPartitions.addAll(assignment);
      totalAssignedNumPartitions += assignment.size();
    }
    // Make sure all the partitions are assigned and there is no double assignment.
    assertEquals("Total assigned number of partitions should be " + totalNumPartitions,
        totalNumPartitions, totalAssignedNumPartitions);
    assertEquals("Total number of unique assigned partitions should be " + totalNumPartitions,
        totalNumPartitions, uniqueAssignedPartitions.size());

    int avgAssignedPartitionsPerFetcher = totalNumPartitions / NUM_FETCHERS;
    assertTrue("In the worst case the max number of partitions assigned to a metric fetchers should not differ by " +
            "more than the partition number of the biggest topic, which is " + maxNumPartitionsForTopic,
        maxAssignedNumPartitionsForFetcher - avgAssignedPartitionsPerFetcher <= maxNumPartitionsForTopic);
    assertTrue("In the worst case the min number of partitions assigned to a metric fetchers should not differ by " +
            "more than the partition number of the biggest topic, which is " + maxNumPartitionsForTopic,
        avgAssignedPartitionsPerFetcher - minAssignedNumPartitionsForFetcher <= maxNumPartitionsForTopic);
  }
}
