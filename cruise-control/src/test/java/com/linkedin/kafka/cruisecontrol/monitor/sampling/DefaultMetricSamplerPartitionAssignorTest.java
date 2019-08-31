/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import java.util.Arrays;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.NODE_0;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.nodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link DefaultMetricSamplerPartitionAssignor}
 */
public class DefaultMetricSamplerPartitionAssignorTest {
  private static final String TOPIC_PREFIX = "topic-";
  private static final int NUM_TOPICS = 100;
  private static final Random RANDOM = new Random(0xDEADBEEF);

  /**
   * This is a pretty loose test because the default assignor is sort of doing a best effort job.
   */
  @Test
  public void testAssignment() {
    int maxNumPartitionsForTopic = -1;
    int totalNumPartitions = 0;
    // Prepare the metadata
    Set<PartitionInfo> partitions = new HashSet<>();
    for (int i = 0; i < NUM_TOPICS; i++) {
      // Random number of partitions ranging from 4 to 400
      int randomNumPartitions = 4 * (RANDOM.nextInt(100) + 1);
      maxNumPartitionsForTopic = Math.max(randomNumPartitions, maxNumPartitionsForTopic);
      totalNumPartitions += randomNumPartitions;
      for (int j = 0; j < randomNumPartitions; j++) {
        partitions.add(new PartitionInfo(TOPIC_PREFIX + i, j, NODE_0, nodes(), nodes()));
      }
    }
    Cluster cluster = new Cluster("cluster", Arrays.asList(nodes()), partitions, Collections.emptySet(), Collections.emptySet());
    MetricSamplerPartitionAssignor assignor = new DefaultMetricSamplerPartitionAssignor();
    Set<TopicPartition> assignment = assignor.assignPartitions(cluster);

    int maxAssignedNumPartitionsForFetcher = -1;
    int minAssignedNumPartitionsForFetcher = Integer.MAX_VALUE;
    int totalAssignedNumPartitions = 0;
    maxAssignedNumPartitionsForFetcher = Math.max(maxAssignedNumPartitionsForFetcher, assignment.size());
    minAssignedNumPartitionsForFetcher = Math.min(minAssignedNumPartitionsForFetcher, assignment.size());
    Set<TopicPartition> uniqueAssignedPartitions = new HashSet<>(assignment);
    totalAssignedNumPartitions += assignment.size();
    // Make sure all the partitions are assigned and there is no double assignment.
    assertEquals("Total assigned number of partitions should be " + totalNumPartitions,
        totalNumPartitions, totalAssignedNumPartitions);
    assertEquals("Total number of unique assigned partitions should be " + totalNumPartitions,
        totalNumPartitions, uniqueAssignedPartitions.size());

    int avgAssignedPartitionsPerFetcher = totalNumPartitions;
    assertTrue("In the worst case the max number of partitions assigned to a metric fetchers should not differ by " +
            "more than the partition number of the biggest topic, which is " + maxNumPartitionsForTopic,
        maxAssignedNumPartitionsForFetcher - avgAssignedPartitionsPerFetcher <= maxNumPartitionsForTopic);
    assertTrue("In the worst case the min number of partitions assigned to a metric fetchers should not differ by " +
            "more than the partition number of the biggest topic, which is " + maxNumPartitionsForTopic,
        avgAssignedPartitionsPerFetcher - minAssignedNumPartitionsForFetcher <= maxNumPartitionsForTopic);
  }
}
