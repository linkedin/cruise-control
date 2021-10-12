/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.junit.Test;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.METADATA_REFRESH_BACKOFF;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.METADATA_EXPIRY_MS;
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
    Metadata metadata = new Metadata(METADATA_REFRESH_BACKOFF,
                                     METADATA_EXPIRY_MS,
                                     new LogContext(),
                                     new ClusterResourceListeners());

    Map<String, Set<PartitionInfo>> topicToTopicPartitions = new HashMap<>();
    for (PartitionInfo tp : partitions) {
      topicToTopicPartitions.putIfAbsent(tp.topic(), new HashSet<>());
      topicToTopicPartitions.get(tp.topic()).add(tp);
    }

    List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>(partitions.size());
    for (Map.Entry<String, Set<PartitionInfo>> entry : topicToTopicPartitions.entrySet()) {
      List<MetadataResponse.PartitionMetadata> partitionMetadata = new ArrayList<>(entry.getValue().size());
      for (PartitionInfo tp : entry.getValue()) {
        partitionMetadata.add(new MetadataResponse.PartitionMetadata(Errors.NONE, tp.partition(), NODE_0,
                                                                     Optional.of(RecordBatch.NO_PARTITION_LEADER_EPOCH),
                                                                     Arrays.asList(nodes()), Arrays.asList(nodes()),
                                                                     Collections.emptyList()));
      }
      topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, entry.getKey(), false, partitionMetadata));
    }

    MetadataResponse metadataResponse = KafkaCruiseControlUtils.prepareMetadataResponse(cluster.nodes(),
                                                                                        cluster.clusterResource().clusterId(),
                                                                                        MetadataResponse.NO_CONTROLLER_ID,
                                                                                        topicMetadata);
    metadata.update(KafkaCruiseControlUtils.REQUEST_VERSION_UPDATE, metadataResponse, 0);

    MetricSamplerPartitionAssignor assignor = new DefaultMetricSamplerPartitionAssignor();
    Set<TopicPartition> assignment = assignor.assignPartitions(metadata.fetch());

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
    assertTrue("In the worst case the max number of partitions assigned to a metric fetchers should not differ by "
               + "more than the partition number of the biggest topic, which is " + maxNumPartitionsForTopic,
        maxAssignedNumPartitionsForFetcher - avgAssignedPartitionsPerFetcher <= maxNumPartitionsForTopic);
    assertTrue("In the worst case the min number of partitions assigned to a metric fetchers should not differ by "
               + "more than the partition number of the biggest topic, which is " + maxNumPartitionsForTopic,
        avgAssignedPartitionsPerFetcher - minAssignedNumPartitionsForFetcher <= maxNumPartitionsForTopic);
  }
}
