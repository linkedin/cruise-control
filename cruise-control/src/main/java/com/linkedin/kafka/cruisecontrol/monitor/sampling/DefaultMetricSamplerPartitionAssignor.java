/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The default implementation of metric sampler partition assignor.
 * <p>
 * The assignment tries to achieve the following goals:
 * 1. All the partitions of the same topic goes to one metric fetcher.
 * 2. The number of partitions assigned to each fetcher should be about the same.
 */
public class DefaultMetricSamplerPartitionAssignor implements MetricSamplerPartitionAssignor {

  private final static Logger LOG = LoggerFactory.getLogger(DefaultMetricSamplerPartitionAssignor.class);

  @Override
  public void configure(Map<String, ?> configs) {

  }

  @Override
  public List<Set<TopicPartition>> assignPartitions(Cluster cluster, int numMetricFetchers) {
    // Create an array to host the assignment of all the metric fetchers.
    List<Set<TopicPartition>> assignments = new ArrayList<>();
    for (int i = 0; i < numMetricFetchers; i++) {
      assignments.add(new HashSet<>());
    }
    int index = 0;
    // The total number of partitions that has been assigned.
    int totalPartitionAssigned = 0;
    for (String topic : cluster.topics()) {
      while (assignments.get(index % numMetricFetchers).size() > totalPartitionAssigned / numMetricFetchers) {
        index++;
      }
      Set<TopicPartition> assignmentForFetcher = assignments.get(index % numMetricFetchers);
      List<PartitionInfo> partitionsForTopic = cluster.partitionsForTopic(topic);
      for (PartitionInfo partitionInfo : partitionsForTopic) {
        assignmentForFetcher.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
      }
      totalPartitionAssigned += partitionsForTopic.size();
    }
    // Print the assignments if the logger is set to debug level or lower.
    maybeDumpAssignments(assignments);
    return assignments;
  }

  private void maybeDumpAssignments(List<Set<TopicPartition>> assignments) {
    int i = 0;
    LOG.trace("Partition assignment for metric fetchers:");
    for (Set<TopicPartition> assignment : assignments) {
      LOG.trace("Assignment {}: {}", i++, Arrays.toString(assignment.toArray()));
    }
  }
}
