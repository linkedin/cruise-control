/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for fetching Kafka cluster metadata using Kafka Admin APIs.
 *
 * This replaces the use of MetadataClient which relies on internal Kafka APIs.
 */
public class MetadataAdminClient {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataAdminClient.class);

  private final Admin _adminClient;

  /**
   * Creates a new MetadataAdminClient.
   *
   * @param adminClient The AdminClient to use for fetching cluster metadata.
   */
  public MetadataAdminClient(Admin adminClient) {
    _adminClient = adminClient;
  }

  /**
   * Close adminClient
   */
  public void close() {
    if (_adminClient != null) {
      try {
        _adminClient.close();
      } catch (Exception e) {
        LOG.warn("Failed to close AdminClient", e);
      }
    }
  }

  /**
   * Fetches the current metadata for the Kafka cluster.
   *
   * @return a {@link Cluster} containing the cluster ID, broker nodes, and partition information for all topics
   */
  public Cluster cluster() {
    try {
      Set<String> topicNames = _adminClient.listTopics().names().get();

      Map<String, TopicDescription> topicDescriptions = _adminClient.describeTopics(topicNames).allTopicNames().get();

      DescribeClusterResult describeResult = _adminClient.describeCluster();
      Collection<Node> nodes = describeResult.nodes().get();
      String clusterId = describeResult.clusterId().get();

      List<PartitionInfo> partitionInfos = new ArrayList<>();

      for (TopicDescription desc : topicDescriptions.values()) {
        for (TopicPartitionInfo partInfo : desc.partitions()) {

          Node leader = partInfo.leader();
          Node[] replicas = partInfo.replicas().toArray(Node[]::new);
          Node[] isr = partInfo.isr().toArray(Node[]::new);

          partitionInfos.add(new PartitionInfo(desc.name(), partInfo.partition(), leader, replicas, isr));
        }
      }

      return new Cluster(clusterId, nodes, partitionInfos, Collections.emptySet(), Collections.emptySet());
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error("Interrupted while fetching cluster metadata", e);
        throw new RuntimeException("Interrupted while fetching cluster metadata", e);

    } catch (ExecutionException e) {
        LOG.error("ExecutionException while fetching cluster metadata", e);
        // Could inspect e.getCause() and retry if RetriableException, but fail fast for now
        throw new RuntimeException("Failed to fetch cluster metadata", e);
    }
  }
}
