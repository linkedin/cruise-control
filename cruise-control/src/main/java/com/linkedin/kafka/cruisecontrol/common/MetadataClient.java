/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.Cluster;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetadataClient {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataClient.class);
  private final AtomicInteger _metadataGeneration;
  private final Time _time;
  private final long _metadataTTLMs;
  private final AdminClient _adminClient;
  private final int _refreshMetadataTimeoutMs;

  private long _lastSuccessfulUpdateMs;
  private Cluster _cluster;

  long _version;

  public MetadataClient(KafkaCruiseControlConfig config,
                        long metadataTTLMs,
                        Time time) {
    this(config, metadataTTLMs, time, AdminClient.create(KafkaCruiseControlUtils.parseAdminClientConfigs(config)));
  }

  // for testing
  MetadataClient(KafkaCruiseControlConfig config,
                 long metadataTTLMs,
                 Time time,
                 AdminClient adminClient) {
    _metadataGeneration = new AtomicInteger(0);
    _refreshMetadataTimeoutMs = config.getInt(MonitorConfig.METADATA_MAX_AGE_CONFIG);
    _adminClient = adminClient;
    _time = time;
    _metadataTTLMs = metadataTTLMs;
    _version = 0;
    _lastSuccessfulUpdateMs = 0;
    _cluster = Cluster.empty();
  }

  /**
   * Refresh the metadata.
   * @return A new {@link ClusterAndGeneration} with latest cluster and generation.
   */
  public ClusterAndGeneration refreshMetadata() {
    return refreshMetadata(_refreshMetadataTimeoutMs);
  }

  /**
   * Refresh the metadata. Synchronized to prevent concurrent updates to the metadata cache
   * @return A new {@link ClusterAndGeneration} with latest cluster and generation.
   */
  public synchronized ClusterAndGeneration refreshMetadata(int timeoutMs) {
    // Do not update metadata if the metadata has just been refreshed.
    if (_time.milliseconds() >= _lastSuccessfulUpdateMs + _metadataTTLMs) {
      try {
        // Cruise Control always fetches metadata for all the topics.
        Cluster refreshedCluster = doRefreshMetadata(timeoutMs);
        _lastSuccessfulUpdateMs = _time.milliseconds();
        _version += 1;
        LOG.debug("Updated metadata {}", _cluster);
        if (MonitorUtils.metadataChanged(_cluster, refreshedCluster)) {
          _metadataGeneration.incrementAndGet();
          _cluster = refreshedCluster;
        }
      } catch (ExecutionException | TimeoutException | InterruptedException e) {
        LOG.warn("Exception while updating metadata", e);
        LOG.warn("Failed to update metadata in {}ms. Using old metadata with version {} and last successful update {}.",
            timeoutMs, _version, _lastSuccessfulUpdateMs);
      }
    }
    return new ClusterAndGeneration(_cluster, _metadataGeneration.get());
  }

  private Cluster doRefreshMetadata(int timeoutMs) throws InterruptedException, ExecutionException, TimeoutException {
    // We use both AdminClient timeout options and `future.get(timeout)`
    // The former ensures the timeouts are enforced in the brokers
    int remainingMs = timeoutMs;
    long startMs = _time.milliseconds();

    Set<String> topicNames = _adminClient.listTopics(new ListTopicsOptions().timeoutMs(remainingMs).listInternal(true))
        .names().get(remainingMs, TimeUnit.MILLISECONDS);
    long endMs = _time.milliseconds();
    long elapsedTimeMs = endMs - startMs;
    remainingMs -= elapsedTimeMs;

    DescribeClusterResult result = _adminClient.describeCluster(new DescribeClusterOptions().timeoutMs(remainingMs));
    Collection<Node> nodes = result.nodes().get(remainingMs, TimeUnit.MILLISECONDS);
    String clusterId = result.clusterId().get(remainingMs, TimeUnit.MILLISECONDS);
    Node controller = result.controller().get(remainingMs, TimeUnit.MILLISECONDS);
    elapsedTimeMs = _time.milliseconds() - endMs;
    remainingMs -= elapsedTimeMs;

    Map<String, TopicDescription> topicDescriptions = _adminClient.describeTopics(topicNames, new DescribeTopicsOptions().timeoutMs(remainingMs))
        .all().get(remainingMs, TimeUnit.MILLISECONDS);

    return cluster(clusterId, nodes, controller, topicDescriptions);
  }

  private Cluster cluster(String clusterId, Collection<Node> nodes, Node controller,
                          Map<String, TopicDescription> describeTopicResult) {
    List<PartitionInfo> partitionInfos = new LinkedList<>();
    Set<String> internalTopics = new HashSet<>();

    for (Map.Entry<String, TopicDescription> topicDescription : describeTopicResult.entrySet()) {
      if (topicDescription.getValue().isInternal()) {
        internalTopics.add(topicDescription.getKey());
      }

      for (TopicPartitionInfo topicPartitionInfo : topicDescription.getValue().partitions()) {
        PartitionInfo partitionInfo = new PartitionInfo(topicDescription.getKey(),
            topicPartitionInfo.partition(),
            topicPartitionInfo.leader(),
            topicPartitionInfo.replicas().toArray(new Node[0]),
            topicPartitionInfo.isr().toArray(new Node[0]),
            topicPartitionInfo.replicas()
                              .stream()
                              .filter(r -> !topicPartitionInfo.isr().contains(r))
                              .toArray(Node[]::new)
            );
        partitionInfos.add(partitionInfo);
      }
    }
    return new Cluster(clusterId, nodes, partitionInfos, Collections.emptySet(),
        internalTopics, controller);
  }

  /**
   * Close the admin client. Synchronized to avoid calling the admin client during a shutdown
   */
  public synchronized void close() {
    _adminClient.close();
  }

  /**
   * @return The current cluster and generation.
   */
  public ClusterAndGeneration clusterAndGeneration() {
    return new ClusterAndGeneration(cluster(), _metadataGeneration.get());
  }

  /**
   * @return The current cluster.
   */
  public Cluster cluster() {
    return _cluster;
  }

  public static class ClusterAndGeneration {
    private final Cluster _cluster;
    private final int _generation;

    public ClusterAndGeneration(Cluster cluster, int generation) {
      _cluster = cluster;
      _generation = generation;
    }

    /**
     * @return Kafka cluster.
     */
    public Cluster cluster() {
      return _cluster;
    }

    /**
     * @return Model generation of the cluster
     */
    public int generation() {
      return _generation;
    }
  }
}
