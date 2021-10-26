/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;


@JsonResponseClass
public class ClusterPartitionState {
  @JsonResponseField
  public static final String OFFLINE = "offline";
  @JsonResponseField
  public static final String WITH_OFFLINE_REPLICAS = "with-offline-replicas";
  @JsonResponseField
  public static final String URP = "urp";
  @JsonResponseField
  public static final String UNDER_MIN_ISR = "under-min-isr";
  @JsonResponseField(required = false)
  public static final String OTHER = "other";
  public static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";
  public static final int DEFAULT_MIN_INSYNC_REPLICAS = 1;

  protected final Set<PartitionInfo> _underReplicatedPartitions;
  protected final Set<PartitionInfo> _offlinePartitions;
  protected final Set<PartitionInfo> _otherPartitions;
  protected final Set<PartitionInfo> _partitionsWithOfflineReplicas;
  protected final Set<PartitionInfo> _underMinIsrPartitions;
  protected final Cluster _kafkaCluster;
  protected final Map<String, Properties> _allTopicConfigs;
  protected final Properties _clusterConfigs;
  protected final Map<String, Integer> _minIsrByTopic;

  public ClusterPartitionState(boolean verbose, Pattern topicPattern, Cluster kafkaCluster,
                               Map<String, Properties> allTopicConfigs, Properties clusterConfigs) {
    _kafkaCluster = kafkaCluster;
    _allTopicConfigs = allTopicConfigs;
    _clusterConfigs = clusterConfigs;
    Comparator<PartitionInfo> comparator = Comparator.comparing(PartitionInfo::topic).thenComparingInt(PartitionInfo::partition);
    _underReplicatedPartitions = new TreeSet<>(comparator);
    _offlinePartitions = new TreeSet<>(comparator);
    _otherPartitions = new TreeSet<>(comparator);
    _partitionsWithOfflineReplicas = new TreeSet<>(comparator);
    _underMinIsrPartitions = new TreeSet<>(comparator);
    _minIsrByTopic = new HashMap<>();
    // Gather the partition state.
    populateKafkaPartitionState(_underReplicatedPartitions, _offlinePartitions, _otherPartitions,
                                _partitionsWithOfflineReplicas, _underMinIsrPartitions, verbose, topicPattern);
  }

  /**
   * @return response with JSON structure.
   */
  public Map<String, Object> getJsonStructure() {
    // Write the partition state.
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put(OFFLINE, getJsonPartitions(_offlinePartitions));
    jsonMap.put(WITH_OFFLINE_REPLICAS, getJsonPartitions(_partitionsWithOfflineReplicas));
    jsonMap.put(URP, getJsonPartitions(_underReplicatedPartitions));
    jsonMap.put(UNDER_MIN_ISR, getJsonPartitions(_underMinIsrPartitions));
    if (!_otherPartitions.isEmpty()) {
      jsonMap.put(OTHER, getJsonPartitions(_otherPartitions));
    }
    return jsonMap;
  }

  /**
   * Gather the Kafka partition state within the given under replicated, offline, under minIsr,
   * and other partitions (if verbose).
   *
   * @param underReplicatedPartitions state of under replicated partitions.
   * @param offlinePartitions state of offline partitions.
   * @param otherPartitions state of partitions other than offline or urp.
   * @param partitionsWithOfflineReplicas state of partitions with offline replicas.
   * @param underMinIsrPartitions state of under min isr partitions.
   * @param verbose {@code true} if requested to gather state of partitions other than offline or urp.
   * @param topicPattern regex of topic to filter partition states by, is null if no filter is to be applied
   */
  protected void populateKafkaPartitionState(Set<PartitionInfo> underReplicatedPartitions,
                                             Set<PartitionInfo> offlinePartitions,
                                             Set<PartitionInfo> otherPartitions,
                                             Set<PartitionInfo> partitionsWithOfflineReplicas,
                                             Set<PartitionInfo> underMinIsrPartitions,
                                             boolean verbose,
                                             Pattern topicPattern) {
    for (String topic : _kafkaCluster.topics()) {
      if (topicPattern == null || topicPattern.matcher(topic).matches()) {
        int minInsyncReplicas = minInsyncReplicas(topic);
        _minIsrByTopic.put(topic, minInsyncReplicas);
        for (PartitionInfo partitionInfo : _kafkaCluster.partitionsForTopic(topic)) {
          int numInsyncReplicas = partitionInfo.inSyncReplicas().length;
          boolean isURP = numInsyncReplicas != partitionInfo.replicas().length;
          if (numInsyncReplicas < minInsyncReplicas) {
            underMinIsrPartitions.add(partitionInfo);
          }
          if (isURP || verbose) {
            boolean hasOfflineReplica = partitionInfo.offlineReplicas().length != 0;
            if (hasOfflineReplica) {
              partitionsWithOfflineReplicas.add(partitionInfo);
            }
            boolean isOffline = partitionInfo.inSyncReplicas().length == 0;
            if (isOffline) {
              offlinePartitions.add(partitionInfo);
            } else if (isURP) {
              underReplicatedPartitions.add(partitionInfo);
            } else {
              // verbose -- other
              otherPartitions.add(partitionInfo);
            }
          }
        }
      }
    }
  }

  /**
   * Get the effective config value of {@link #MIN_INSYNC_REPLICAS} for the given topic.
   *
   * @param topic Topic for which the {@link #MIN_INSYNC_REPLICAS} is queried
   * @return the effective config value of {@link #MIN_INSYNC_REPLICAS} for the given topic.
   */
  protected int minInsyncReplicas(String topic) {
    Properties topicLevelConfig = _allTopicConfigs.get(topic);
    if (topicLevelConfig != null && topicLevelConfig.get(MIN_INSYNC_REPLICAS) != null) {
      return Integer.parseInt(topicLevelConfig.getProperty(MIN_INSYNC_REPLICAS));
    } else if (_clusterConfigs != null && _clusterConfigs.get(MIN_INSYNC_REPLICAS) != null) {
      return Integer.parseInt(_clusterConfigs.getProperty(MIN_INSYNC_REPLICAS));
    } else {
      return DEFAULT_MIN_INSYNC_REPLICAS;
    }
  }

  protected List<Object> getJsonPartitions(Set<PartitionInfo> partitions) {
    List<Object> partitionList = new ArrayList<>();
    for (PartitionInfo partitionInfo : partitions) {
      partitionList.add(new PartitionState(partitionInfo, _minIsrByTopic.get(partitionInfo.topic())).getJsonStructure());
    }

    return partitionList;
  }

  /**
   * Write partition summary
   * @param sb String builder to write the response to.
   * @param verbose {@code true} if verbose, {@code false} otherwise.
   */
  public void writePartitionSummary(StringBuilder sb, boolean verbose) {
    Stream<String> topicStream;
    if (verbose) {
      topicStream = _kafkaCluster.topics().stream();
    } else {
      Set<PartitionInfo> topicPartitionInfos = new HashSet<>(_offlinePartitions);
      topicPartitionInfos.addAll(_partitionsWithOfflineReplicas);
      topicPartitionInfos.addAll(_underReplicatedPartitions);
      topicPartitionInfos.addAll(_underMinIsrPartitions);
      topicStream = topicPartitionInfos.stream().map(PartitionInfo::topic);
    }
    int topicNameLength = topicStream.mapToInt(String::length).max().orElse(20) + 5;

    String initMessage = verbose ? "All Partitions in the Cluster (verbose):"
                                 : "Under Replicated, Offline, and Under MinIsr Partitions:";
    sb.append(String.format("%n%s%n%" + topicNameLength + PartitionState.PARTITION_STATE_FORMAT_SUFFIX, initMessage, "TOPIC",
                            "PARTITION", "LEADER", "REPLICAS", "IN-SYNC", "OUT-OF-SYNC", "OFFLINE", "MIN-ISR"));

    // Write the cluster state.
    sb.append(String.format("Offline Partitions:%n"));
    writeKafkaPartitionState(sb, _offlinePartitions, topicNameLength);

    sb.append(String.format("Partitions with Offline Replicas:%n"));
    writeKafkaPartitionState(sb, _partitionsWithOfflineReplicas, topicNameLength);

    sb.append(String.format("Under Replicated Partitions:%n"));
    writeKafkaPartitionState(sb, _underReplicatedPartitions, topicNameLength);

    sb.append(String.format("Under MinIsr Partitions:%n"));
    writeKafkaPartitionState(sb, _underMinIsrPartitions, topicNameLength);

    if (verbose) {
      sb.append(String.format("Other Partitions:%n"));
      writeKafkaPartitionState(sb, _otherPartitions, topicNameLength);
    }
  }

  protected void writeKafkaPartitionState(StringBuilder sb, Set<PartitionInfo> partitions, int topicNameLength) {
    for (PartitionInfo partitionInfo : partitions) {
      sb.append(new PartitionState(partitionInfo, _minIsrByTopic.get(partitionInfo.topic())).writeKafkaPartitionState(topicNameLength));
    }
  }
}
