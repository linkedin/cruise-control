/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.config.TopicConfigProvider;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo;

import static java.lang.Math.max;


public class KafkaClusterState {
  private static final String TOPIC = "topic";
  private static final String PARTITION = "partition";
  private static final String LEADER = "leader";
  private static final String REPLICAS = "replicas";
  private static final String IN_SYNC = "in-sync";
  private static final String OUT_OF_SYNC = "out-of-sync";
  private static final String OFFLINE = "offline";
  private static final String WITH_OFFLINE_REPLICAS = "with-offline-replicas";
  private static final String URP = "urp";
  private static final String UNDER_MIN_ISR = "under-min-isr";
  private static final String OTHER = "other";
  private static final String KAFKA_BROKER_STATE = "KafkaBrokerState";
  private static final String KAFKA_PARTITION_STATE = "KafkaPartitionState";
  private static final String LEADER_COUNT = "LeaderCountByBrokerId";
  private static final String OUT_OF_SYNC_COUNT = "OutOfSyncCountByBrokerId";
  private static final String REPLICA_COUNT = "ReplicaCountByBrokerId";
  private static final String OFFLINE_REPLICA_COUNT = "OfflineReplicaCountByBrokerId";
  private static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";
  private static final String ONLINE_LOGDIRS = "OnlineLogDirsByBrokerId";
  private static final String OFFLINE_LOGDIRS = "OfflineLogDirsByBrokerId";
  private static final int DEFAULT_MIN_INSYNC_REPLICAS = 1;
  private final Cluster _kafkaCluster;
  private final Map<String, Properties> _allTopicConfigs;
  private final Properties _clusterConfigs;
  private final String _bootstrapServers;

  public KafkaClusterState(Cluster kafkaCluster, TopicConfigProvider topicConfigProvider, String bootstrapServers) {
    _kafkaCluster = kafkaCluster;
    _allTopicConfigs = topicConfigProvider.allTopicConfigs();
    _clusterConfigs = topicConfigProvider.clusterConfigs();
    _bootstrapServers = bootstrapServers;
  }

  /**
   * Return a valid JSON encoded string
   *
   * @param version JSON version
   * @param verbose True if verbose, false otherwise.
   */
  public String getJSONString(int version, boolean verbose) throws ExecutionException, InterruptedException {
    Gson gson = new Gson();
    Map<String, Object> jsonStructure = getJsonStructure(verbose);
    jsonStructure.put("version", version);
    return gson.toJson(jsonStructure);
  }

  /**
   * Get the effective config value of {@link #MIN_INSYNC_REPLICAS} for the given topic.
   *
   * @param topic Topic for which the {@link #MIN_INSYNC_REPLICAS} is queried
   */
  private int minInsyncReplicas(String topic) {
    Properties topicLevelConfig = _allTopicConfigs.get(topic);
    if (topicLevelConfig != null && topicLevelConfig.get(MIN_INSYNC_REPLICAS) != null) {
      return Integer.parseInt(topicLevelConfig.getProperty(MIN_INSYNC_REPLICAS));
    } else if (_clusterConfigs != null && _clusterConfigs.get(MIN_INSYNC_REPLICAS) != null) {
      return Integer.parseInt(_clusterConfigs.getProperty(MIN_INSYNC_REPLICAS));
    } else {
      return DEFAULT_MIN_INSYNC_REPLICAS;
    }
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
   * @param verbose true if requested to gather state of partitions other than offline or urp.
   */
  private void populateKafkaPartitionState(Set<PartitionInfo> underReplicatedPartitions,
                                           Set<PartitionInfo> offlinePartitions,
                                           Set<PartitionInfo> otherPartitions,
                                           Set<PartitionInfo> partitionsWithOfflineReplicas,
                                           Set<PartitionInfo> underMinIsrPartitions,
                                           boolean verbose) {
    for (String topic : _kafkaCluster.topics()) {
      int minInsyncReplicas = minInsyncReplicas(topic);
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

  /**
   * Gather the Kafka broker state within the given under leader, out-of-sync, and replica counts.
   *
   * @param leaderCountByBrokerId Leader count by broker id.
   * @param outOfSyncCountByBrokerId Out of sync replica count by broker id.
   * @param replicaCountByBrokerId Replica count by broker id.
   * @param offlineReplicaCountByBrokerId Offline replica count by broker id.
   */
  private void populateKafkaBrokerState(Map<Integer, Integer> leaderCountByBrokerId,
                                        Map<Integer, Integer> outOfSyncCountByBrokerId,
                                        Map<Integer, Integer> replicaCountByBrokerId,
                                        Map<Integer, Integer> offlineReplicaCountByBrokerId) {
    // Gather the broker states.
    for (String topic : _kafkaCluster.topics()) {
      for (PartitionInfo partitionInfo : _kafkaCluster.partitionsForTopic(topic)) {
        if (partitionInfo.leader() == null) {
          continue;
        }
        leaderCountByBrokerId.merge(partitionInfo.leader().id(), 1, Integer::sum);

        Set<Integer> replicas =
            Arrays.stream(partitionInfo.replicas()).map(Node::id).collect(Collectors.toSet());
        Set<Integer> inSyncReplicas =
            Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::id).collect(Collectors.toSet());
        Set<Integer> outOfSyncReplicas = new HashSet<>(replicas);
        outOfSyncReplicas.removeAll(inSyncReplicas);
        Set<Integer> offlineReplicas =
            Arrays.stream(partitionInfo.offlineReplicas()).map(Node::id).collect(Collectors.toSet());

        outOfSyncReplicas.forEach(brokerId -> outOfSyncCountByBrokerId.merge(brokerId, 1, Integer::sum));
        offlineReplicas.forEach(brokerId -> offlineReplicaCountByBrokerId.merge(brokerId, 1, Integer::sum));
        replicas.forEach(brokerId -> replicaCountByBrokerId.merge(brokerId, 1, Integer::sum));
      }
    }
  }

  private List<Object> getJsonPartitions(Set<PartitionInfo> partitions) {
    List<Object> partitionList = new ArrayList<>();
    for (PartitionInfo partitionInfo : partitions) {
      Set<Integer> replicas =
          Arrays.stream(partitionInfo.replicas()).map(Node::id).collect(Collectors.toSet());
      Set<Integer> inSyncReplicas =
          Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::id).collect(Collectors.toSet());
      Set<Integer> outOfSyncReplicas = new HashSet<>(replicas);
      outOfSyncReplicas.removeAll(inSyncReplicas);
      Set<Integer> offlineReplicas =
          Arrays.stream(partitionInfo.offlineReplicas()).map(Node::id).collect(Collectors.toSet());

      Map<String, Object> recordMap = new HashMap<>();
      recordMap.put(TOPIC, partitionInfo.topic());
      recordMap.put(PARTITION, partitionInfo.partition());
      recordMap.put(LEADER, partitionInfo.leader() == null ? -1 : partitionInfo.leader().id());
      recordMap.put(REPLICAS, replicas);
      recordMap.put(IN_SYNC, inSyncReplicas);
      recordMap.put(OUT_OF_SYNC, outOfSyncReplicas);
      recordMap.put(OFFLINE, offlineReplicas);
      partitionList.add(recordMap);
    }

    return partitionList;
  }

  /**
   * Return an object that can be further used to encode into JSON.
   *
   * @param verbose True if verbose, false otherwise.
   */
  public Map<String, Object> getJsonStructure(boolean verbose) throws ExecutionException, InterruptedException {
    // Gather the broker state.
    Map<Integer, Integer> leaderCountByBrokerId = new HashMap<>();
    Map<Integer, Integer> outOfSyncCountByBrokerId = new HashMap<>();
    Map<Integer, Integer> replicaCountByBrokerId = new HashMap<>();
    Map<Integer, Integer> offlineReplicaCountByBrokerId = new HashMap<>();

    populateKafkaBrokerState(leaderCountByBrokerId,
                             outOfSyncCountByBrokerId,
                             replicaCountByBrokerId,
                             offlineReplicaCountByBrokerId);

    Map<String, Object> kafkaClusterByBrokerState = new HashMap<>();
    kafkaClusterByBrokerState.put(LEADER_COUNT, leaderCountByBrokerId);
    kafkaClusterByBrokerState.put(OUT_OF_SYNC_COUNT, outOfSyncCountByBrokerId);
    kafkaClusterByBrokerState.put(REPLICA_COUNT, replicaCountByBrokerId);
    kafkaClusterByBrokerState.put(OFFLINE_REPLICA_COUNT, offlineReplicaCountByBrokerId);

    // Broker LogDirs Summary
    Map<Integer, Set<String>> onlineLogDirsByBrokerId = new HashMap<>(replicaCountByBrokerId.keySet().size());
    Map<Integer, Set<String>> offlineLogDirsByBrokerId = new HashMap<>(replicaCountByBrokerId.keySet().size());
    populateKafkaBrokerLogDirState(onlineLogDirsByBrokerId, offlineLogDirsByBrokerId, replicaCountByBrokerId.keySet());

    kafkaClusterByBrokerState.put(ONLINE_LOGDIRS, onlineLogDirsByBrokerId);
    kafkaClusterByBrokerState.put(OFFLINE_LOGDIRS, offlineLogDirsByBrokerId);

    // Gather the partition state.
    Set<PartitionInfo> underReplicatedPartitions = new HashSet<>();
    Set<PartitionInfo> offlinePartitions = new HashSet<>();
    Set<PartitionInfo> otherPartitions = new HashSet<>();
    Set<PartitionInfo> partitionsWithOfflineReplicas = new HashSet<>();
    Set<PartitionInfo> underMinIsrPartitions = new HashSet<>();

    populateKafkaPartitionState(underReplicatedPartitions,
                                offlinePartitions,
                                otherPartitions,
                                partitionsWithOfflineReplicas,
                                underMinIsrPartitions,
                                verbose);

    // Write the partition state.
    Map<String, List> kafkaClusterByPartitionState = new HashMap<>();
    kafkaClusterByPartitionState.put(OFFLINE, getJsonPartitions(offlinePartitions));
    kafkaClusterByPartitionState.put(WITH_OFFLINE_REPLICAS, getJsonPartitions(partitionsWithOfflineReplicas));
    kafkaClusterByPartitionState.put(URP, getJsonPartitions(underReplicatedPartitions));
    kafkaClusterByPartitionState.put(UNDER_MIN_ISR, getJsonPartitions(underMinIsrPartitions));
    if (verbose) {
      kafkaClusterByPartitionState.put(OTHER, getJsonPartitions(otherPartitions));
    }

    Map<String, Object> cruiseControlState = new HashMap<>();
    cruiseControlState.put(KAFKA_BROKER_STATE, kafkaClusterByBrokerState);
    cruiseControlState.put(KAFKA_PARTITION_STATE, kafkaClusterByPartitionState);
    return cruiseControlState;
  }

  private void writeKafkaPartitionState(OutputStream out, SortedSet<PartitionInfo> partitions, int topicNameLength)
      throws IOException {
    for (PartitionInfo partitionInfo : partitions) {
      Set<String> replicas =
          Arrays.stream(partitionInfo.replicas()).map(Node::idString).collect(Collectors.toSet());
      Set<String> inSyncReplicas =
          Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::idString).collect(Collectors.toSet());
      Set<String> outOfSyncReplicas = new HashSet<>(replicas);
      outOfSyncReplicas.removeAll(inSyncReplicas);
      Set<String> offlineReplicas =
          Arrays.stream(partitionInfo.offlineReplicas()).map(Node::idString).collect(Collectors.toSet());

      out.write(String.format("%" + topicNameLength + "s%10s%10s%30s%30s%25s%25s%n",
                              partitionInfo.topic(),
                              partitionInfo.partition(),
                              partitionInfo.leader() == null ? -1 : partitionInfo.leader().id(),
                              replicas,
                              inSyncReplicas,
                              outOfSyncReplicas,
                              offlineReplicas)
                      .getBytes(StandardCharsets.UTF_8));
    }
  }

  private void populateKafkaBrokerLogDirState(Map<Integer, Set<String>> onlineLogDirsByBrokerId,
                                              Map<Integer, Set<String>> offlineLogDirsByBrokerId,
                                              Set<Integer> brokers)
      throws ExecutionException, InterruptedException {
    Map<Integer, KafkaFuture<Map<String, LogDirInfo>>> logDirsByBrokerId
        = KafkaCruiseControlUtils.describeLogDirs(_bootstrapServers, brokers).values();

    for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirInfo>>> entry : logDirsByBrokerId.entrySet()) {
      onlineLogDirsByBrokerId.put(entry.getKey(), new HashSet<>());
      offlineLogDirsByBrokerId.put(entry.getKey(), new HashSet<>());

      entry.getValue().get().forEach((key, value) -> {
        if (value.error == Errors.NONE) {
          onlineLogDirsByBrokerId.get(entry.getKey()).add(key);
        } else {
          offlineLogDirsByBrokerId.get(entry.getKey()).add(key);
        }
      });
    }
  }

  private int getLogDirsNameLength(Map<Integer, Set<String>> logDirsByBrokerId) {
    int maxLogDirsNameLength = 0;
    for (Set<String> logDirs : logDirsByBrokerId.values()) {
      int logDirsNameLength = logDirs.toString().length();
      if (logDirsNameLength > maxLogDirsNameLength) {
        maxLogDirsNameLength = logDirsNameLength;
      }
    }
    return maxLogDirsNameLength;
  }

  private void writeKafkaBrokerLogDirState(OutputStream out, Set<Integer> brokers)
      throws IOException, ExecutionException, InterruptedException {
    Map<Integer, Set<String>> onlineLogDirsByBrokerId = new HashMap<>(brokers.size());
    Map<Integer, Set<String>> offlineLogDirsByBrokerId = new HashMap<>(brokers.size());
    populateKafkaBrokerLogDirState(onlineLogDirsByBrokerId, offlineLogDirsByBrokerId, brokers);

    int onlineLogDirsNameLength = max(26, getLogDirsNameLength(onlineLogDirsByBrokerId));
    int offlineLogDirsNameLength = max(27, getLogDirsNameLength(offlineLogDirsByBrokerId));

    String initMessage = "LogDirs of brokers with replicas:";
    out.write(String.format("%n%s%n%20s%" + onlineLogDirsNameLength + "s%" + offlineLogDirsNameLength + "s%n",
                            initMessage, "BROKER", "ONLINE-LOGDIRS", "OFFLINE-LOGDIRS").getBytes(StandardCharsets.UTF_8));

    for (int brokerId : brokers) {
      out.write(String.format("%20d%" + onlineLogDirsNameLength + "s%" + offlineLogDirsNameLength + "s%n",
                              brokerId,
                              onlineLogDirsByBrokerId.get(brokerId).toString(),
                              offlineLogDirsByBrokerId.get(brokerId).toString())
                      .getBytes(StandardCharsets.UTF_8));
    }
  }

  private void writeBrokerSummary(OutputStream out) throws IOException, ExecutionException, InterruptedException {
    SortedMap<Integer, Integer> leaderCountByBrokerId = new TreeMap<>();
    SortedMap<Integer, Integer> outOfSyncCountByBrokerId = new TreeMap<>();
    SortedMap<Integer, Integer> replicaCountByBrokerId = new TreeMap<>();
    SortedMap<Integer, Integer> offlineReplicaCountByBrokerId = new TreeMap<>();

    populateKafkaBrokerState(leaderCountByBrokerId,
                             outOfSyncCountByBrokerId,
                             replicaCountByBrokerId,
                             offlineReplicaCountByBrokerId);

    String initMessage = "Brokers with replicas:";
    out.write(String.format("%s%n%20s%20s%20s%20s%20s%n", initMessage, "BROKER", "LEADER(S)", "REPLICAS", "OUT-OF-SYNC", "OFFLINE")
                    .getBytes(StandardCharsets.UTF_8));

    for (Integer brokerId : replicaCountByBrokerId.keySet()) {
      out.write(String.format("%20d%20d%20d%20d%20d%n",
                              brokerId,
                              leaderCountByBrokerId.getOrDefault(brokerId, 0),
                              replicaCountByBrokerId.getOrDefault(brokerId, 0),
                              outOfSyncCountByBrokerId.getOrDefault(brokerId, 0),
                              offlineReplicaCountByBrokerId.getOrDefault(brokerId, 0))
                      .getBytes(StandardCharsets.UTF_8));
    }
    // Broker LogDirs Summary
    writeKafkaBrokerLogDirState(out, replicaCountByBrokerId.keySet());
  }

  private void writePartitionSummary(OutputStream out, boolean verbose) throws IOException {
    int topicNameLength = _kafkaCluster.topics().stream().mapToInt(String::length).max().orElse(20) + 5;

    String initMessage = verbose ? "All Partitions in the Cluster (verbose):"
                                 : "Under Replicated, Offline, and Under MinIsr Partitions:";
    out.write(String.format("%n%s%n%" + topicNameLength + "s%10s%10s%30s%30s%25s%25s%n", initMessage, "TOPIC",
                            "PARTITION", "LEADER", "REPLICAS", "IN-SYNC", "OUT-OF-SYNC", "OFFLINE")
                    .getBytes(StandardCharsets.UTF_8));

    // Gather the cluster state.
    Comparator<PartitionInfo> comparator =
        Comparator.comparing(PartitionInfo::topic).thenComparingInt(PartitionInfo::partition);
    SortedSet<PartitionInfo> underReplicatedPartitions = new TreeSet<>(comparator);
    SortedSet<PartitionInfo> offlinePartitions = new TreeSet<>(comparator);
    SortedSet<PartitionInfo> otherPartitions = new TreeSet<>(comparator);
    SortedSet<PartitionInfo> partitionsWithOfflineReplicas = new TreeSet<>(comparator);
    SortedSet<PartitionInfo> underMinIsrPartitions = new TreeSet<>(comparator);

    populateKafkaPartitionState(underReplicatedPartitions,
                                offlinePartitions,
                                otherPartitions,
                                partitionsWithOfflineReplicas,
                                underMinIsrPartitions,
                                verbose);

    // Write the cluster state.
    out.write(String.format("Offline Partitions:%n").getBytes(StandardCharsets.UTF_8));
    writeKafkaPartitionState(out, offlinePartitions, topicNameLength);

    out.write(String.format("Partitions with Offline Replicas:%n").getBytes(StandardCharsets.UTF_8));
    writeKafkaPartitionState(out, partitionsWithOfflineReplicas, topicNameLength);

    out.write(String.format("Under Replicated Partitions:%n").getBytes(StandardCharsets.UTF_8));
    writeKafkaPartitionState(out, underReplicatedPartitions, topicNameLength);

    out.write(String.format("Under MinIsr Partitions:%n").getBytes(StandardCharsets.UTF_8));
    writeKafkaPartitionState(out, underMinIsrPartitions, topicNameLength);

    if (verbose) {
      out.write(String.format("Other Partitions:%n").getBytes(StandardCharsets.UTF_8));
      writeKafkaPartitionState(out, otherPartitions, topicNameLength);
    }
  }

  public void writeOutputStream(OutputStream out, boolean verbose)
      throws IOException, ExecutionException, InterruptedException {
    // Broker summary.
    writeBrokerSummary(out);
    // Partition summary.
    writePartitionSummary(out, verbose);
  }
}