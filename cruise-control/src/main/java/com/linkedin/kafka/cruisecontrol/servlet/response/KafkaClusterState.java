/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.config.TopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.describeLogDirs;


public class KafkaClusterState extends AbstractCruiseControlResponse {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaClusterState.class);
  private static final long LOGDIR_RESPONSE_TIMEOUT_MS = 10000;
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
  private final Map<String, Properties> _allTopicConfigs;
  private final Properties _clusterConfigs;
  private final Map<String, Object> _adminClientConfigs;
  private Cluster _kafkaCluster;

  public KafkaClusterState(Cluster kafkaCluster,
                           TopicConfigProvider topicConfigProvider,
                           Map<String, Object> adminClientConfigs) {
    _kafkaCluster = kafkaCluster;
    _allTopicConfigs = topicConfigProvider.allTopicConfigs();
    _clusterConfigs = topicConfigProvider.clusterConfigs();
    _adminClientConfigs = adminClientConfigs;
  }

  private String getJSONString(CruiseControlParameters parameters) {
    Gson gson = new Gson();
    Map<String, Object> jsonStructure = null;
    try {
      jsonStructure = getJsonStructure(((KafkaClusterStateParameters) parameters).isVerbose());
      jsonStructure.put(VERSION, JSON_VERSION);
    }  catch (TimeoutException | InterruptedException | ExecutionException e) {
      LOG.error("Failed to populate broker logDir state.", e);
    }
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
    // Part-1: Gather the states of brokers with replicas.
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
    // Part-2: Gather the states of brokers without replicas.
    for (Node node : _kafkaCluster.nodes()) {
      int nodeId = node.id();
      if (replicaCountByBrokerId.get(nodeId) == null) {
        offlineReplicaCountByBrokerId.put(nodeId, 0);
        replicaCountByBrokerId.put(nodeId, 0);
        outOfSyncCountByBrokerId.put(nodeId, 0);
        leaderCountByBrokerId.put(nodeId, 0);
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
  public Map<String, Object> getJsonStructure(boolean verbose)
      throws ExecutionException, InterruptedException, TimeoutException {
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

  private void writeKafkaPartitionState(StringBuilder sb, SortedSet<PartitionInfo> partitions, int topicNameLength) {
    for (PartitionInfo partitionInfo : partitions) {
      Set<String> replicas =
          Arrays.stream(partitionInfo.replicas()).map(Node::idString).collect(Collectors.toSet());
      Set<String> inSyncReplicas =
          Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::idString).collect(Collectors.toSet());
      Set<String> outOfSyncReplicas = new HashSet<>(replicas);
      outOfSyncReplicas.removeAll(inSyncReplicas);
      Set<String> offlineReplicas =
          Arrays.stream(partitionInfo.offlineReplicas()).map(Node::idString).collect(Collectors.toSet());

      sb.append(String.format("%" + topicNameLength + "s%10s%10s%30s%30s%25s%25s%n",
                              partitionInfo.topic(),
                              partitionInfo.partition(),
                              partitionInfo.leader() == null ? -1 : partitionInfo.leader().id(),
                              replicas,
                              inSyncReplicas,
                              outOfSyncReplicas,
                              offlineReplicas));
    }
  }

  private void populateKafkaBrokerLogDirState(Map<Integer, Set<String>> onlineLogDirsByBrokerId,
                                              Map<Integer, Set<String>> offlineLogDirsByBrokerId,
                                              Set<Integer> brokers)
      throws ExecutionException, InterruptedException {
    Map<Integer, KafkaFuture<Map<String, LogDirInfo>>> logDirsByBrokerId = describeLogDirs(brokers, _adminClientConfigs).values();

    for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirInfo>>> entry : logDirsByBrokerId.entrySet()) {
      onlineLogDirsByBrokerId.put(entry.getKey(), new HashSet<>());
      offlineLogDirsByBrokerId.put(entry.getKey(), new HashSet<>());

      try {
        entry.getValue().get(LOGDIR_RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS).forEach((key, value) -> {
          if (value.error == Errors.NONE) {
            onlineLogDirsByBrokerId.get(entry.getKey()).add(key);
          } else {
            offlineLogDirsByBrokerId.get(entry.getKey()).add(key);
          }
        });
      } catch (TimeoutException te) {
        LOG.error("Getting log dir information for broker {} timed out.", entry.getKey());
        onlineLogDirsByBrokerId.get(entry.getKey()).add("timeout");
        offlineLogDirsByBrokerId.get(entry.getKey()).add("timeout");
      }
    }
  }

  private void writeKafkaBrokerLogDirState(StringBuilder sb, Set<Integer> brokers)
      throws ExecutionException, InterruptedException {
    Map<Integer, Set<String>> onlineLogDirsByBrokerId = new HashMap<>(brokers.size());
    Map<Integer, Set<String>> offlineLogDirsByBrokerId = new HashMap<>(brokers.size());
    populateKafkaBrokerLogDirState(onlineLogDirsByBrokerId, offlineLogDirsByBrokerId, brokers);

    int onlineLogDirsNameLength = onlineLogDirsByBrokerId.values().stream().mapToInt(dir -> dir.toString().length())
                                                         .max().orElse(20) + 14;
    int offlineLogDirsNameLength = offlineLogDirsByBrokerId.values().stream().mapToInt(dir -> dir.toString().length())
                                                           .max().orElse(20) + 15;

    String initMessage = "LogDirs of brokers with replicas:";
    sb.append(String.format("%n%s%n%20s%" + onlineLogDirsNameLength + "s%" + offlineLogDirsNameLength + "s%n",
                            initMessage, "BROKER", "ONLINE-LOGDIRS", "OFFLINE-LOGDIRS"));

    for (int brokerId : brokers) {
      sb.append(String.format("%20d%" + onlineLogDirsNameLength + "s%" + offlineLogDirsNameLength + "s%n",
                              brokerId,
                              onlineLogDirsByBrokerId.get(brokerId).toString(),
                              offlineLogDirsByBrokerId.get(brokerId).toString()));
    }
  }

  private void writeBrokerSummary(StringBuilder sb) throws ExecutionException, InterruptedException {
    SortedMap<Integer, Integer> leaderCountByBrokerId = new TreeMap<>();
    SortedMap<Integer, Integer> outOfSyncCountByBrokerId = new TreeMap<>();
    SortedMap<Integer, Integer> replicaCountByBrokerId = new TreeMap<>();
    SortedMap<Integer, Integer> offlineReplicaCountByBrokerId = new TreeMap<>();

    populateKafkaBrokerState(leaderCountByBrokerId,
                             outOfSyncCountByBrokerId,
                             replicaCountByBrokerId,
                             offlineReplicaCountByBrokerId);

    String initMessage = "Brokers:";
    sb.append(String.format("%s%n%20s%20s%20s%20s%20s%n", initMessage, "BROKER", "LEADER(S)", "REPLICAS", "OUT-OF-SYNC", "OFFLINE"));

    for (Integer brokerId : replicaCountByBrokerId.keySet()) {
      sb.append(String.format("%20d%20d%20d%20d%20d%n",
                              brokerId,
                              leaderCountByBrokerId.getOrDefault(brokerId, 0),
                              replicaCountByBrokerId.getOrDefault(brokerId, 0),
                              outOfSyncCountByBrokerId.getOrDefault(brokerId, 0),
                              offlineReplicaCountByBrokerId.getOrDefault(brokerId, 0)));
    }
    // Broker LogDirs Summary
    writeKafkaBrokerLogDirState(sb, replicaCountByBrokerId.keySet());
  }

  private void writePartitionSummary(StringBuilder sb, CruiseControlParameters parameters) {
    int topicNameLength = _kafkaCluster.topics().stream().mapToInt(String::length).max().orElse(20) + 5;
    boolean verbose = ((KafkaClusterStateParameters) parameters).isVerbose();

    String initMessage = verbose ? "All Partitions in the Cluster (verbose):"
                                 : "Under Replicated, Offline, and Under MinIsr Partitions:";
    sb.append(String.format("%n%s%n%" + topicNameLength + "s%10s%10s%30s%30s%25s%25s%n", initMessage, "TOPIC",
                            "PARTITION", "LEADER", "REPLICAS", "IN-SYNC", "OUT-OF-SYNC", "OFFLINE"));

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
    sb.append(String.format("Offline Partitions:%n"));
    writeKafkaPartitionState(sb, offlinePartitions, topicNameLength);

    sb.append(String.format("Partitions with Offline Replicas:%n"));
    writeKafkaPartitionState(sb, partitionsWithOfflineReplicas, topicNameLength);

    sb.append(String.format("Under Replicated Partitions:%n"));
    writeKafkaPartitionState(sb, underReplicatedPartitions, topicNameLength);

    sb.append(String.format("Under MinIsr Partitions:%n"));
    writeKafkaPartitionState(sb, underMinIsrPartitions, topicNameLength);

    if (verbose) {
      sb.append(String.format("Other Partitions:%n"));
      writeKafkaPartitionState(sb, otherPartitions, topicNameLength);
    }
  }

  private String getPlaintext(CruiseControlParameters parameters) {
    StringBuilder sb = new StringBuilder();
    try {
      // Broker summary.
      writeBrokerSummary(sb);
      // Partition summary.
      writePartitionSummary(sb, parameters);
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Failed to populate broker logDir state.", e);
    }

    return sb.toString();
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJSONString(parameters) : getPlaintext(parameters);
    // Discard irrelevant response.
    _kafkaCluster = null;
  }
}