/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.config.TopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.describeLogDirs;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;

public class KafkaClusterState extends AbstractCruiseControlResponse {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaClusterState.class);
  protected static final String TOPIC = "topic";
  protected static final String PARTITION = "partition";
  protected static final String LEADER = "leader";
  protected static final String REPLICAS = "replicas";
  protected static final String IN_SYNC = "in-sync";
  protected static final String OUT_OF_SYNC = "out-of-sync";
  protected static final String OFFLINE = "offline";
  protected static final String WITH_OFFLINE_REPLICAS = "with-offline-replicas";
  protected static final String URP = "urp";
  protected static final String UNDER_MIN_ISR = "under-min-isr";
  protected static final String OTHER = "other";
  protected static final String KAFKA_BROKER_STATE = "KafkaBrokerState";
  protected static final String KAFKA_PARTITION_STATE = "KafkaPartitionState";
  protected static final String LEADER_COUNT = "LeaderCountByBrokerId";
  protected static final String OUT_OF_SYNC_COUNT = "OutOfSyncCountByBrokerId";
  protected static final String REPLICA_COUNT = "ReplicaCountByBrokerId";
  protected static final String OFFLINE_REPLICA_COUNT = "OfflineReplicaCountByBrokerId";
  protected static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";
  protected static final String ONLINE_LOGDIRS = "OnlineLogDirsByBrokerId";
  protected static final String OFFLINE_LOGDIRS = "OfflineLogDirsByBrokerId";
  protected static final String IS_CONTROLLER = "IsController";
  protected static final int DEFAULT_MIN_INSYNC_REPLICAS = 1;
  protected final Map<String, Properties> _allTopicConfigs;
  protected final Properties _clusterConfigs;
  protected final Map<String, Object> _adminClientConfigs;
  protected Cluster _kafkaCluster;

  public KafkaClusterState(Cluster kafkaCluster,
                           TopicConfigProvider topicConfigProvider,
                           Map<String, Object> adminClientConfigs,
                           KafkaCruiseControlConfig config) {
    super(config);
    _kafkaCluster = kafkaCluster;
    _allTopicConfigs = topicConfigProvider.allTopicConfigs();
    _clusterConfigs = topicConfigProvider.clusterConfigs();
    _adminClientConfigs = adminClientConfigs;
  }

  protected String getJSONString(CruiseControlParameters parameters) {
    Gson gson = new Gson();
    Map<String, Object> jsonStructure;
    KafkaClusterStateParameters kafkaClusterStateParams = (KafkaClusterStateParameters) parameters;
    boolean isVerbose = kafkaClusterStateParams.isVerbose();
    Pattern topic = kafkaClusterStateParams.topic();
    try {
      jsonStructure = getJsonStructure(isVerbose, topic);
      jsonStructure.put(VERSION, JSON_VERSION);
    }  catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Failed to populate broker logDir state.", e);
    }
    return gson.toJson(jsonStructure);
  }

  /**
   * Get the effective config value of {@link #MIN_INSYNC_REPLICAS} for the given topic.
   *
   * @param topic Topic for which the {@link #MIN_INSYNC_REPLICAS} is queried
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
   * Gather the Kafka broker state within the given under leader, out-of-sync, and replica counts.
   *
   * @param leaderCountByBrokerId Leader count by broker id.
   * @param outOfSyncCountByBrokerId Out of sync replica count by broker id.
   * @param replicaCountByBrokerId Replica count by broker id.
   * @param offlineReplicaCountByBrokerId Offline replica count by broker id.
   * @param isControllerByBrokerId Controller information by broker id.
   */
  protected void populateKafkaBrokerState(Map<Integer, Integer> leaderCountByBrokerId,
                                          Map<Integer, Integer> outOfSyncCountByBrokerId,
                                          Map<Integer, Integer> replicaCountByBrokerId,
                                          Map<Integer, Integer> offlineReplicaCountByBrokerId,
                                          Map<Integer, Boolean> isControllerByBrokerId) {
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
    // Part-3: Gather controller information.
    replicaCountByBrokerId.keySet().forEach(brokerId -> isControllerByBrokerId.put(brokerId, false));
    Node controller = _kafkaCluster.controller();
    if (controller != null) {
      isControllerByBrokerId.put(controller.id(), true);
    }
  }

  protected List<Object> getJsonPartitions(Set<PartitionInfo> partitions) {
    List<Object> partitionList = new ArrayList<>();
    for (PartitionInfo partitionInfo : partitions) {
      List<Integer> replicas =
          Arrays.stream(partitionInfo.replicas()).map(Node::id).collect(Collectors.toList());
      List<Integer> inSyncReplicas =
          Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::id).collect(Collectors.toList());
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
   * @param topic Regex of topic to filter partition states by, is null if no filter is to be applied
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure(boolean verbose, Pattern topic)
      throws ExecutionException, InterruptedException {
    // Gather the broker state.
    Map<Integer, Integer> leaderCountByBrokerId = new HashMap<>();
    Map<Integer, Integer> outOfSyncCountByBrokerId = new HashMap<>();
    Map<Integer, Integer> replicaCountByBrokerId = new HashMap<>();
    Map<Integer, Integer> offlineReplicaCountByBrokerId = new HashMap<>();
    Map<Integer, Boolean> isControllerByBrokerId = new HashMap<>();

    populateKafkaBrokerState(leaderCountByBrokerId,
                             outOfSyncCountByBrokerId,
                             replicaCountByBrokerId,
                             offlineReplicaCountByBrokerId,
                             isControllerByBrokerId);

    Map<String, Object> kafkaClusterByBrokerState = new HashMap<>();
    kafkaClusterByBrokerState.put(LEADER_COUNT, leaderCountByBrokerId);
    kafkaClusterByBrokerState.put(OUT_OF_SYNC_COUNT, outOfSyncCountByBrokerId);
    kafkaClusterByBrokerState.put(REPLICA_COUNT, replicaCountByBrokerId);
    kafkaClusterByBrokerState.put(OFFLINE_REPLICA_COUNT, offlineReplicaCountByBrokerId);
    kafkaClusterByBrokerState.put(IS_CONTROLLER, isControllerByBrokerId);

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
                                verbose,
                                topic);

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

  protected void writeKafkaPartitionState(StringBuilder sb, SortedSet<PartitionInfo> partitions, int topicNameLength) {
    for (PartitionInfo partitionInfo : partitions) {
      List<String> replicas =
          Arrays.stream(partitionInfo.replicas()).map(Node::idString).collect(Collectors.toList());
      List<String> inSyncReplicas =
          Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::idString).collect(Collectors.toList());
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

  protected void populateKafkaBrokerLogDirState(Map<Integer, Set<String>> onlineLogDirsByBrokerId,
                                                Map<Integer, Set<String>> offlineLogDirsByBrokerId,
                                                Set<Integer> brokers)
      throws ExecutionException, InterruptedException {
    // If the broker does not show up in latest metadata, the broker is dead.
    Set<Integer> aliveBrokers = new HashSet<>(brokers.size());
    _kafkaCluster.nodes().forEach(node -> aliveBrokers.add(node.id()));
    for (Integer broker: brokers) {
      if (!aliveBrokers.contains(broker)) {
        onlineLogDirsByBrokerId.put(broker, Collections.singleton("broker_dead"));
        offlineLogDirsByBrokerId.put(broker, Collections.singleton("broker_dead"));
      }
    }

    Map<Integer, KafkaFuture<Map<String, LogDirInfo>>> logDirsByBrokerId = describeLogDirs(aliveBrokers, _adminClientConfigs).values();
    for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirInfo>>> entry : logDirsByBrokerId.entrySet()) {
      onlineLogDirsByBrokerId.put(entry.getKey(), new HashSet<>());
      offlineLogDirsByBrokerId.put(entry.getKey(), new HashSet<>());

      try {
        entry.getValue().get(_config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS).forEach((key, value) -> {
          if (value.error == Errors.NONE) {
            onlineLogDirsByBrokerId.get(entry.getKey()).add(key);
          } else {
            offlineLogDirsByBrokerId.get(entry.getKey()).add(key);
          }
        });
      } catch (TimeoutException te) {
        LOG.error("Getting log dir information for broker {} timed out.", entry.getKey());
        onlineLogDirsByBrokerId.get(entry.getKey()).add("timed_out");
        offlineLogDirsByBrokerId.get(entry.getKey()).add("timed_out");
      }
    }
  }

  protected void writeKafkaBrokerLogDirState(StringBuilder sb, Set<Integer> brokers)
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

  protected void writeBrokerSummary(StringBuilder sb) throws ExecutionException, InterruptedException {
    SortedMap<Integer, Integer> leaderCountByBrokerId = new TreeMap<>();
    SortedMap<Integer, Integer> outOfSyncCountByBrokerId = new TreeMap<>();
    SortedMap<Integer, Integer> replicaCountByBrokerId = new TreeMap<>();
    SortedMap<Integer, Integer> offlineReplicaCountByBrokerId = new TreeMap<>();
    Map<Integer, Boolean> isControllerByBrokerId = new TreeMap<>();

    populateKafkaBrokerState(leaderCountByBrokerId,
                             outOfSyncCountByBrokerId,
                             replicaCountByBrokerId,
                             offlineReplicaCountByBrokerId,
                             isControllerByBrokerId);

    String initMessage = "Brokers:";
    sb.append(String.format("%s%n%20s%20s%20s%20s%20s%20s%n", initMessage, "BROKER", "LEADER(S)", "REPLICAS", "OUT-OF-SYNC", "OFFLINE", "IS_CONTROLLER"));

    for (Integer brokerId : replicaCountByBrokerId.keySet()) {
      sb.append(String.format("%20d%20d%20d%20d%20d%20s%n",
                              brokerId,
                              leaderCountByBrokerId.getOrDefault(brokerId, 0),
                              replicaCountByBrokerId.getOrDefault(brokerId, 0),
                              outOfSyncCountByBrokerId.getOrDefault(brokerId, 0),
                              offlineReplicaCountByBrokerId.getOrDefault(brokerId, 0),
                              isControllerByBrokerId.get(brokerId)));
    }
    // Broker LogDirs Summary
    writeKafkaBrokerLogDirState(sb, replicaCountByBrokerId.keySet());
  }

  protected void writePartitionSummary(StringBuilder sb, CruiseControlParameters parameters) {
    int topicNameLength = _kafkaCluster.topics().stream().mapToInt(String::length).max().orElse(20) + 5;
    KafkaClusterStateParameters kafkaClusterStateParams = (KafkaClusterStateParameters) parameters;
    Pattern topic = kafkaClusterStateParams.topic();
    boolean verbose = kafkaClusterStateParams.isVerbose();

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
                                verbose,
                                topic);

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

  protected String getPlaintext(CruiseControlParameters parameters) {
    StringBuilder sb = new StringBuilder();
    try {
      // Broker summary.
      writeBrokerSummary(sb);
      // Partition summary.
      writePartitionSummary(sb, parameters);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Failed to populate broker logDir state.", e);
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