/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;

public class KafkaClusterState extends AbstractCruiseControlResponse {
  protected static final String TOPIC = "topic";
  protected static final String PARTITION = "partition";
  protected static final String LEADER = "leader";
  protected static final String REPLICAS = "replicas";
  protected static final String IN_SYNC = "in-sync";
  protected static final String OUT_OF_SYNC = "out-of-sync";
  protected static final String OFFLINE = "offline";
  protected static final String URP = "urp";
  protected static final String OTHER = "other";
  protected static final String KAFKA_BROKER_STATE = "KafkaBrokerState";
  protected static final String KAFKA_PARTITION_STATE = "KafkaPartitionState";
  protected static final String LEADER_COUNT = "LeaderCountByBrokerId";
  protected static final String OUT_OF_SYNC_COUNT = "OutOfSyncCountByBrokerId";
  protected static final String REPLICA_COUNT = "ReplicaCountByBrokerId";
  protected Cluster _kafkaCluster;

  public KafkaClusterState(Cluster kafkaCluster, KafkaCruiseControlConfig config) {
    super(config);
    _kafkaCluster = kafkaCluster;
  }

  protected String getJSONString(CruiseControlParameters parameters) {
    Gson gson = new Gson();
    KafkaClusterStateParameters kafkaClusterStateParams = (KafkaClusterStateParameters) parameters;

    boolean isVerbose = kafkaClusterStateParams.isVerbose();
    Pattern topic = kafkaClusterStateParams.topic();
    Map<String, Object> jsonStructure = getJsonStructure(isVerbose, topic);
    jsonStructure.put(VERSION, JSON_VERSION);
    return gson.toJson(jsonStructure);
  }

  /**
   * Gather the Kafka partition state within the given under replicated, offline, and other partitions (if verbose).
   *
   * @param underReplicatedPartitions state of under replicated partitions.
   * @param offlinePartitions state of offline partitions.
   * @param otherPartitions state of partitions other than offline or urp.
   * @param verbose true if requested to gather state of partitions other than offline or urp.
   * @param topicPattern regex of topic to filter partition states by, is null if no filter is to be applied
   */
  protected void populateKafkaPartitionState(Set<PartitionInfo> underReplicatedPartitions,
                                           Set<PartitionInfo> offlinePartitions,
                                           Set<PartitionInfo> otherPartitions,
                                           boolean verbose,
                                           Pattern topicPattern) {
    for (String topic : _kafkaCluster.topics()) {
      if (topicPattern == null || topicPattern.matcher(topic).matches()) {
        for (PartitionInfo partitionInfo : _kafkaCluster.partitionsForTopic(topic)) {
          boolean isURP = partitionInfo.inSyncReplicas().length != partitionInfo.replicas().length;
          if (isURP || verbose) {
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
   */
  protected void populateKafkaBrokerState(Map<Integer, Integer> leaderCountByBrokerId,
                                        Map<Integer, Integer> outOfSyncCountByBrokerId,
                                        Map<Integer, Integer> replicaCountByBrokerId) {
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

        outOfSyncReplicas.forEach(brokerId -> outOfSyncCountByBrokerId.merge(brokerId, 1, Integer::sum));
        replicas.forEach(brokerId -> replicaCountByBrokerId.merge(brokerId, 1, Integer::sum));
      }
    }
    // Part-2: Gather the states of brokers without replicas.
    for (Node node : _kafkaCluster.nodes()) {
      int nodeId = node.id();
      if (replicaCountByBrokerId.get(nodeId) == null) {
        replicaCountByBrokerId.put(nodeId, 0);
        outOfSyncCountByBrokerId.put(nodeId, 0);
        leaderCountByBrokerId.put(nodeId, 0);
      }
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

      Map<String, Object> recordMap = new HashMap<>();
      recordMap.put(TOPIC, partitionInfo.topic());
      recordMap.put(PARTITION, partitionInfo.partition());
      recordMap.put(LEADER, partitionInfo.leader() == null ? -1 : partitionInfo.leader().id());
      recordMap.put(REPLICAS, replicas);
      recordMap.put(IN_SYNC, inSyncReplicas);
      recordMap.put(OUT_OF_SYNC, outOfSyncReplicas);
      partitionList.add(recordMap);
    }

    return partitionList;
  }

  /**
   * Return an object that can be further used to encode into JSON.
   *
   * @param verbose True if verbose, false otherwise.
   * @param topic Regex of topic to filter partition states by, is null if no filter is to be applied
   */
  public Map<String, Object> getJsonStructure(boolean verbose, Pattern topic) {
    Map<Integer, Integer> leaderCountByBrokerId = new HashMap<>();
    Map<Integer, Integer> outOfSyncCountByBrokerId = new HashMap<>();
    Map<Integer, Integer> replicaCountByBrokerId = new HashMap<>();

    populateKafkaBrokerState(leaderCountByBrokerId, outOfSyncCountByBrokerId, replicaCountByBrokerId);

    Map<String, Object> kafkaClusterByBrokerState = new HashMap<>();
    kafkaClusterByBrokerState.put(LEADER_COUNT, leaderCountByBrokerId);
    kafkaClusterByBrokerState.put(OUT_OF_SYNC_COUNT, outOfSyncCountByBrokerId);
    kafkaClusterByBrokerState.put(REPLICA_COUNT, replicaCountByBrokerId);

    // Gather the partition state.
    Set<PartitionInfo> underReplicatedPartitions = new HashSet<>();
    Set<PartitionInfo> offlinePartitions = new HashSet<>();
    Set<PartitionInfo> otherPartitions = new HashSet<>();

    populateKafkaPartitionState(underReplicatedPartitions, offlinePartitions, otherPartitions, verbose, topic);

    // Write the partition state.
    Map<String, List> kafkaClusterByPartitionState = new HashMap<>();
    kafkaClusterByPartitionState.put(OFFLINE, getJsonPartitions(offlinePartitions));
    kafkaClusterByPartitionState.put(URP, getJsonPartitions(underReplicatedPartitions));
    if (verbose) {
      kafkaClusterByPartitionState.put(OTHER, getJsonPartitions(otherPartitions));
    }

    Map<String, Object> cruiseControlState = new HashMap<>();
    cruiseControlState.put(KAFKA_BROKER_STATE, kafkaClusterByBrokerState);
    cruiseControlState.put(KAFKA_PARTITION_STATE, kafkaClusterByPartitionState);
    return cruiseControlState;
  }

  protected void writeKafkaClusterState(StringBuilder sb, SortedSet<PartitionInfo> partitions, int topicNameLength) {
    for (PartitionInfo partitionInfo : partitions) {
      List<String> replicas =
          Arrays.stream(partitionInfo.replicas()).map(Node::idString).collect(Collectors.toList());
      List<String> inSyncReplicas =
          Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::idString).collect(Collectors.toList());
      Set<String> outOfSyncReplicas = new HashSet<>(replicas);
      outOfSyncReplicas.removeAll(inSyncReplicas);

      sb.append(String.format("%" + topicNameLength + "s%10s%10s%40s%40s%30s%n",
                              partitionInfo.topic(),
                              partitionInfo.partition(),
                              partitionInfo.leader() == null ? -1 : partitionInfo.leader().id(),
                              replicas,
                              inSyncReplicas,
                              outOfSyncReplicas));
    }
  }

  protected String getPlaintext(CruiseControlParameters parameters) {
    StringBuilder sb = new StringBuilder();

    // Brokers summary.
    SortedMap<Integer, Integer> leaderCountByBrokerId = new TreeMap<>();
    SortedMap<Integer, Integer> outOfSyncCountByBrokerId = new TreeMap<>();
    SortedMap<Integer, Integer> replicaCountByBrokerId = new TreeMap<>();
    KafkaClusterStateParameters kafkaClusterStateParams = (KafkaClusterStateParameters) parameters;

    Pattern topic = kafkaClusterStateParams.topic();
    populateKafkaBrokerState(leaderCountByBrokerId, outOfSyncCountByBrokerId, replicaCountByBrokerId);

    String initMessage = "Brokers:";


    sb.append(String.format("%s%n%20s%20s%20s%20s%n", initMessage, "BROKER", "LEADER(S)", "REPLICAS", "OUT-OF-SYNC"));

    for (Integer brokerId : replicaCountByBrokerId.keySet()) {
      sb.append(String.format("%20d%20d%20d%20d%n",
                              brokerId,
                              leaderCountByBrokerId.getOrDefault(brokerId, 0),
                              replicaCountByBrokerId.getOrDefault(brokerId, 0),
                              outOfSyncCountByBrokerId.getOrDefault(brokerId, 0)));
    }

    // Partitions summary.
    int topicNameLength = _kafkaCluster.topics().stream().mapToInt(String::length).max().orElse(20) + 5;

    boolean verbose = kafkaClusterStateParams.isVerbose();
    initMessage = verbose ? "All Partitions in the Cluster (verbose):"
                          : "Under Replicated and Offline Partitions in the Cluster:";

    sb.append(String.format("%n%s%n%" + topicNameLength + "s%10s%10s%40s%40s%30s%n", initMessage, "TOPIC", "PARTITION",
                            "LEADER", "REPLICAS", "IN-SYNC", "OUT-OF-SYNC"));

    // Gather the cluster state.
    Comparator<PartitionInfo> comparator =
        Comparator.comparing(PartitionInfo::topic).thenComparingInt(PartitionInfo::partition);
    SortedSet<PartitionInfo> underReplicatedPartitions = new TreeSet<>(comparator);
    SortedSet<PartitionInfo> offlinePartitions = new TreeSet<>(comparator);
    SortedSet<PartitionInfo> otherPartitions = new TreeSet<>(comparator);

    populateKafkaPartitionState(underReplicatedPartitions, offlinePartitions, otherPartitions, verbose, topic);

    // Write the cluster state.
    sb.append(String.format("Offline Partitions:%n"));
    writeKafkaClusterState(sb, offlinePartitions, topicNameLength);

    sb.append(String.format("Under Replicated Partitions:%n"));
    writeKafkaClusterState(sb, underReplicatedPartitions, topicNameLength);

    if (verbose) {
      sb.append(String.format("Other Partitions:%n"));
      writeKafkaClusterState(sb, otherPartitions, topicNameLength);
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