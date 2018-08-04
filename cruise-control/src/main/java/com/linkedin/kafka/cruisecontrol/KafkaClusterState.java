/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.google.gson.Gson;
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
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;


public class KafkaClusterState {
  private Cluster _kafkaCluster;
  private static final String TOPIC = "topic";
  private static final String PARTITION = "partition";
  private static final String LEADER = "leader";
  private static final String REPLICAS = "replicas";
  private static final String IN_SYNC = "in-sync";
  private static final String OUT_OF_SYNC = "out-of-sync";
  private static final String OFFLINE = "offline";
  private static final String URP = "urp";
  private static final String OTHER = "other";
  private static final String KAFKA_BROKER_STATE = "KafkaBrokerState";
  private static final String KAFKA_PARTITION_STATE = "KafkaPartitionState";
  private static final String LEADER_COUNT = "LeaderCountByBrokerId";
  private static final String OUT_OF_SYNC_COUNT = "OutOfSyncCountByBrokerId";
  private static final String REPLICA_COUNT = "ReplicaCountByBrokerId";

  public KafkaClusterState(Cluster kafkaCluster) {
    _kafkaCluster = kafkaCluster;
  }

  public Cluster kafkaCluster() {
    return _kafkaCluster;
  }

  /**
   * Return a valid JSON encoded string
   *
   * @param version JSON version
   * @param verbose True if verbose, false otherwise.
   */
  public String getJSONString(int version, boolean verbose) {
    Gson gson = new Gson();
    Map<String, Object> jsonStructure = getJsonStructure(verbose);
    jsonStructure.put("version", version);
    return gson.toJson(jsonStructure);
  }

  /**
   * Gather the Kafka partition state within the given under replicated, offline, and other partitions (if verbose).
   *
   * @param underReplicatedPartitions state of under replicated partitions.
   * @param offlinePartitions state of offline partitions.
   * @param otherPartitions state of partitions other than offline or urp.
   * @param verbose true if requested to gather state of partitions other than offline or urp.
   */
  private void populateKafkaPartitionState(Set<PartitionInfo> underReplicatedPartitions,
                                           Set<PartitionInfo> offlinePartitions,
                                           Set<PartitionInfo> otherPartitions,
                                           boolean verbose) {
    for (String topic : _kafkaCluster.topics()) {
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

  /**
   * Gather the Kafka broker state within the given under leader, out-of-sync, and replica counts.
   *
   * @param leaderCountByBrokerId Leader count by broker id.
   * @param outOfSyncCountByBrokerId Out of sync replica count by broker id.
   * @param replicaCountByBrokerId Replica count by broker id.
   */
  private void populateKafkaBrokerState(Map<Integer, Integer> leaderCountByBrokerId,
                                        Map<Integer, Integer> outOfSyncCountByBrokerId,
                                        Map<Integer, Integer> replicaCountByBrokerId) {
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

        outOfSyncReplicas.forEach(brokerId -> outOfSyncCountByBrokerId.merge(brokerId, 1, Integer::sum));
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
   */
  public Map<String, Object> getJsonStructure(boolean verbose) {
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

    populateKafkaPartitionState(underReplicatedPartitions, offlinePartitions, otherPartitions, verbose);

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

  private void writeKafkaClusterState(OutputStream out, SortedSet<PartitionInfo> partitions, int topicNameLength)
      throws IOException {
    for (PartitionInfo partitionInfo : partitions) {
      Set<String> replicas =
          Arrays.stream(partitionInfo.replicas()).map(Node::idString).collect(Collectors.toSet());
      Set<String> inSyncReplicas =
          Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::idString).collect(Collectors.toSet());
      Set<String> outOfSyncReplicas = new HashSet<>(replicas);
      outOfSyncReplicas.removeAll(inSyncReplicas);

      out.write(String.format("%" + topicNameLength + "s%10s%10s%40s%40s%30s%n",
                              partitionInfo.topic(),
                              partitionInfo.partition(),
                              partitionInfo.leader() == null ? -1 : partitionInfo.leader().id(),
                              replicas,
                              inSyncReplicas,
                              outOfSyncReplicas)
                      .getBytes(StandardCharsets.UTF_8));
    }
  }

  public void writeOutputStream(OutputStream out, boolean verbose) throws IOException {
    Cluster clusterState = kafkaCluster();
    // Brokers summary.
    SortedMap<Integer, Integer> leaderCountByBrokerId = new TreeMap<>();
    SortedMap<Integer, Integer> outOfSyncCountByBrokerId = new TreeMap<>();
    SortedMap<Integer, Integer> replicaCountByBrokerId = new TreeMap<>();

    populateKafkaBrokerState(leaderCountByBrokerId, outOfSyncCountByBrokerId, replicaCountByBrokerId);

    String initMessage = "Brokers with replicas:";
    out.write(String.format("%s%n%20s%20s%20s%20s%n", initMessage, "BROKER", "LEADER(S)", "REPLICAS", "OUT-OF-SYNC")
                    .getBytes(StandardCharsets.UTF_8));

    for (Integer brokerId : replicaCountByBrokerId.keySet()) {
      out.write(String.format("%20d%20d%20d%20d%n",
                              brokerId,
                              leaderCountByBrokerId.getOrDefault(brokerId, 0),
                              replicaCountByBrokerId.getOrDefault(brokerId, 0),
                              outOfSyncCountByBrokerId.getOrDefault(brokerId, 0))
                      .getBytes(StandardCharsets.UTF_8));
    }

    // Partitions summary.
    int topicNameLength = clusterState.topics().stream().mapToInt(String::length).max().orElse(20) + 5;

    initMessage = verbose ? "All Partitions in the Cluster (verbose):"
                          : "Under Replicated and Offline Partitions in the Cluster:";
    out.write(String.format("%n%s%n%" + topicNameLength + "s%10s%10s%40s%40s%30s%n", initMessage, "TOPIC", "PARTITION",
                            "LEADER", "REPLICAS", "IN-SYNC", "OUT-OF-SYNC")
                    .getBytes(StandardCharsets.UTF_8));

    // Gather the cluster state.
    Comparator<PartitionInfo> comparator =
        Comparator.comparing(PartitionInfo::topic).thenComparingInt(PartitionInfo::partition);
    SortedSet<PartitionInfo> underReplicatedPartitions = new TreeSet<>(comparator);
    SortedSet<PartitionInfo> offlinePartitions = new TreeSet<>(comparator);
    SortedSet<PartitionInfo> otherPartitions = new TreeSet<>(comparator);

    populateKafkaPartitionState(underReplicatedPartitions, offlinePartitions, otherPartitions, verbose);

    // Write the cluster state.
    out.write(String.format("Offline Partitions:%n").getBytes(StandardCharsets.UTF_8));
    writeKafkaClusterState(out, offlinePartitions, topicNameLength);

    out.write(String.format("Under Replicated Partitions:%n").getBytes(StandardCharsets.UTF_8));
    writeKafkaClusterState(out, underReplicatedPartitions, topicNameLength);

    if (verbose) {
      out.write(String.format("Other Partitions:%n").getBytes(StandardCharsets.UTF_8));
      writeKafkaClusterState(out, otherPartitions, topicNameLength);
    }
  }
}