/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  public void populateKafkaPartitionState(Set<PartitionInfo> underReplicatedPartitions,
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
  public void populateKafkaBrokerState(Map<Integer, Integer> leaderCountByBrokerId,
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
      Set<String> replicas =
          Arrays.stream(partitionInfo.replicas()).map(Node::idString).collect(Collectors.toSet());
      Set<String> inSyncReplicas =
          Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::idString).collect(Collectors.toSet());
      Set<String> outOfSyncReplicas = new HashSet<>(replicas);
      outOfSyncReplicas.removeAll(inSyncReplicas);

      Map<String, String> recordMap = new HashMap<>();
      recordMap.put(TOPIC, partitionInfo.topic());
      recordMap.put(PARTITION, Integer.toString(partitionInfo.partition()));
      recordMap.put(LEADER, Integer.toString(partitionInfo.leader() == null ? -1 : partitionInfo.leader().id()));
      recordMap.put(REPLICAS, replicas.toString());
      recordMap.put(IN_SYNC, inSyncReplicas.toString());
      recordMap.put(OUT_OF_SYNC, outOfSyncReplicas.toString());
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
}