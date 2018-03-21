/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;


public class KafkaClusterState {
  private Cluster _kafkaCluster;

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
   * Return an object that can be further used to encode into JSON.
   *
   * @param verbose True if verbose, false otherwise.
   */
  public Map<String, Object> getJsonStructure(boolean verbose) {
    List<Object> partitionList = new ArrayList<>();
    for (String topic : _kafkaCluster.topics()) {
      for (PartitionInfo partitionInfo : _kafkaCluster.partitionsForTopic(topic)) {
        boolean isURP = partitionInfo.inSyncReplicas().length != partitionInfo.replicas().length;
        if (isURP || verbose) {
          Set<String> replicas =
              Arrays.stream(partitionInfo.replicas()).map(Node::idString).collect(Collectors.toSet());
          Set<String> isr =
              Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::idString).collect(Collectors.toSet());

          Map<String, String> recordMap = new HashMap<>();
          recordMap.put("topic", partitionInfo.topic());
          recordMap.put("partition", Integer.toString(partitionInfo.partition()));
          recordMap.put("leader", Integer.toString(partitionInfo.leader().id()));
          recordMap.put("replicas", replicas.toString());
          recordMap.put("isr", isr.toString());
          partitionList.add(recordMap);
        }
      }
    }

    Map<String, Object> cruiseControlState = new HashMap<>();
    cruiseControlState.put("KafkaCluster", partitionList);
    return cruiseControlState;
  }

  @Override
  public String toString() {
    List<String> partitionList = new ArrayList<>();

    _kafkaCluster.topics().forEach(topic -> _kafkaCluster.partitionsForTopic(topic).stream()
        .map(PartitionInfo::toString)
        .forEach(partitionList::add));

    return String.format("{KafkaCluster: %s}", partitionList);
  }
}