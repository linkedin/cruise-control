/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;


public class PartitionLoadState extends AbstractCruiseControlResponse {
  private static final String TOPIC = "topic";
  private static final String PARTITION = "partition";
  private static final String LEADER = "leader";
  private static final String FOLLOWERS = "followers";
  private static final String MSG_IN = "msg_in";
  private static final String RECORDS = "records";
  private final List<Partition> _sortedPartitions;
  private final boolean _wantMaxLoad;
  private final int _entries;
  private final int _partitionUpperBoundary;
  private final int _partitionLowerBoundary;
  private final int _topicNameLength;
  private Pattern _topic;

  public PartitionLoadState(List<Partition> sortedPartitions,
                            boolean wantMaxLoad,
                            int entries,
                            int partitionUpperBoundary,
                            int partitionLowerBoundary,
                            Pattern topic,
                            int topicNameLength) {
    _sortedPartitions = sortedPartitions;
    _wantMaxLoad = wantMaxLoad;
    _entries = entries;
    _partitionUpperBoundary = partitionUpperBoundary;
    _partitionLowerBoundary = partitionLowerBoundary;
    _topic = topic;
    _topicNameLength = topicNameLength;
  }

  private String getPlaintext() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("%" + _topicNameLength + "s%10s%30s%20s%20s%20s%20s%20s%n", "PARTITION", "LEADER", "FOLLOWERS",
                            "CPU (%)", "DISK (MB)", "NW_IN (KB/s)", "NW_OUT (KB/s)", "MSG_IN (#/s)"));
    int numEntries = 0;
    for (Partition p : _sortedPartitions) {
      if ((_topic != null && !_topic.matcher(p.topicPartition().topic()).matches()) ||
          p.topicPartition().partition() < _partitionLowerBoundary ||
          p.topicPartition().partition() > _partitionUpperBoundary) {
        continue;
      }
      if (++numEntries > _entries) {
        break;
      }
      List<Integer> followers = p.followers().stream().map((replica) -> replica.broker().id()).collect(Collectors.toList());
      sb.append(String.format("%" + _topicNameLength + "s%10s%30s%19.6f%19.3f%19.3f%19.3f%19.3f%n",
                              p.leader().topicPartition(),
                              p.leader().broker().id(),
                              followers,
                              p.leader().load().expectedUtilizationFor(Resource.CPU, _wantMaxLoad),
                              p.leader().load().expectedUtilizationFor(Resource.DISK, _wantMaxLoad),
                              p.leader().load().expectedUtilizationFor(Resource.NW_IN, _wantMaxLoad),
                              p.leader().load().expectedUtilizationFor(Resource.NW_OUT, _wantMaxLoad),
                              p.leader().load().expectedUtilizationFor(KafkaMetricDef.MESSAGE_IN_RATE, _wantMaxLoad)));
    }
    return sb.toString();
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJSONString() : getPlaintext();
    // Discard irrelevant response.
    _sortedPartitions.clear();
    _topic = null;
  }

  private String getJSONString() {
    Map<String, Object> partitionMap = new HashMap<>();
    List<Object> partitionList = new ArrayList<>();
    partitionMap.put(VERSION, JSON_VERSION);
    int numEntries = 0;
    for (Partition p : _sortedPartitions) {
      if ((_topic != null && !_topic.matcher(p.topicPartition().topic()).matches()) ||
          p.topicPartition().partition() < _partitionLowerBoundary ||
          p.topicPartition().partition() > _partitionUpperBoundary) {
        continue;
      }
      if (++numEntries > _entries) {
        break;
      }
      List<Integer> followers = p.followers().stream().map((replica) -> replica.broker().id()).collect(
          Collectors.toList());
      Map<String, Object> record = new HashMap<>();
      record.put(TOPIC, p.leader().topicPartition().topic());
      record.put(PARTITION, p.leader().topicPartition().partition());
      record.put(LEADER, p.leader().broker().id());
      record.put(FOLLOWERS, followers);
      record.put(Resource.CPU.resource(), p.leader().load().expectedUtilizationFor(Resource.CPU, _wantMaxLoad));
      record.put(Resource.DISK.resource(), p.leader().load().expectedUtilizationFor(Resource.DISK, _wantMaxLoad));
      record.put(Resource.NW_IN.resource(), p.leader().load().expectedUtilizationFor(Resource.NW_IN, _wantMaxLoad));
      record.put(Resource.NW_OUT.resource(), p.leader().load().expectedUtilizationFor(Resource.NW_OUT, _wantMaxLoad));
      record.put(MSG_IN, p.leader().load().expectedUtilizationFor(KafkaMetricDef.MESSAGE_IN_RATE, _wantMaxLoad));
      partitionList.add(record);
    }
    partitionMap.put(RECORDS, partitionList);
    Gson gson = new Gson();
    return gson.toJson(partitionMap);
  }
}
