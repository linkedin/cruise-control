/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaPartitionLoadState {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaPartitionLoadState.class);
  private final List<Partition> _sortedPartitions;
  private final boolean _wantMaxLoad;
  private final int _entries;
  private final int _partitionUpperBoundary;
  private final int _partitionLowerBoundary;
  private final Pattern _topic;
  private final int _topicNameLength;
  private static final String VERSION = "version";
  private static final String TOPIC = "topic";
  private static final String PARTITION = "partition";
  private static final String LEADER = "leader";
  private static final String FOLLOWERS = "followers";
  private static final String MSG_IN = "msg_in";
  private static final String RECORDS = "records";

  public KafkaPartitionLoadState(List<Partition> sortedPartitions,
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


  /**
   * Write the partition load state to the given output stream.
   *
   * @param out Output stream to write the partition load state.
   */
  public void writeOutputStream(OutputStream out) {
    try {
      out.write(String.format("%" + _topicNameLength + "s%10s%30s%20s%20s%20s%20s%20s%n", "PARTITION", "LEADER", "FOLLOWERS",
                              "CPU (%)", "DISK (MB)", "NW_IN (KB/s)", "NW_OUT (KB/s)", "MSG_IN (#/s)")
                      .getBytes(StandardCharsets.UTF_8));
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
        out.write(String.format("%" + _topicNameLength + "s%10s%30s%19.6f%19.3f%19.3f%19.3f%19.3f%n",
                                p.leader().topicPartition(),
                                p.leader().broker().id(),
                                followers,
                                p.leader().load().expectedUtilizationFor(Resource.CPU, _wantMaxLoad),
                                p.leader().load().expectedUtilizationFor(Resource.DISK, _wantMaxLoad),
                                p.leader().load().expectedUtilizationFor(Resource.NW_IN, _wantMaxLoad),
                                p.leader().load().expectedUtilizationFor(Resource.NW_OUT, _wantMaxLoad),
                                p.leader().load().expectedUtilizationFor(KafkaMetricDef.MESSAGE_IN_RATE, _wantMaxLoad))
                        .getBytes(StandardCharsets.UTF_8));
      }
    } catch (IOException e) {
      LOG.error("Failed to write output stream.", e);
    }
  }

  public String getJSONString(int version) {
    Map<String, Object> partitionMap = new HashMap<>();
    List<Object> partitionList = new ArrayList<>();
    partitionMap.put(VERSION, version);
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
