/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;


@JsonResponseClass
public class PartitionState {
  @JsonResponseField
  protected static final String TOPIC = "topic";
  @JsonResponseField
  protected static final String PARTITION = "partition";
  @JsonResponseField
  protected static final String LEADER = "leader";
  @JsonResponseField
  protected static final String REPLICAS = "replicas";
  @JsonResponseField
  protected static final String IN_SYNC = "in-sync";
  @JsonResponseField
  protected static final String OUT_OF_SYNC = "out-of-sync";
  @JsonResponseField
  protected static final String OFFLINE = "offline";
  @JsonResponseField
  protected static final String MIN_ISR = "min-isr";
  public static final String PARTITION_STATE_FORMAT_SUFFIX = "s%10s%10s%30s%30s%25s%25s%10s%n";
  protected final String _topic;
  protected final int _partition;
  protected final int _leader;
  protected final List<Integer> _replicas;
  protected final List<Integer> _inSyncReplicas;
  protected final Set<Integer> _outOfSyncReplicas;
  protected final Set<Integer> _offlineReplicas;
  protected final int _minIsr;

  public PartitionState(PartitionInfo partitionInfo, int minIsr) {
    _topic = partitionInfo.topic();
    _partition = partitionInfo.partition();
    _leader = partitionInfo.leader() == null ? -1 : partitionInfo.leader().id();
    _replicas = Arrays.stream(partitionInfo.replicas()).map(Node::id).collect(Collectors.toList());
    _inSyncReplicas = Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::id).collect(Collectors.toList());
    _outOfSyncReplicas = new HashSet<>(_replicas);
    _outOfSyncReplicas.removeAll(_inSyncReplicas);
    _offlineReplicas = Arrays.stream(partitionInfo.offlineReplicas()).map(Node::id).collect(Collectors.toSet());
    _minIsr = minIsr;
  }

  protected Map<String, Object> getJsonStructure() {
    return Map.of(TOPIC, _topic, PARTITION, _partition, LEADER, _leader, REPLICAS, _replicas, IN_SYNC, _inSyncReplicas,
                  OUT_OF_SYNC, _outOfSyncReplicas, OFFLINE, _offlineReplicas, MIN_ISR, _minIsr);
  }

  protected String writeKafkaPartitionState(int topicNameLength) {
    return String.format("%" + topicNameLength + PARTITION_STATE_FORMAT_SUFFIX,
                         _topic, _partition,
                         _leader, _replicas,
                         _inSyncReplicas,
                         _outOfSyncReplicas,
                         _offlineReplicas,
                         _minIsr);
  }
}
