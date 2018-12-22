/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import java.util.Comparator;
import org.apache.kafka.common.Cluster;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.isPartitionUnderReplicated;

/**
 * The strategy, which tries to first move replicas of partitions which currently are not under replicated.
 */
public class PostponeUrpReplicaMovementStrategy extends AbstractReplicaMovementStrategy {

  @Override
  public Comparator<ExecutionTask> taskComparator(Cluster cluster) {
    return (task1, task2) -> isPartitionUnderReplicated(cluster, task1.proposal().topicPartition()) ?
                             (isPartitionUnderReplicated(cluster, task2.proposal().topicPartition()) ? 0 : 1) :
                             (isPartitionUnderReplicated(cluster, task2.proposal().topicPartition()) ? -1 : 0);
  }
}