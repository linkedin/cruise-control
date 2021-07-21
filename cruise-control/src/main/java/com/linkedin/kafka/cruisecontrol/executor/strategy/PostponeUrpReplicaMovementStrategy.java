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
  public Comparator<ExecutionTask> taskComparator(StrategyOptions strategyOptions) {
    return (task1, task2) -> isPartitionUnderReplicated(strategyOptions.cluster(), task1.proposal().topicPartition())
                             ? (isPartitionUnderReplicated(strategyOptions.cluster(), task2.proposal().topicPartition()) ? PRIORITIZE_NONE
                                                                                                                         : PRIORITIZE_TASK_2)
                             : (isPartitionUnderReplicated(strategyOptions.cluster(), task2.proposal().topicPartition()) ? PRIORITIZE_TASK_1
                                                                                                                         : PRIORITIZE_NONE);
  }

  @Override
  public Comparator<ExecutionTask> taskComparator(Cluster cluster) {
    return taskComparator(new StrategyOptions.Builder(cluster).build());
  }

  /**
   * Get the name of this strategy. Name of a strategy provides an identification for the strategy in human readable format.
   */
  @Override
  public String name() {
    return PostponeUrpReplicaMovementStrategy.class.getSimpleName();
  }
}
