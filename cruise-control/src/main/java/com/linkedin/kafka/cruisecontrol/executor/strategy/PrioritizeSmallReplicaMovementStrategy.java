/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import java.util.Comparator;
import org.apache.kafka.common.Cluster;

/**
 * The strategy, which tries to first move replicas of small size partitions.
 */
public class PrioritizeSmallReplicaMovementStrategy extends AbstractReplicaMovementStrategy {

  @Override
  public Comparator<ExecutionTask> taskComparator(Cluster cluster) {
    return (task1, task2) -> (int) (task1.proposal().dataToMoveInMB() - task2.proposal().dataToMoveInMB());
  }
}