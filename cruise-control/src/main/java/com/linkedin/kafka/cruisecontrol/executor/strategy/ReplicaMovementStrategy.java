/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import org.apache.kafka.common.Cluster;


public interface ReplicaMovementStrategy {

  /**
   * Determine the execution order for replica movement tasks based on a customized strategy.
   *
   * @param replicaMovementTasks The replica movement tasks to be executed.
   * @param cluster The current cluster state.
   * @return Ordered set of tasks to be executed for each broker.
   */
  Map<Integer, SortedSet<ExecutionTask>> applyStrategy(Set<ExecutionTask> replicaMovementTasks, Cluster cluster);

  /**
   * Chain with another replica movement strategy to create a composite strategy.The returned strategy should use a combined rule
   * of two strategies in determining the task execution order.
   *
   * @param strategy The other replica movement strategy.
   * @return the composite replica movement strategy.
   */
  ReplicaMovementStrategy chain(ReplicaMovementStrategy strategy);

  /**
   * Generate a comparator for replica movement task which incorporate the strategy to apply. The "smaller" task will have
   * higher execution priority.
   *
   * @param cluster The current cluster state.
   * @return The comparator of task.
   */
  Comparator<ExecutionTask> taskComparator(Cluster cluster);
}
