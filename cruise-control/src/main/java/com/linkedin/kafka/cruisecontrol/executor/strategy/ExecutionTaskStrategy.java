/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import java.util.Collection;
import java.util.Map;
import java.util.SortedSet;
import org.apache.kafka.common.Cluster;


public interface ExecutionTaskStrategy {

  /**
   * Determine the execution order for tasks that moves data between brokers based on a customized strategy.
   *
   * @param executionTasks Execution tasks to be executed.
   * @param cluster The current cluster state.
   * @return Ordered set of tasks to be executed for each broker.
   */
  Map<Integer, SortedSet<ExecutionTask>> applyStrategy(Collection<ExecutionTask> executionTasks, Cluster cluster);
}
