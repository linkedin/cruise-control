/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.kafka.common.Cluster;


public abstract class AbstractExecutionTaskStrategy implements ExecutionTaskStrategy {

  protected abstract int compareTask(ExecutionTask task1, ExecutionTask task2);

  @Override
  public Map<Integer, SortedSet<ExecutionTask>> applyStrategy(Collection<ExecutionTask> executionTasks, Cluster cluster) {
    Map<Integer, SortedSet<ExecutionTask>> partMoveTaskByBrokerId = new HashMap<>();

    for (ExecutionTask task : executionTasks) {
      ExecutionProposal proposal = task.proposal();

      // Add the task to source broker's execution plan
      int sourceBroker = proposal.oldLeader();
      SortedSet<ExecutionTask> sourceBrokerTaskSet = partMoveTaskByBrokerId.computeIfAbsent(sourceBroker,
                                                                                            k -> new TreeSet<>(this::compareTask));
      sourceBrokerTaskSet.add(task);

      // Add the task to destination brokers' execution plan
      for (int destinationBroker : proposal.replicasToAdd()) {
        SortedSet<ExecutionTask> destinationBrokerTaskSet = partMoveTaskByBrokerId.computeIfAbsent(destinationBroker,
                                                                                                   k -> new TreeSet<>(this::compareTask));
        destinationBrokerTaskSet.add(task);
      }
    }
    return partMoveTaskByBrokerId;
  }
}
