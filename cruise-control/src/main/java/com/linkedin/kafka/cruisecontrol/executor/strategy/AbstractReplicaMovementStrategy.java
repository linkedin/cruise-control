/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.kafka.common.Cluster;

/**
 * An abstract class for replica movement strategy. This class will be extended to create custom strategy to determine the
 * execution order the replica movement tasks.
 */
public abstract class AbstractReplicaMovementStrategy implements ReplicaMovementStrategy {

  @Override
  public ReplicaMovementStrategy chain(ReplicaMovementStrategy strategy) {
    AbstractReplicaMovementStrategy current = this;
    return new AbstractReplicaMovementStrategy() {
      @Override
      public Comparator<ExecutionTask> taskComparator(Cluster cluster) {
        Comparator<ExecutionTask> comparator1 = current.taskComparator(cluster);
        Comparator<ExecutionTask> comparator2 = strategy.taskComparator(cluster);

        return (task1, task2) -> {
          int compareResult1 = comparator1.compare(task1, task2);
          return compareResult1 == 0 ? comparator2.compare(task1, task2) : compareResult1;
        };
      }

      @Override
      public String name() {
        return current.name() + "," + strategy.name();
      }
    };
  }

  @Override
  public Map<Integer, SortedSet<ExecutionTask>> applyStrategy(Set<ExecutionTask> replicaMovementTasks, Cluster cluster) {
    Map<Integer, SortedSet<ExecutionTask>> tasksByBrokerId = new HashMap<>();

    for (ExecutionTask task : replicaMovementTasks) {
      ExecutionProposal proposal = task.proposal();

      // Add the task to source broker's execution plan
      SortedSet<ExecutionTask> sourceBrokerTaskSet = tasksByBrokerId.computeIfAbsent(proposal.oldLeader().brokerId(),
                                                                                     k -> new TreeSet<>(taskComparator(cluster)));
      if (!sourceBrokerTaskSet.add(task)) {
        throw new IllegalStateException("Replica movement strategy " + this.getClass().getSimpleName() + " failed to determine order of tasks.");
      }

      // Add the task to destination brokers' execution plan
      for (ReplicaPlacementInfo destinationBroker : proposal.replicasToAdd()) {
        SortedSet<ExecutionTask> destinationBrokerTaskSet = tasksByBrokerId.computeIfAbsent(destinationBroker.brokerId(),
                                                                                            k -> new TreeSet<>(taskComparator(cluster)));
        if (!destinationBrokerTaskSet.add(task)) {
          throw new IllegalStateException("Replica movement strategy " + this.getClass().getSimpleName() + " failed to determine order of tasks.");
        }
      }
    }
    return tasksByBrokerId;
  }
}
