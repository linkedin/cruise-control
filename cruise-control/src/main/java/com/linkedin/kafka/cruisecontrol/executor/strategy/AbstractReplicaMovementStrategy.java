/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
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
    return new ReplicaMovementStrategy() {
      @Override
      public Map<Integer, SortedSet<ExecutionTask>> applyStrategy(Set<ExecutionTask> replicaMovementTasks, Cluster cluster) {
        return current.applyStrategy(replicaMovementTasks, cluster);
      }

      @Override
      public ReplicaMovementStrategy chain(ReplicaMovementStrategy strategy) {
        return current.chain(strategy);
      }

      @Override
      public Comparator<ExecutionTask> taskComparator(Cluster cluster) {
        Comparator<ExecutionTask> comparator1 = current.taskComparator(cluster);
        Comparator<ExecutionTask> comparator2 = strategy.taskComparator(cluster);

        return (task1, task2) -> {
          int compareResult1 = comparator1.compare(task1, task2);
          return compareResult1 == 0 ? comparator2.compare(task1, task2) : compareResult1;
        };
      }
    };
  }

  @Override
  public Map<Integer, SortedSet<ExecutionTask>> applyStrategy(Set<ExecutionTask> replicaMovementTasks, Cluster cluster) {
    Map<Integer, SortedSet<ExecutionTask>> tasksByBrokerId = new HashMap<>();

    for (ExecutionTask task : replicaMovementTasks) {
      ExecutionProposal proposal = task.proposal();

      // Add the task to source broker's execution plan
      int sourceBroker = proposal.oldLeader();
      SortedSet<ExecutionTask> sourceBrokerTaskSet = tasksByBrokerId.computeIfAbsent(sourceBroker,
                                                                                     k -> new TreeSet<>(taskComparator(cluster)));
      if (!sourceBrokerTaskSet.add(task)) {
        throw new IllegalStateException("Replica movement strategy " + this.getClass().getSimpleName() + " is unable to determine order of all tasks.");
      }

      // Add the task to destination brokers' execution plan
      for (int destinationBroker : proposal.replicasToAdd()) {
        SortedSet<ExecutionTask> destinationBrokerTaskSet = tasksByBrokerId.computeIfAbsent(destinationBroker,
                                                                                            k -> new TreeSet<>(taskComparator(cluster)));
        if (!destinationBrokerTaskSet.add(task)) {
          throw new IllegalStateException("Replica movement strategy " + this.getClass().getSimpleName() + " is unable to determine order of all tasks.");
        }
      }
    }
    return tasksByBrokerId;
  }
}
