/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;

import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The class that helps track the execution status for the balancing.
 * It does the following things:
 * <li>1. Keep track of the in progress partition movements between each pair of source-destination broker.
 * <li>2. When one partition movement finishes, it check the involved source and destination broker to see
 * if we can run more partition movements.
 * We only keep track of the number of concurrent partition movements but not the sizes of the partitions.
 * Because the concurrent level determines how much impact the balancing process would have on the involved
 * brokers. And the size of partitions only affect how long the impact would last.
 */
public class ExecutionTaskManager {
  private final Map<Integer, Integer> _inProgressPartMovementsByBrokerId;
  private final Set<ExecutionTask> _taskInProgress;
  private final ExecutionTaskPlanner _executionTaskPlanner;
  private final int _partitionMovementConcurrency;
  private final int _leaderMovementConcurrency;
  private final Set<Integer> _brokersToSkipConcurrencyCheck;

  /**
   * The constructor of The Execution task manager.
   *
   * @param partitionMovementConcurrency The maximum number of concurrent partition movements per broker.
   */
  public ExecutionTaskManager(int partitionMovementConcurrency, int leaderMovementConcurrency) {
    _inProgressPartMovementsByBrokerId = new HashMap<>();
    _taskInProgress = new HashSet<>();
    _executionTaskPlanner = new ExecutionTaskPlanner();
    _partitionMovementConcurrency = partitionMovementConcurrency;
    _leaderMovementConcurrency = leaderMovementConcurrency;
    _brokersToSkipConcurrencyCheck = new HashSet<>();
  }

  /**
   * Returns a list of balancing proposal that moves the partitions.
   */
  public List<ExecutionTask> getPartitionMovementTasks() {
    Map<Integer, Integer> readyBrokers = new HashMap<>();
    for (Map.Entry<Integer, Integer> entry : _inProgressPartMovementsByBrokerId.entrySet()) {
      // We skip the concurrency level check if caller requested so.
      // This is useful when we detected a broker failure and want to move all its partitions to the
      // rest of the brokers.
      if (_brokersToSkipConcurrencyCheck.contains(entry.getKey())) {
        readyBrokers.put(entry.getKey(), Integer.MAX_VALUE);
      } else {
        readyBrokers.put(entry.getKey(), Math.max(0, _partitionMovementConcurrency - entry.getValue()));
      }
    }
    return _executionTaskPlanner.getPartitionMovementTasks(readyBrokers);
  }

  /**
   * Returns a list of proposals that moves the leadership.
   */
  public List<ExecutionTask> getLeaderMovementTasks() {
    return _executionTaskPlanner.getLeaderMovementTasks(_leaderMovementConcurrency);
  }

  /**
   * Returns the remaining partition movement tasks.
   */
  public Set<ExecutionTask> remainingPartitionMovements() {
    return _executionTaskPlanner.remainingPartitionMovements();
  }

  /**
   * Returns the remaining leader movement tasks;
   */
  public Collection<ExecutionTask> remainingLeaderMovements() {
    return _executionTaskPlanner.remainingLeaderMovements();
  }

  /**
   * Returns the remaining data to move in MB.
   */
  public long remainingDataToMoveInMB() {
    return _executionTaskPlanner.remainingDataToMoveInMB();
  }

  /**
   * Check if there is any task in progress.
   */
  public boolean hasTaskInProgress() {
    return _taskInProgress.size() > 0;
  }

  /**
   * Get all the in progress execution tasks.
   */
  public Set<ExecutionTask> tasksInProgress() {
    return _taskInProgress;
  }

  /**
   * Add a collection of balancing proposals for execution. The method allows users to skip the concurrency check
   * on some given brokers. Notice that this method will replace the existing brokers that were in the concurrency
   * check privilege state with the new broker set.
   *
   * @param proposals the balancing proposals to execute.
   * @param brokersToSkipConcurrencyCheck the brokers that does not need to be throttled when move the partitions.
   */
  public void addBalancingProposals(Collection<BalancingProposal> proposals,
                                    Collection<Integer> brokersToSkipConcurrencyCheck) {
    _executionTaskPlanner.addBalancingProposals(proposals);
    for (BalancingProposal p : proposals) {
      if (!_inProgressPartMovementsByBrokerId.containsKey(p.sourceBrokerId())) {
        _inProgressPartMovementsByBrokerId.put(p.sourceBrokerId(), 0);
      }
      if (!_inProgressPartMovementsByBrokerId.containsKey(p.destinationBrokerId())) {
        _inProgressPartMovementsByBrokerId.put(p.destinationBrokerId(), 0);
      }
    }
    _brokersToSkipConcurrencyCheck.clear();
    if (brokersToSkipConcurrencyCheck != null) {
      _brokersToSkipConcurrencyCheck.addAll(brokersToSkipConcurrencyCheck);
    }
  }

  /**
   * Mark the given tasks as in progress.
   */
  public void markTasksInProgress(List<ExecutionTask> tasks) {
    for (ExecutionTask task : tasks) {
      _taskInProgress.add(task);
      if (task.proposal.balancingAction() == BalancingAction.REPLICA_MOVEMENT) {
        _inProgressPartMovementsByBrokerId.put(task.sourceBrokerId(),
                                               _inProgressPartMovementsByBrokerId.get(task.sourceBrokerId()) + 1);
        _inProgressPartMovementsByBrokerId.put(task.destinationBrokerId(),
                                               _inProgressPartMovementsByBrokerId.get(task.destinationBrokerId()) + 1);
      }
    }
  }

  /**
   * Mark a given tasks as completed.
   */
  public void completeTasks(List<ExecutionTask> tasks) {
    for (ExecutionTask task : tasks) {
      _taskInProgress.remove(task);
      if (task.proposal.balancingAction() == BalancingAction.REPLICA_MOVEMENT) {
        _inProgressPartMovementsByBrokerId.put(task.sourceBrokerId(),
                                               _inProgressPartMovementsByBrokerId.get(task.sourceBrokerId()) - 1);
        _inProgressPartMovementsByBrokerId.put(task.destinationBrokerId(),
                                               _inProgressPartMovementsByBrokerId.get(task.destinationBrokerId()) - 1);
      }
    }
  }

  public void clear() {
    _brokersToSkipConcurrencyCheck.clear();
    _inProgressPartMovementsByBrokerId.clear();
    _executionTaskPlanner.clear();
    _taskInProgress.clear();
  }

}
