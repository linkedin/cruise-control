/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.State;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.ExecutionTasksSummary;
/**
 * The class that helps track the execution status for the balancing.
 * It does the following things:
 * 1. Keep track of the in progress partition movements between each pair of source-destination broker.
 * 2. When one partition movement finishes, it check the involved source and destination broker to see
 * if we can run more partition movements.
 * We only keep track of the number of concurrent partition movements but not the sizes of the partitions.
 * Because the concurrent level determines how much impact the balancing process would have on the involved
 * brokers. And the size of partitions only affect how long the impact would last.
 *
 * The execution task manager is thread-safe.
 */
public class ExecutionTaskManager {
  private final Map<Integer, Integer> _inProgressReplicaMovementsByBrokerId;
  private final ExecutionTaskTracker _executionTaskTracker;
  private final Set<TopicPartition> _inProgressPartitions;
  private final ExecutionTaskPlanner _executionTaskPlanner;
  private final int _defaultPartitionMovementConcurrency;
  private final int _defaultLeadershipMovementConcurrency;
  private Integer _requestedPartitionMovementConcurrency;
  private Integer _requestedLeadershipMovementConcurrency;
  private final Set<Integer> _brokersToSkipConcurrencyCheck;
  private boolean _isKafkaAssignerMode;

  /**
   * The constructor of The Execution task manager.
   *
   * @param defaultPartitionMovementConcurrency The maximum number of concurrent partition movements per broker. It can
   *                                            be overwritten by user parameter upon post request.
   * @param defaultLeadershipMovementConcurrency The maximum number of concurrent leadership movements per batch. It can
   *                                             be overwritten by user parameter upon post request.
   * @param replicaMovementStrategies The strategies used to determine the execution order of replica movement tasks.
   * @param dropwizardMetricRegistry The metric registry.
   * @param time The time object to get the time.
   */
  public ExecutionTaskManager(int defaultPartitionMovementConcurrency,
                              int defaultLeadershipMovementConcurrency,
                              List<String> replicaMovementStrategies,
                              MetricRegistry dropwizardMetricRegistry,
                              Time time) {
    _inProgressReplicaMovementsByBrokerId = new HashMap<>();
    _inProgressPartitions = new HashSet<>();
    _executionTaskTracker = new ExecutionTaskTracker(dropwizardMetricRegistry, time);
    _executionTaskPlanner = new ExecutionTaskPlanner(replicaMovementStrategies);
    _defaultPartitionMovementConcurrency = defaultPartitionMovementConcurrency;
    _defaultLeadershipMovementConcurrency = defaultLeadershipMovementConcurrency;
    _brokersToSkipConcurrencyCheck = new HashSet<>();
    _isKafkaAssignerMode = false;
    _requestedPartitionMovementConcurrency = null;
    _requestedLeadershipMovementConcurrency = null;
  }

  /**
   * Dynamically set the partition movement concurrency per broker.
   *
   * @param requestedPartitionMovementConcurrency The maximum number of concurrent partition movements per broker
   *                                              (if null, use {@link #_defaultPartitionMovementConcurrency}).
   */
  public synchronized void setRequestedPartitionMovementConcurrency(Integer requestedPartitionMovementConcurrency) {
    _requestedPartitionMovementConcurrency = requestedPartitionMovementConcurrency;
  }

  /**
   * Dynamically set the leadership movement concurrency.
   *
   * @param requestedLeadershipMovementConcurrency The maximum number of concurrent leader movements
   *                                               (if null, {@link #_defaultLeadershipMovementConcurrency}).
   */
  public synchronized void setRequestedLeadershipMovementConcurrency(Integer requestedLeadershipMovementConcurrency) {
    _requestedLeadershipMovementConcurrency = requestedLeadershipMovementConcurrency;
  }

  public synchronized int partitionMovementConcurrency() {
    return _requestedPartitionMovementConcurrency == null ? _defaultPartitionMovementConcurrency
                                                          : _requestedPartitionMovementConcurrency;
  }

  public synchronized int leadershipMovementConcurrency() {
    return _requestedLeadershipMovementConcurrency == null ? _defaultLeadershipMovementConcurrency
                                                           : _requestedLeadershipMovementConcurrency;
  }

  /**
   * Returns a list of execution proposal that moves the partitions.
   */
  public synchronized List<ExecutionTask> getReplicaMovementTasks() {
    Map<Integer, Integer> brokersReadyForReplicaMovement = brokersReadyForReplicaMovement(_inProgressReplicaMovementsByBrokerId,
                                                                                          partitionMovementConcurrency());
    return _executionTaskPlanner.getReplicaMovementTasks(brokersReadyForReplicaMovement, _inProgressPartitions);

  }

  /**
   * Based on replica movement concurrency requirement and number of ongoing replica movements, calculate how many
   * new replica movements can be triggered on each broker.
   *
   * @param inProgressReplicaMovementsByBrokerId Number of ongoing replica movements in each broker.
   * @param movementConcurrency The required per-broker replica movement concurrency.
   * @return A map of how many new replica movements can be triggered for each broker.
   */
  private Map<Integer, Integer> brokersReadyForReplicaMovement(Map<Integer, Integer> inProgressReplicaMovementsByBrokerId,
                                                               int movementConcurrency) {
    Map<Integer, Integer> readyBrokers = new HashMap<>(inProgressReplicaMovementsByBrokerId.size());
    for (Map.Entry<Integer, Integer> entry : inProgressReplicaMovementsByBrokerId.entrySet()) {
      // We skip the concurrency level check if caller requested so.
      // This is useful when we detected a broker failure and want to move all its partitions to the
      // rest of the brokers.
      if (_brokersToSkipConcurrencyCheck.contains(entry.getKey())) {
        readyBrokers.put(entry.getKey(), Integer.MAX_VALUE);
      } else {
        readyBrokers.put(entry.getKey(), Math.max(0, movementConcurrency - entry.getValue()));
      }
    }
    return readyBrokers;
  }

  /**
   * Returns a list of proposals that move the leadership.
   */
  public synchronized List<ExecutionTask> getLeadershipMovementTasks() {
    return _executionTaskPlanner.getLeadershipMovementTasks(leadershipMovementConcurrency());
  }

  /**
   * Add a collection of execution proposals for execution. The method allows users to skip the concurrency check
   * on some given brokers. Notice that this method will replace the existing brokers that were in the concurrency
   * check privilege state with the new broker set.
   *
   * @param proposals the execution proposals to execute.
   * @param brokersToSkipConcurrencyCheck the brokers that does not need to be throttled when move the partitions.
   * @param cluster Cluster state.
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks.
   */
  public synchronized void addExecutionProposals(Collection<ExecutionProposal> proposals,
                                                 Collection<Integer> brokersToSkipConcurrencyCheck,
                                                 Cluster cluster,
                                                 ReplicaMovementStrategy replicaMovementStrategy) {
    _executionTaskPlanner.addExecutionProposals(proposals, cluster, replicaMovementStrategy);
    for (ExecutionProposal p : proposals) {
      _inProgressReplicaMovementsByBrokerId.putIfAbsent(p.oldLeader(), 0);
      for (int broker : p.replicasToAdd()) {
        _inProgressReplicaMovementsByBrokerId.putIfAbsent(broker, 0);
      }
    }
    // Set the execution mode for tasks.
    _executionTaskTracker.setExecutionMode(_isKafkaAssignerMode);

    // Populate the generated tasks to tracker to trace their execution.
    _executionTaskTracker.addTasksToTrace(_executionTaskPlanner.remainingReplicaMovements(), TaskType.REPLICA_ACTION);
    _executionTaskTracker.addTasksToTrace(_executionTaskPlanner.remainingLeadershipMovements(), TaskType.LEADER_ACTION);
    _brokersToSkipConcurrencyCheck.clear();
    if (brokersToSkipConcurrencyCheck != null) {
      _brokersToSkipConcurrencyCheck.addAll(brokersToSkipConcurrencyCheck);
    }
  }

  /**
   * Set the execution mode of the tasks to keep track of the ongoing execution mode via sensors.
   *
   * @param isKafkaAssignerMode True if kafka assigner mode, false otherwise.
   */
  public synchronized void setExecutionModeForTaskTracker(boolean isKafkaAssignerMode) {
    _isKafkaAssignerMode = isKafkaAssignerMode;
  }

  /**
   * Mark the given tasks as in progress. Tasks are executed homogeneously -- all tasks have the same balancing action.
   */
  public synchronized void markTasksInProgress(List<ExecutionTask> tasks) {
    if (!tasks.isEmpty()) {
      for (ExecutionTask task : tasks) {
        _executionTaskTracker.markTaskState(task, State.IN_PROGRESS);
        if (task.type() == TaskType.REPLICA_ACTION) {
          _inProgressPartitions.add(task.proposal().topicPartition());
          int oldLeader = task.proposal().oldLeader();
          _inProgressReplicaMovementsByBrokerId.put(oldLeader, _inProgressReplicaMovementsByBrokerId.get(oldLeader) + 1);
          for (int broker : task.proposal().replicasToAdd()) {
            _inProgressReplicaMovementsByBrokerId.put(broker, _inProgressReplicaMovementsByBrokerId.get(broker) + 1);
          }
        }
      }
    }
  }

  /**
   * Mark the successful completion of a given task. In-progress execution will yield successful completion.
   * Aborting execution will yield Aborted completion.
   */
  public synchronized void markTaskDone(ExecutionTask task) {
    if (task.state() == State.IN_PROGRESS) {
      _executionTaskTracker.markTaskState(task, State.COMPLETED);
      completeTask(task);
    } else if (task.state() == State.ABORTING) {
      _executionTaskTracker.markTaskState(task, State.ABORTED);
      completeTask(task);
    }
  }

  /**
   * Mark an in-progress task as aborting (1) if an error is encountered and (2) the rollback is possible.
   */
  public synchronized void markTaskAborting(ExecutionTask task) {
    if (task.state() == State.IN_PROGRESS) {
      _executionTaskTracker.markTaskState(task, State.ABORTING);
    }
  }

  /**
   * Mark an in-progress task as aborting (1) if an error is encountered and (2) the rollback is not possible.
   */
  public synchronized void markTaskDead(ExecutionTask task) {
    if (task.state() != State.DEAD) {
      _executionTaskTracker.markTaskState(task, State.DEAD);
      completeTask(task);
    }
  }

  /**
   * Mark a given tasks as completed.
   */
  private void completeTask(ExecutionTask task) {
    if (task.type() ==  TaskType.REPLICA_ACTION) {
        _inProgressPartitions.remove(task.proposal().topicPartition());
        int oldLeader = task.proposal().oldLeader();
        _inProgressReplicaMovementsByBrokerId.put(oldLeader, _inProgressReplicaMovementsByBrokerId.get(oldLeader) - 1);
        task.proposal()
            .replicasToAdd()
            .forEach(r -> _inProgressReplicaMovementsByBrokerId.put(r, _inProgressReplicaMovementsByBrokerId.get(r) - 1));
    }
  }

  public synchronized int numRemainingPartitionMovements() {
    return _executionTaskTracker.numRemainingPartitionMovements();
  }

  public synchronized long remainingDataToMoveInMB() {
    return _executionTaskTracker.remainingDataToMoveInMB();
  }

  public synchronized int numFinishedPartitionMovements() {
    return _executionTaskTracker.numFinishedPartitionMovements();
  }

  public synchronized long finishedDataMovementInMB() {
    return _executionTaskTracker.finishedDataMovementInMB();
  }

  public synchronized Set<ExecutionTask> inExecutionTasks() {
    return inExecutionTasks(TaskType.cachedValues());
  }

  public synchronized Set<ExecutionTask> inExecutionTasks(Collection<TaskType> types) {
    return _executionTaskTracker.inExecutionTasks(types);
  }

  public synchronized long inExecutionDataToMoveInMB() {
    return _executionTaskTracker.inExecutionDataMovementInMB();
  }

  public synchronized int numRemainingLeadershipMovements() {
    return _executionTaskTracker.numRemainingLeadershipMovements();
  }

  public synchronized int numFinishedLeadershipMovements() {
    return _executionTaskTracker.numFinishedLeadershipMovements();
  }

  public synchronized void clear() {
    _brokersToSkipConcurrencyCheck.clear();
    _inProgressReplicaMovementsByBrokerId.clear();
    _inProgressPartitions.clear();
    _executionTaskPlanner.clear();
    _executionTaskTracker.clear();
  }

  public synchronized ExecutionTasksSummary getExecutionTasksSummary(Set<TaskType> taskTypesToGetFullList) {
    return _executionTaskTracker.getExecutionTasksSummary(taskTypesToGetFullList);
  }
}