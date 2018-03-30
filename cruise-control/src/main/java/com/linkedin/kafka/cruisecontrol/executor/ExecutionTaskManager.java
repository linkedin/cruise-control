/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;


/**
 * The class that helps track the execution status for the balancing.
 * It does the following things:
 * <li>1. Keep track of the in progress partition movements between each pair of source-destination broker.
 * <li>2. When one partition movement finishes, it check the involved source and destination broker to see
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
  private final int _partitionMovementConcurrency;
  private final int _leaderMovementConcurrency;
  private final Set<Integer> _brokersToSkipConcurrencyCheck;
  private volatile long _inExecutionDataToMove;

  private static final String REPLICA_ACTION = "replica-action";
  private static final String LEADERSHIP_ACTION = "leadership-action";
  private static final String IN_PROGRESS = "in-progress";
  private static final String PENDING = "pending";
  private static final String ABORTING = "aborting";
  private static final String ABORTED = "aborted";
  private static final String DEAD = "dead";

  private static final String GAUGE_REPLICA_ACTION_IN_PROGRESS = REPLICA_ACTION + "-" + IN_PROGRESS;
  private static final String GAUGE_LEADERSHIP_ACTION_IN_PROGRESS = LEADERSHIP_ACTION + "-" + IN_PROGRESS;
  private static final String GAUGE_REPLICA_ACTION_PENDING = REPLICA_ACTION + "-" + PENDING;
  private static final String GAUGE_LEADERSHIP_ACTION_PENDING = LEADERSHIP_ACTION + "-" + PENDING;
  private static final String GAUGE_REPLICA_ACTION_ABORTING = REPLICA_ACTION + "-" + ABORTING;
  private static final String GAUGE_LEADERSHIP_ACTION_ABORTING = LEADERSHIP_ACTION + "-" + ABORTING;
  private static final String GAUGE_REPLICA_ACTION_ABORTED = REPLICA_ACTION + "-" + ABORTED;
  private static final String GAUGE_LEADERSHIP_ACTION_ABORTED = LEADERSHIP_ACTION + "-" + ABORTED;
  private static final String GAUGE_REPLICA_ACTION_DEAD = REPLICA_ACTION + "-" + DEAD;
  private static final String GAUGE_LEADERSHIP_ACTION_DEAD = LEADERSHIP_ACTION + "-" + DEAD;

  /**
   * The constructor of The Execution task manager.
   *
   * @param partitionMovementConcurrency The maximum number of concurrent partition movements per broker.
   */
  public ExecutionTaskManager(int partitionMovementConcurrency,
                              int leaderMovementConcurrency,
                              MetricRegistry dropwizardMetricRegistry) {
    _inProgressReplicaMovementsByBrokerId = new HashMap<>();
    _inProgressPartitions = new HashSet<>();
    _executionTaskTracker = new ExecutionTaskTracker();
    _executionTaskPlanner = new ExecutionTaskPlanner();
    _partitionMovementConcurrency = partitionMovementConcurrency;
    _leaderMovementConcurrency = leaderMovementConcurrency;
    _brokersToSkipConcurrencyCheck = new HashSet<>();
    _inExecutionDataToMove = 0L;

    // Register gauge sensors.
    registerGaugeSensors(dropwizardMetricRegistry);
  }

  /**
   * Register gauge sensors.
   */
  private void registerGaugeSensors(MetricRegistry dropwizardMetricRegistry) {
    String metricName = "Executor";
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_ACTION_IN_PROGRESS),
                                      (Gauge<Integer>) _executionTaskTracker::numInProgressReplicaAction);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_LEADERSHIP_ACTION_IN_PROGRESS),
                                      (Gauge<Integer>) _executionTaskTracker::numInProgressLeadershipAction);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_ACTION_PENDING),
                                      (Gauge<Integer>) _executionTaskTracker::numPendingReplicaAction);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_LEADERSHIP_ACTION_PENDING),
                                      (Gauge<Integer>) _executionTaskTracker::numPendingLeadershipAction);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_ACTION_ABORTING),
                                      (Gauge<Integer>) _executionTaskTracker::numAbortingReplicaAction);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_LEADERSHIP_ACTION_ABORTING),
                                      (Gauge<Integer>) _executionTaskTracker::numAbortingLeadershipAction);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_ACTION_ABORTED),
                                      (Gauge<Integer>) _executionTaskTracker::numAbortedReplicaAction);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_LEADERSHIP_ACTION_ABORTED),
                                      (Gauge<Integer>) _executionTaskTracker::numAbortedLeadershipAction);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_ACTION_DEAD),
                                      (Gauge<Integer>) _executionTaskTracker::numDeadReplicaAction);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_LEADERSHIP_ACTION_DEAD),
                                      (Gauge<Integer>) _executionTaskTracker::numDeadLeadershipAction);
  }

  /**
   * Returns a list of execution proposal that moves the partitions.
   */
  public synchronized List<ExecutionTask> getReplicaMovementTasks() {
    Map<Integer, Integer> readyBrokers = new HashMap<>();
    for (Map.Entry<Integer, Integer> entry : _inProgressReplicaMovementsByBrokerId.entrySet()) {
      // We skip the concurrency level check if caller requested so.
      // This is useful when we detected a broker failure and want to move all its partitions to the
      // rest of the brokers.
      if (_brokersToSkipConcurrencyCheck.contains(entry.getKey())) {
        readyBrokers.put(entry.getKey(), Integer.MAX_VALUE);
      } else {
        readyBrokers.put(entry.getKey(), Math.max(0, _partitionMovementConcurrency - entry.getValue()));
      }
    }
    return _executionTaskPlanner.getReplicaMovementTasks(readyBrokers, _inProgressPartitions);
  }

  /**
   * Returns a list of proposals that moves the leadership.
   */
  public synchronized List<ExecutionTask> getLeaderMovementTasks() {
    return _executionTaskPlanner.getLeaderMovementTasks(_leaderMovementConcurrency);
  }

  /**
   * Returns the remaining partition movement tasks.
   */
  public synchronized Set<ExecutionTask> remainingPartitionMovements() {
    return _executionTaskPlanner.remainingReplicaMovements();
  }

  /**
   * Returns the remaining leader movement tasks;
   */
  public synchronized Collection<ExecutionTask> remainingLeaderMovements() {
    return _executionTaskPlanner.remainingLeaderMovements();
  }

  /**
   * Returns the remaining data to move in MB.
   */
  public synchronized long remainingDataToMoveInMB() {
    return _executionTaskPlanner.remainingDataToMoveInMB();
  }

  /**
   * Returns the in execution data to move in MB -- i.e. data to move for in progress or aborting tasks.
   */
  public synchronized long inExecutionDataToMoveInMB() {
    return _inExecutionDataToMove;
  }

  /**
   * Get all the tasks that are not completed yet.
   * The uncompleted tasks include tasks in IN_PROGRESS and ABORTING state.
   */
  public synchronized Set<ExecutionTask> inExecutionTasks() {
    Set<ExecutionTask> inExecution = new HashSet<>();
    inExecution.addAll(_executionTaskTracker.tasksInState(ExecutionTask.State.IN_PROGRESS));
    inExecution.addAll(_executionTaskTracker.tasksInState(ExecutionTask.State.ABORTING));
    return inExecution;
  }

  /**
   * Get all the in-progress execution tasks.
   */
  public synchronized Set<ExecutionTask> inProgressTasks() {
    return _executionTaskTracker.tasksInState(ExecutionTask.State.IN_PROGRESS);
  }

  /**
   * @return the aborting tasks.
   */
  public synchronized Set<ExecutionTask> abortingTasks() {
    return _executionTaskTracker.tasksInState(ExecutionTask.State.ABORTING);
  }

  /**
   * @return the aborted tasks.
   */
  public synchronized Set<ExecutionTask> abortedTasks() {
    return _executionTaskTracker.tasksInState(ExecutionTask.State.ABORTED);
  }

  /**
   * @return the dead tasks.
   */
  public synchronized Set<ExecutionTask> deadTasks() {
    return _executionTaskTracker.tasksInState(ExecutionTask.State.DEAD);
  }

  /**
   * Add a collection of execution proposals for execution. The method allows users to skip the concurrency check
   * on some given brokers. Notice that this method will replace the existing brokers that were in the concurrency
   * check privilege state with the new broker set.
   *
   * @param proposals the execution proposals to execute.
   * @param brokersToSkipConcurrencyCheck the brokers that does not need to be throttled when move the partitions.
   * @param cluster Cluster state.
   */
  public synchronized void addExecutionProposals(Collection<ExecutionProposal> proposals,
                                                 Collection<Integer> brokersToSkipConcurrencyCheck,
                                                 Cluster cluster) {
    _executionTaskPlanner.addExecutionProposals(proposals, cluster);
    for (ExecutionProposal p : proposals) {
      _inProgressReplicaMovementsByBrokerId.putIfAbsent(p.oldLeader(), 0);
      for (int broker : p.replicasToAdd()) {
        _inProgressReplicaMovementsByBrokerId.putIfAbsent(broker, 0);
      }
    }
    // Add pending proposals to indicate the phase before they become an executable task.
    _executionTaskTracker.taskForReplicaAction(ExecutionTask.State.PENDING)
                         .addAll(_executionTaskPlanner.remainingReplicaMovements());
    _executionTaskTracker.taskForLeaderAction(ExecutionTask.State.PENDING)
                         .addAll(_executionTaskPlanner.remainingLeaderMovements());
    _brokersToSkipConcurrencyCheck.clear();
    if (brokersToSkipConcurrencyCheck != null) {
      _brokersToSkipConcurrencyCheck.addAll(brokersToSkipConcurrencyCheck);
    }
  }

  /**
   * Mark the given tasks as in progress. Tasks are executed homogeneously -- all tasks have the same balancing action.
   */
  public synchronized void markTasksInProgress(List<ExecutionTask> tasks) {
    if (!tasks.isEmpty()) {
      for (ExecutionTask task : tasks) {
        // Add task to the relevant task in progress.
        markTaskState(task, ExecutionTask.State.IN_PROGRESS);
        _inProgressPartitions.add(task.proposal().topicPartition());
        if (task.type() == ExecutionTask.TaskType.REPLICA_ACTION) {
          int oldLeader = task.proposal().oldLeader();
          // Negative oldLeader means new partition creation.
          if (oldLeader >= 0) {
            _inProgressReplicaMovementsByBrokerId.put(oldLeader, _inProgressReplicaMovementsByBrokerId.get(oldLeader) + 1);
          }
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
    if (task.state() == ExecutionTask.State.IN_PROGRESS) {
      markTaskState(task, ExecutionTask.State.COMPLETED);
      completeTask(task);
    } else if (task.state() == ExecutionTask.State.ABORTING) {
      markTaskState(task, ExecutionTask.State.ABORTED);
      completeTask(task);
    }
  }

  /**
   * Mark an in-progress task as aborting (1) if an error is encountered and (2) the rollback is possible.
   */
  public synchronized void markTaskAborting(ExecutionTask task) {
    if (task.state() != ExecutionTask.State.ABORTING) {
      markTaskState(task, ExecutionTask.State.ABORTING);
    }
  }

  /**
   * Mark an in-progress task as aborting (1) if an error is encountered and (2) the rollback is not possible.
   */
  public synchronized void markTaskDead(ExecutionTask task) {
    if (task.state() != ExecutionTask.State.DEAD) {
      markTaskState(task, ExecutionTask.State.DEAD);
      completeTask(task);
    }
  }

  private void markTaskState(ExecutionTask task, ExecutionTask.State targetState) {
    if (task.canTransferToState(targetState)) {
      ExecutionTask.State currentState = task.state();
      if (task.type() == ExecutionTask.TaskType.REPLICA_ACTION) {
        _executionTaskTracker.taskForReplicaAction(currentState).remove(task);
        _executionTaskTracker.taskForReplicaAction(targetState).add(task);
      } else {
        _executionTaskTracker.taskForLeaderAction(currentState).remove(task);
        _executionTaskTracker.taskForLeaderAction(targetState).add(task);
      }

      if (currentState == ExecutionTask.State.IN_PROGRESS || currentState == ExecutionTask.State.ABORTING) {
        _inExecutionDataToMove -= task.proposal().dataToMoveInMB();
      }

      switch (targetState) {
        case IN_PROGRESS:
          task.inProgress();
          _inExecutionDataToMove += task.proposal().dataToMoveInMB();
          break;
        case ABORTING:
          task.abort();
          _inExecutionDataToMove += task.proposal().dataToMoveInMB();
          break;
        case DEAD:
          task.kill();
          break;
        case ABORTED:
          task.aborted();
          break;
        case COMPLETED:
          task.completed();
          break;
        default:
          throw new IllegalStateException("Cannot mark a task in " + task.state() + " to " + targetState + " state");
      }
    } else {
      throw new IllegalStateException("Cannot mark a task in " + task.state() + " to " + targetState + " state. The "
                                          + "valid target states are " + task.validTargetState());
    }
  }

  /**
   * Mark a given tasks as completed.
   */
  private void completeTask(ExecutionTask task) {
    if (task.type() == ExecutionTask.TaskType.REPLICA_ACTION) {
      int oldLeader = task.proposal().oldLeader();
      // When old leader is negative the task is a partition creation. (not supported yet)
      if (oldLeader >= 0) {
        _inProgressReplicaMovementsByBrokerId.put(oldLeader, _inProgressReplicaMovementsByBrokerId.get(oldLeader) - 1);
      }
      for (int broker : task.proposal().replicasToAdd()) {
        _inProgressReplicaMovementsByBrokerId.put(broker, _inProgressReplicaMovementsByBrokerId.get(broker) - 1);
      }
    }
  }

  public synchronized void clear() {
    _brokersToSkipConcurrencyCheck.clear();
    _inProgressReplicaMovementsByBrokerId.clear();
    _inProgressPartitions.clear();
    _executionTaskPlanner.clear();
    _executionTaskTracker.clear();
  }

  public synchronized ExecutionTasksSummary getExecutionTasksSummary() {
    return new ExecutionTasksSummary(_executionTaskPlanner.remainingReplicaMovements(),
                              _executionTaskTracker.tasksInState(ExecutionTask.State.IN_PROGRESS),
                              _executionTaskTracker.tasksInState(ExecutionTask.State.ABORTING),
                              _executionTaskTracker.tasksInState(ExecutionTask.State.ABORTED),
                              _executionTaskTracker.tasksInState(ExecutionTask.State.DEAD),
                              _executionTaskPlanner.remainingDataToMoveInMB());
  }

  static class ExecutionTasksSummary {
    private final Set<ExecutionTask> _remainingPartitionMovements;
    private final Set<ExecutionTask> _inProgressTasks;
    private final Set<ExecutionTask> _abortingTasks;
    private final Set<ExecutionTask> _abortedTasks;
    private final Set<ExecutionTask> _deadTasks;
    private final long _remainingDataToMoveInMB;

    ExecutionTasksSummary(Set<ExecutionTask> remainingPartitionMovements,
                          Set<ExecutionTask> inProgressTasks,
                          Set<ExecutionTask> abortingTasks,
                          Set<ExecutionTask> abortedTasks,
                          Set<ExecutionTask> deadTasks,
                          long remainingDataToMoveInMB) {
      _remainingPartitionMovements = remainingPartitionMovements;
      _inProgressTasks = inProgressTasks;
      _abortingTasks = abortingTasks;
      _abortedTasks = abortedTasks;
      _deadTasks = deadTasks;
      _remainingDataToMoveInMB = remainingDataToMoveInMB;
    }

    /**
     * Returns the remaining partition movement tasks.
     */
    public Set<ExecutionTask> remainingPartitionMovements() {
      return _remainingPartitionMovements;
    }

    /**
     * Get all the in-progress execution tasks.
     */
    public Set<ExecutionTask> inProgressTasks() {
      return _inProgressTasks;
    }

    /**
     * @return the aborting tasks.
     */
    public Set<ExecutionTask> abortingTasks() {
      return _abortingTasks;
    }

    /**
     * @return the aborted tasks.
     */
    public Set<ExecutionTask> abortedTasks() {
      return _abortedTasks;
    }

    /**
     * @return the dead tasks.
     */
    public Set<ExecutionTask> deadTasks() {
      return _deadTasks;
    }

    /**
     * Returns the remaining data to move in MB.
     */
    public long remainingDataToMoveInMB() {
      return _remainingDataToMoveInMB;
    }
  }
}
