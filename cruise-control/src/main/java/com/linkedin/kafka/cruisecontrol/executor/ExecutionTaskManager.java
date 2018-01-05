/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;

import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 */
public class ExecutionTaskManager {
  private final Map<Integer, Integer> _inProgressPartMovementsByBrokerId;
  private final ExecutionTaskTracker _executionTaskTracker;
  private final Set<TopicPartition> _inProgressPartitions;
  private final ExecutionTaskPlanner _executionTaskPlanner;
  private final int _partitionMovementConcurrency;
  private final int _leaderMovementConcurrency;
  private final Set<Integer> _brokersToSkipConcurrencyCheck;

  private static final String REPLICA_MOVE = "replica-move";
  private static final String LEADERSHIP_MOVE = "leadership-move";
  private static final String REPLICA_ADDITION = "replica-addition";
  private static final String REPLICA_DELETION = "replica-deletion";
  private static final String IN_PROGRESS = "in-progress";
  private static final String PENDING = "pending";
  private static final String ABORTING = "aborting";
  private static final String ABORTED = "aborted";
  private static final String DEAD = "dead";

  private static final String GAUGE_REPLICA_MOVE_IN_PROGRESS = REPLICA_MOVE + "-" + IN_PROGRESS;
  private static final String GAUGE_LEADERSHIP_MOVE_IN_PROGRESS = LEADERSHIP_MOVE + "-" + IN_PROGRESS;
  private static final String GAUGE_REPLICA_MOVE_PENDING = REPLICA_MOVE + "-" + PENDING;
  private static final String GAUGE_LEADERSHIP_MOVE_PENDING = LEADERSHIP_MOVE + "-" + PENDING;
  private static final String GAUGE_REPLICA_MOVE_ABORTING = REPLICA_MOVE + "-" + ABORTING;
  private static final String GAUGE_LEADERSHIP_MOVE_ABORTING = LEADERSHIP_MOVE + "-" + ABORTING;
  private static final String GAUGE_REPLICA_MOVE_ABORTED = REPLICA_MOVE + "-" + ABORTED;
  private static final String GAUGE_LEADERSHIP_MOVE_ABORTED = LEADERSHIP_MOVE + "-" + ABORTED;
  private static final String GAUGE_REPLICA_MOVE_DEAD = REPLICA_MOVE + "-" + DEAD;
  private static final String GAUGE_LEADERSHIP_MOVE_DEAD = LEADERSHIP_MOVE + "-" + DEAD;
  private static final String GAUGE_REPLICA_ADDITION_IN_PROGRESS = REPLICA_ADDITION + "-" + IN_PROGRESS;
  private static final String GAUGE_REPLICA_DELETION_IN_PROGRESS = REPLICA_DELETION + "-" + IN_PROGRESS;
  private static final String GAUGE_REPLICA_ADDITION_PENDING = REPLICA_ADDITION + "-" + PENDING;
  private static final String GAUGE_REPLICA_DELETION_PENDING = REPLICA_DELETION + "-" + PENDING;
  private static final String GAUGE_REPLICA_ADDITION_ABORTING = REPLICA_ADDITION + "-" + ABORTING;
  private static final String GAUGE_REPLICA_DELETION_ABORTING = REPLICA_DELETION + "-" + ABORTING;
  private static final String GAUGE_REPLICA_ADDITION_ABORTED = REPLICA_ADDITION + "-" + ABORTED;
  private static final String GAUGE_REPLICA_DELETION_ABORTED = REPLICA_DELETION + "-" + ABORTED;
  private static final String GAUGE_REPLICA_ADDITION_DEAD = REPLICA_ADDITION + "-" + DEAD;
  private static final String GAUGE_REPLICA_DELETION_DEAD = REPLICA_DELETION + "-" + DEAD;

  /**
   * The constructor of The Execution task manager.
   *
   * @param partitionMovementConcurrency The maximum number of concurrent partition movements per broker.
   */
  public ExecutionTaskManager(int partitionMovementConcurrency,
                              int leaderMovementConcurrency,
                              MetricRegistry dropwizardMetricRegistry) {
    _inProgressPartMovementsByBrokerId = new HashMap<>();
    _inProgressPartitions = new HashSet<>();
    _executionTaskTracker = new ExecutionTaskTracker();
    _executionTaskPlanner = new ExecutionTaskPlanner();
    _partitionMovementConcurrency = partitionMovementConcurrency;
    _leaderMovementConcurrency = leaderMovementConcurrency;
    _brokersToSkipConcurrencyCheck = new HashSet<>();

    // Register gauge sensors.
    registerGaugeSensors(dropwizardMetricRegistry);
  }

  /**
   * Register gauge sensors.
   *
   * @param dropwizardMetricRegistry
   */
  private void registerGaugeSensors(MetricRegistry dropwizardMetricRegistry) {
    String metricName = "Executor";
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_MOVE_IN_PROGRESS),
                                      (Gauge<Integer>) _executionTaskTracker::numInProgressReplicaMove);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_LEADERSHIP_MOVE_IN_PROGRESS),
                                      (Gauge<Integer>) _executionTaskTracker::numInProgressLeadershipMove);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_MOVE_PENDING),
                                      (Gauge<Integer>) _executionTaskTracker::numPendingReplicaMove);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_LEADERSHIP_MOVE_PENDING),
                                      (Gauge<Integer>) _executionTaskTracker::numPendingLeadershipMove);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_ADDITION_IN_PROGRESS),
                                      (Gauge<Integer>) _executionTaskTracker::numInProgressReplicaAddition);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_DELETION_IN_PROGRESS),
                                      (Gauge<Integer>) _executionTaskTracker::numInProgressReplicaDeletion);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_ADDITION_PENDING),
                                      (Gauge<Integer>) _executionTaskTracker::numPendingReplicaAddition);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_DELETION_PENDING),
                                      (Gauge<Integer>) _executionTaskTracker::numPendingReplicaDeletion);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_MOVE_ABORTING),
                                      (Gauge<Integer>) _executionTaskTracker::numAbortingReplicaMove);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_LEADERSHIP_MOVE_ABORTING),
                                      (Gauge<Integer>) _executionTaskTracker::numAbortingLeadershipMove);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_ADDITION_ABORTING),
                                      (Gauge<Integer>) _executionTaskTracker::numAbortingReplicaAddition);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_DELETION_ABORTING),
                                      (Gauge<Integer>) _executionTaskTracker::numAbortingReplicaDeletion);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_MOVE_ABORTED),
                                      (Gauge<Integer>) _executionTaskTracker::numAbortedReplicaMove);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_LEADERSHIP_MOVE_ABORTED),
                                      (Gauge<Integer>) _executionTaskTracker::numAbortedLeadershipMove);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_ADDITION_ABORTED),
                                      (Gauge<Integer>) _executionTaskTracker::numAbortedReplicaAddition);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_DELETION_ABORTED),
                                      (Gauge<Integer>) _executionTaskTracker::numAbortedReplicaDeletion);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_MOVE_DEAD),
                                      (Gauge<Integer>) _executionTaskTracker::numDeadReplicaMove);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_LEADERSHIP_MOVE_DEAD),
                                      (Gauge<Integer>) _executionTaskTracker::numDeadLeadershipMove);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_ADDITION_DEAD),
                                      (Gauge<Integer>) _executionTaskTracker::numDeadReplicaAddition);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_REPLICA_DELETION_DEAD),
                                      (Gauge<Integer>) _executionTaskTracker::numDeadReplicaDeletion);
  }

  /**
   * Returns a list of balancing proposal that moves the partitions.
   */
  public List<ExecutionTask> getReplicaMovementTasks() {
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
    return _executionTaskPlanner.getReplicaMovementTasks(readyBrokers, _inProgressPartitions);
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
    return _executionTaskPlanner.remainingReplicaMovements();
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
    return _executionTaskTracker.hasTaskInProgress();
  }

  /**
   * Get all the in progress execution tasks.
   */
  public Set<ExecutionTask> inProgressTasks() {
    return _executionTaskTracker.inProgressTasks();
  }

  /**
   * @return the aborting tasks.
   */
  public Set<ExecutionTask> abortingTasks() {
    return _executionTaskTracker.abortingTasks();
  }

  /**
   * @return the aborted tasks.
   */
  public Set<ExecutionTask> abortedTasks() {
    return _executionTaskTracker.abortedTasks();
  }

  /**
   * @return the dead tasks.
   */
  public Set<ExecutionTask> deadTasks() {
    return _executionTaskTracker.deadTasks();
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

      // Add pending proposals to indicate the phase before they become an executable task.
      _executionTaskTracker.pendingProposalsFor(p.balancingAction()).add(p);
    }
    _brokersToSkipConcurrencyCheck.clear();
    if (brokersToSkipConcurrencyCheck != null) {
      _brokersToSkipConcurrencyCheck.addAll(brokersToSkipConcurrencyCheck);
    }
  }

  /**
   * Mark the given tasks as in progress. Tasks are executed homogeneously -- all tasks have the same balancing action.
   */
  public void markTasksInProgress(List<ExecutionTask> tasks) {
    if (!tasks.isEmpty()) {
      for (ExecutionTask task : tasks) {
        // Add task to the relevant task in progress.
        markTaskState(task, ExecutionTask.State.IN_PROGRESS);
        _inProgressPartitions.add(task.proposal.topicPartition());
        if (task.proposal.balancingAction() == BalancingAction.REPLICA_MOVEMENT) {
          if (task.sourceBrokerId() != null) {
            _inProgressPartMovementsByBrokerId.put(task.sourceBrokerId(),
                                                   _inProgressPartMovementsByBrokerId.get(task.sourceBrokerId()) + 1);
          }
          if (task.destinationBrokerId() != null) {
            _inProgressPartMovementsByBrokerId.put(task.destinationBrokerId(),
                                                   _inProgressPartMovementsByBrokerId.get(task.destinationBrokerId()) + 1);
          }
        }
      }
    }
  }

  /**
   * Mark the successful completion of a given task. In-progress execution will yield successful completion.
   * Aborting execution will yield Aborted completion.
   */
  public void markTaskDone(ExecutionTask task) {
    if (task.state() == ExecutionTask.State.IN_PROGRESS) {
      markTaskState(task, ExecutionTask.State.COMPLETED);
    } else if (task.state() == ExecutionTask.State.ABORTING) {
      markTaskState(task, ExecutionTask.State.ABORTED);
    }
  }

  /**
   * Mark an in-progress task as aborting (1) if an error is encountered and (2) the rollback is possible.
   */
  public void markTaskAborting(ExecutionTask task) {
    if (task.state() != ExecutionTask.State.ABORTING) {
      markTaskState(task, ExecutionTask.State.ABORTING);
    }
  }

  /**
   * Mark an in-progress task as aborting (1) if an error is encountered and (2) the rollback is not possible.
   */
  public void markTaskDead(ExecutionTask task) {
    if (task.state() != ExecutionTask.State.DEAD) {
      markTaskState(task, ExecutionTask.State.DEAD);
    }
  }

  private void markTaskState(ExecutionTask task, ExecutionTask.State targetState) {
    if (task.canTransferToState(targetState)) {
      ExecutionTask.State currentState = task.state();
      BalancingAction balancingAction = task.proposal.balancingAction();
      switch (currentState) {
        case PENDING:
          _executionTaskTracker.pendingProposalsFor(balancingAction).remove(task.proposal);
          break;
        case IN_PROGRESS:
          _executionTaskTracker.inProgressTasksFor(balancingAction).remove(task);
          break;
        case ABORTING:
          _executionTaskTracker.abortingTasksFor(balancingAction).remove(task);
          break;
        default:
          throw new IllegalStateException("Cannot mark a task in " + task.state() + " to " + targetState + " state");
      }

      switch (targetState) {
        case IN_PROGRESS:
          task.inProgress();
          _executionTaskTracker.inProgressTasksFor(balancingAction).add(task);
          break;
        case ABORTING:
          task.abort();
          _executionTaskTracker.abortingTasksFor(balancingAction).add(task);
          break;
        case DEAD:
          task.kill();
          _executionTaskTracker.deadTasksFor(balancingAction).add(task);
          break;
        case ABORTED:
          task.aborted();
          _executionTaskTracker.abortedTasksFor(balancingAction).add(task);
          break;
        case COMPLETED:
          task.completed();
          break;
        default:
          throw new IllegalStateException("Cannot mark a task in " + task.state() + " to " + targetState + " state");
      }
    } else {
      throw new IllegalStateException("Cannot mark a task in " + task.state() + " to " + targetState + " state. The "
                                          + "valid target state are " + task.validTargetState());
    }
  }

  /**
   * Mark a given tasks as completed.
   */
  public void completeTasks(List<ExecutionTask> tasks) {
    for (ExecutionTask task : tasks) {
      BalancingAction balancingAction = task.proposal.balancingAction();
      _executionTaskTracker.inProgressTasksFor(balancingAction).remove(task);
      _inProgressPartitions.remove(task.proposal.topicPartition());
      if (balancingAction == BalancingAction.REPLICA_MOVEMENT) {
        if (task.sourceBrokerId() != null) {
          _inProgressPartMovementsByBrokerId.put(task.sourceBrokerId(),
                                                 _inProgressPartMovementsByBrokerId.get(task.sourceBrokerId()) - 1);
        }
        if (task.destinationBrokerId() != null) {
          _inProgressPartMovementsByBrokerId.put(task.destinationBrokerId(),
                                                 _inProgressPartMovementsByBrokerId.get(task.destinationBrokerId()) - 1);
        }
      }
    }
  }

  public void clear() {
    _brokersToSkipConcurrencyCheck.clear();
    _inProgressPartMovementsByBrokerId.clear();
    _inProgressPartitions.clear();
    _executionTaskPlanner.clear();
    _executionTaskTracker.clear();
  }
}
