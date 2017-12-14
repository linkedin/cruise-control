/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
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
  private final Set<ExecutionTask> _inProgressTasks;
  private final Set<TopicPartition> _inProgressPartitions;
  private final Set<ExecutionTask> _abortedTasks;
  private final Set<ExecutionTask> _deadTasks;
  private final ExecutionTaskPlanner _executionTaskPlanner;
  private final int _partitionMovementConcurrency;
  private final int _leaderMovementConcurrency;
  private final Set<Integer> _brokersToSkipConcurrencyCheck;

  private static final String COUNTER_REPLICA_MOVE_IN_PROGRESS = "replica-move-in-progress";
  private static final String COUNTER_LEADERSHIP_MOVE_IN_PROGRESS = "leadership-move-in-progress";
  private static final String COUNTER_REPLICA_MOVE_PENDING = "replica-move-pending";
  private static final String COUNTER_LEADERSHIP_MOVE_PENDING = "leadership-move-pending";
  private static final String METER_REPLICA_MOVE_ABORTED = "replica-move-aborted";
  private static final String METER_LEADERSHIP_MOVE_ABORTED = "leadership-move-aborted";
  private static final String METER_REPLICA_MOVE_DEAD = "replica-move-dead";
  private static final String METER_LEADERSHIP_MOVE_DEAD = "leadership-move-dead";
  private static final String COUNTER_REPLICA_ADDITION_IN_PROGRESS = "replica-addition-in-progress";
  private static final String COUNTER_REPLICA_DELETION_IN_PROGRESS = "replica-deletion-in-progress";
  private static final String COUNTER_REPLICA_ADDITION_PENDING = "replica-addition-pending";
  private static final String COUNTER_REPLICA_DELETION_PENDING = "replica-deletion-pending";
  private static final String METER_REPLICA_ADDITION_ABORTED = "replica-addition-aborted";
  private static final String METER_REPLICA_DELETION_ABORTED = "replica-deletion-aborted";
  private static final String METER_REPLICA_ADDITION_DEAD = "replica-addition-dead";
  private static final String METER_REPLICA_DELETION_DEAD = "replica-deletion-dead";

  private static final String[] EXECUTION_COUNTER_NAMES = new String []
      {COUNTER_REPLICA_MOVE_IN_PROGRESS, COUNTER_LEADERSHIP_MOVE_IN_PROGRESS,
          COUNTER_REPLICA_MOVE_PENDING, COUNTER_LEADERSHIP_MOVE_PENDING,
          COUNTER_REPLICA_ADDITION_IN_PROGRESS, COUNTER_REPLICA_DELETION_IN_PROGRESS,
          COUNTER_REPLICA_ADDITION_PENDING, COUNTER_REPLICA_DELETION_PENDING};

  private static final String[] EXECUTION_METER_NAMES = new String []
      {METER_REPLICA_MOVE_ABORTED, METER_LEADERSHIP_MOVE_ABORTED,
          METER_REPLICA_MOVE_DEAD, METER_LEADERSHIP_MOVE_DEAD,
          METER_REPLICA_ADDITION_ABORTED, METER_REPLICA_DELETION_ABORTED,
          METER_REPLICA_ADDITION_DEAD, METER_REPLICA_DELETION_DEAD};

  private final Map<String, Counter> _executionCounterByName;
  private final Map<String, Meter> _executionRateByName;
  /**
   * The constructor of The Execution task manager.
   *
   * @param partitionMovementConcurrency The maximum number of concurrent partition movements per broker.
   */
  public ExecutionTaskManager(int partitionMovementConcurrency, int leaderMovementConcurrency,
                              MetricRegistry dropwizardMetricRegistry) {
    _inProgressPartMovementsByBrokerId = new HashMap<>();
    _inProgressPartitions = new HashSet<>();
    _inProgressTasks = new HashSet<>();
    _abortedTasks = new HashSet<>();
    _deadTasks = new HashSet<>();
    _executionTaskPlanner = new ExecutionTaskPlanner();
    _partitionMovementConcurrency = partitionMovementConcurrency;
    _leaderMovementConcurrency = leaderMovementConcurrency;
    _brokersToSkipConcurrencyCheck = new HashSet<>();

    // Initialize execution counter and rate sensors.
    _executionCounterByName = new HashMap<>();
    for (String name : EXECUTION_COUNTER_NAMES) {
      _executionCounterByName.put(name, dropwizardMetricRegistry.counter(MetricRegistry.name("Executor", name)));
    }
    _executionRateByName = new HashMap<>();
    for (String name : EXECUTION_METER_NAMES) {
      _executionRateByName.put(name, dropwizardMetricRegistry.meter(MetricRegistry.name("Executor", name)));
    }
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
    return _inProgressTasks.size() > 0;
  }

  /**
   * Get all the in progress execution tasks.
   */
  public Set<ExecutionTask> tasksInProgress() {
    return _inProgressTasks;
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

      // Increase pending task sensors.
      BalancingAction balancingAction = p.balancingAction();
      if (balancingAction == BalancingAction.REPLICA_MOVEMENT) {
        _executionCounterByName.get(COUNTER_REPLICA_MOVE_PENDING).inc();
      } else if (balancingAction == BalancingAction.LEADERSHIP_MOVEMENT) {
        _executionCounterByName.get(COUNTER_LEADERSHIP_MOVE_PENDING).inc();
      } else if (balancingAction == BalancingAction.REPLICA_ADDITION) {
        _executionCounterByName.get(COUNTER_REPLICA_ADDITION_PENDING).inc();
      } else if (balancingAction == BalancingAction.REPLICA_DELETION) {
        _executionCounterByName.get(COUNTER_REPLICA_DELETION_PENDING).inc();
      } else {
        throw new IllegalStateException(String.format("Unrecognized balancing action: %s.", balancingAction));
      }
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
      // Get the common balancing action for the given tasks.
      BalancingAction taskBalancingAction = tasks.iterator().next().proposal.balancingAction();
      // Sanity check: All tasks must have the same balancing action.
      for (ExecutionTask task : tasks) {
        if (task.proposal.balancingAction() != taskBalancingAction) {
          throw new IllegalStateException("Attempt to execute tasks with heterogeneous balancing actions.");
        }
      }

      for (ExecutionTask task : tasks) {
        _inProgressTasks.add(task);
        _inProgressPartitions.add(task.proposal.topicPartition());
        if (taskBalancingAction == BalancingAction.REPLICA_MOVEMENT) {
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

      // Mark in progress and pending tasks.
      if (taskBalancingAction == BalancingAction.REPLICA_MOVEMENT) {
        _executionCounterByName.get(COUNTER_REPLICA_MOVE_IN_PROGRESS).inc(tasks.size());
        _executionCounterByName.get(COUNTER_REPLICA_MOVE_PENDING).dec(tasks.size());
      } else if (taskBalancingAction == BalancingAction.LEADERSHIP_MOVEMENT) {
        _executionCounterByName.get(COUNTER_LEADERSHIP_MOVE_IN_PROGRESS).inc(tasks.size());
        _executionCounterByName.get(COUNTER_LEADERSHIP_MOVE_PENDING).dec(tasks.size());
      } else if (taskBalancingAction == BalancingAction.REPLICA_ADDITION) {
        _executionCounterByName.get(COUNTER_REPLICA_ADDITION_IN_PROGRESS).inc(tasks.size());
        _executionCounterByName.get(COUNTER_REPLICA_ADDITION_PENDING).dec(tasks.size());
      } else if (taskBalancingAction == BalancingAction.REPLICA_DELETION) {
        _executionCounterByName.get(COUNTER_REPLICA_DELETION_IN_PROGRESS).inc(tasks.size());
        _executionCounterByName.get(COUNTER_REPLICA_DELETION_PENDING).dec(tasks.size());
      } else {
        throw new IllegalStateException(String.format("Unrecognized balancing action: %s.", taskBalancingAction));
      }
    }
  }

  /**
   * Update relevant sensors upon topic deletion of an ongoing task based on the given task balancing action.
   *
   * @param taskBalancingAction Task balancing action for the ongoing task.
   */
  void updateSensorsUponTaskTopicDeletion(BalancingAction taskBalancingAction) {
    switch (taskBalancingAction) {
      case REPLICA_MOVEMENT:
        _executionCounterByName.get(COUNTER_REPLICA_MOVE_IN_PROGRESS).dec();
        _executionRateByName.get(METER_REPLICA_MOVE_ABORTED).mark();
        break;
      case REPLICA_DELETION:
        _executionCounterByName.get(COUNTER_REPLICA_DELETION_IN_PROGRESS).dec();
        _executionRateByName.get(METER_REPLICA_DELETION_ABORTED).mark();
        break;
      case REPLICA_ADDITION:
        _executionCounterByName.get(COUNTER_REPLICA_ADDITION_IN_PROGRESS).dec();
        _executionRateByName.get(METER_REPLICA_ADDITION_ABORTED).mark();
        break;
      case LEADERSHIP_MOVEMENT:
        _executionCounterByName.get(COUNTER_LEADERSHIP_MOVE_IN_PROGRESS).dec();
        _executionRateByName.get(METER_LEADERSHIP_MOVE_ABORTED).mark();
        break;
      default:
        throw new IllegalStateException(String.format("Unrecognized balancing action: %s.", taskBalancingAction));
    }
  }

  /**
   * Mark the successful completion of a given task. Only normal execution may yield successful completion.
   */
  public void markTaskDone(ExecutionTask task) {
    if (task.healthiness() == ExecutionTask.Healthiness.NORMAL) {
      switch (task.proposal.balancingAction()) {
        case REPLICA_MOVEMENT:
          _executionCounterByName.get(COUNTER_REPLICA_MOVE_IN_PROGRESS).dec();
          break;
        case REPLICA_DELETION:
          _executionCounterByName.get(COUNTER_REPLICA_DELETION_IN_PROGRESS).dec();
          break;
        case REPLICA_ADDITION:
          _executionCounterByName.get(COUNTER_REPLICA_ADDITION_IN_PROGRESS).dec();
          break;
        case LEADERSHIP_MOVEMENT:
          _executionCounterByName.get(COUNTER_LEADERSHIP_MOVE_IN_PROGRESS).dec();
          break;
        default:
          throw new IllegalStateException(String.format("Unrecognized balancing action: %s.", task.proposal.balancingAction()));
      }
    }
  }

  /**
   * Mark the given task as aborted.
   */
  public void markTaskAborted(ExecutionTask task) {
    _abortedTasks.add(task);
    switch (task.proposal.balancingAction()) {
      case REPLICA_MOVEMENT:
        _executionRateByName.get(METER_REPLICA_MOVE_ABORTED).mark();
        break;
      case REPLICA_DELETION:
        _executionRateByName.get(METER_REPLICA_DELETION_ABORTED).mark();
        break;
      case REPLICA_ADDITION:
        _executionRateByName.get(METER_REPLICA_ADDITION_ABORTED).mark();
        break;
      case LEADERSHIP_MOVEMENT:
        _executionRateByName.get(METER_LEADERSHIP_MOVE_ABORTED).mark();
        break;
      default:
        throw new IllegalStateException(String.format("Unrecognized balancing action: %s.", task.proposal.balancingAction()));
    }
  }

  /**
   * Mark the given task as dead.
   */
  public void markTaskDead(ExecutionTask task) {
    _deadTasks.add(task);
    switch (task.proposal.balancingAction()) {
      case REPLICA_MOVEMENT:
        _executionRateByName.get(METER_REPLICA_MOVE_DEAD).mark();
        break;
      case REPLICA_DELETION:
        _executionRateByName.get(METER_REPLICA_DELETION_DEAD).mark();
        break;
      case REPLICA_ADDITION:
        _executionRateByName.get(METER_REPLICA_ADDITION_DEAD).mark();
        break;
      case LEADERSHIP_MOVEMENT:
        _executionRateByName.get(METER_LEADERSHIP_MOVE_DEAD).mark();
        break;
      default:
        throw new IllegalStateException(String.format("Unrecognized balancing action: %s.", task.proposal.balancingAction()));
    }
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
   * Mark a given tasks as completed.
   */
  public void completeTasks(List<ExecutionTask> tasks) {
    for (ExecutionTask task : tasks) {
      _inProgressTasks.remove(task);
      _inProgressPartitions.remove(task.proposal.topicPartition());
      if (task.proposal.balancingAction() == BalancingAction.REPLICA_MOVEMENT) {
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
    _executionTaskPlanner.clear();
    _inProgressTasks.clear();
    _abortedTasks.clear();
    _deadTasks.clear();
  }

}
