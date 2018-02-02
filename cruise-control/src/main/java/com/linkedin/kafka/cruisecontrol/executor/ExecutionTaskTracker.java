/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.common.ActionType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * A class for tracking the (1) dead tasks, (2) aborting/aborted tasks, (3) in progress tasks, and (4) pending proposals.
 */
public class ExecutionTaskTracker {
  // Dead tasks indicate cancelled/killed executions affecting the original state before the balancing action execution.
  private final Map<ActionType, Set<ExecutionTask>> _deadTasks;
  // Aborting tasks due to cancelled balancing actions not affecting the state before the execution of balancing
  // action.
  private final Map<ActionType, Set<ExecutionTask>> _abortingTasks;
  // Tasks that have been successfully aborted.
  private final Map<ActionType, Set<ExecutionTask>> _abortedTasks;
  // Tasks in progress indicate the ongoing balancing action.
  private final Map<ActionType, Set<ExecutionTask>> _inProgressTasks;
  // Pending proposals indicate the phase before submitted proposals become executable task.
  private final Map<ActionType, Set<BalancingAction>> _pendingProposals;

  ExecutionTaskTracker() {
    _deadTasks = new HashMap<>();
    for (ActionType actionType : ActionType.cachedValues()) {
      _deadTasks.put(actionType, new HashSet<>());
    }

    _abortingTasks = new HashMap<>();
    for (ActionType actionType : ActionType.cachedValues()) {
      _abortingTasks.put(actionType, new HashSet<>());
    }

    _abortedTasks = new HashMap<>();
    for (ActionType actionType : ActionType.cachedValues()) {
      _abortedTasks.put(actionType, new HashSet<>());
    }

    _inProgressTasks = new HashMap<>();
    for (ActionType actionType : ActionType.cachedValues()) {
      _inProgressTasks.put(actionType, new HashSet<>());
    }

    _pendingProposals = new HashMap<>();
    for (ActionType actionType : ActionType.cachedValues()) {
      _pendingProposals.put(actionType, new HashSet<>());
    }
  }

  /**
   * Get the set of dead tasks for the given balancing action.
   *
   * @param actionType The balancing action of the requested dead tasks.
   * @return The set of dead tasks for the given balancing action.
   */
  public Set<ExecutionTask> deadTasksFor(ActionType actionType) {
    return _deadTasks.get(actionType);
  }

  /**
   * Get the set of aborting tasks for the given balancing action.
   * @param actionType The balancing action of the requested aborting tasks.
   * @return The set of aborting tasks for the given balancing action.
   */
  public Set<ExecutionTask> abortingTasksFor(ActionType actionType) {
    return _abortingTasks.get(actionType);
  }

  /**
   * Get the set of aborted tasks for the given balancing action.
   *
   * @param actionType The balancing action of the requested aborted tasks.
   * @return The set of aborted tasks for the given balancing action.
   */
  public Set<ExecutionTask> abortedTasksFor(ActionType actionType) {
    return _abortedTasks.get(actionType);
  }

  /**
   * Get the set of in progress tasks for the given balancing action.
   *
   * @param actionType The balancing action of the requested in progress tasks.
   * @return The set of in progress tasks for the given balancing action.
   */
  public Set<ExecutionTask> inProgressTasksFor(ActionType actionType) {
    return _inProgressTasks.get(actionType);
  }

  /**
   * Get the set of pending proposals for the given balancing action.
   *
   * @param actionType The balancing action of the requested pending proposals.
   * @return The set of pending proposals for the given balancing action.
   */
  public Set<BalancingAction> pendingProposalsFor(ActionType actionType) {
    return _pendingProposals.get(actionType);
  }

  /**
   * Check if there is any task in progress.
   */
  public boolean hasTaskInProgress() {
    for (ActionType actionType : ActionType.cachedValues()) {
      if (!_inProgressTasks.get(actionType).isEmpty()
          || !_abortingTasks.get(actionType).isEmpty()) {
        return true;
      }
    }

    return false;
  }

  /**
   * Get all the in progress execution tasks.
   */
  public Set<ExecutionTask> inProgressTasks() {
    Set<ExecutionTask> tasksInProgress = new HashSet<>();
    for (ActionType actionType : ActionType.cachedValues()) {
      tasksInProgress.addAll(_inProgressTasks.get(actionType));
    }

    return tasksInProgress;
  }

  /**
   * Get all the aborting tasks.
   */
  public Set<ExecutionTask> abortingTasks() {
    Set<ExecutionTask> tasksAborting = new HashSet<>();
    for (ActionType actionType : ActionType.cachedValues()) {
      tasksAborting.addAll(_abortingTasks.get(actionType));
    }
    return tasksAborting;
  }

  /**
   * Get all the aborted execution tasks.
   */
  public Set<ExecutionTask> abortedTasks() {
    Set<ExecutionTask> tasksAborted = new HashSet<>();
    for (ActionType actionType : ActionType.cachedValues()) {
      tasksAborted.addAll(_abortedTasks.get(actionType));
    }

    return tasksAborted;
  }

  /**
   * Get all the dead execution tasks.
   */
  public Set<ExecutionTask> deadTasks() {
    Set<ExecutionTask> tasksDead = new HashSet<>();
    for (ActionType actionType : ActionType.cachedValues()) {
      tasksDead.addAll(_deadTasks.get(actionType));
    }

    return tasksDead;
  }

  public int numDeadReplicaMove() {
    return _deadTasks.get(ActionType.REPLICA_MOVEMENT).size();
  }

  public int numDeadLeadershipMove() {
    return _deadTasks.get(ActionType.LEADERSHIP_MOVEMENT).size();
  }

  public int numDeadReplicaAddition() {
    return _deadTasks.get(ActionType.REPLICA_ADDITION).size();
  }

  public int numDeadReplicaDeletion() {
    return _deadTasks.get(ActionType.REPLICA_DELETION).size();
  }

  public int numAbortingReplicaMove() {
    return _abortingTasks.get(ActionType.REPLICA_MOVEMENT).size();
  }

  public int numAbortingLeadershipMove() {
    return _abortingTasks.get(ActionType.LEADERSHIP_MOVEMENT).size();
  }

  public int numAbortingReplicaAddition() {
    return _abortingTasks.get(ActionType.REPLICA_ADDITION).size();
  }

  public int numAbortingReplicaDeletion() {
    return _abortingTasks.get(ActionType.REPLICA_DELETION).size();
  }

  public int numAbortedReplicaMove() {
    return _abortedTasks.get(ActionType.REPLICA_MOVEMENT).size();
  }

  public int numAbortedLeadershipMove() {
    return _abortedTasks.get(ActionType.LEADERSHIP_MOVEMENT).size();
  }

  public int numAbortedReplicaAddition() {
    return _abortedTasks.get(ActionType.REPLICA_ADDITION).size();
  }

  public int numAbortedReplicaDeletion() {
    return _abortedTasks.get(ActionType.REPLICA_DELETION).size();
  }

  public int numInProgressReplicaMove() {
    return _inProgressTasks.get(ActionType.REPLICA_MOVEMENT).size();
  }

  public int numInProgressLeadershipMove() {
    return _inProgressTasks.get(ActionType.LEADERSHIP_MOVEMENT).size();
  }

  public int numInProgressReplicaAddition() {
    return _inProgressTasks.get(ActionType.REPLICA_ADDITION).size();
  }

  public int numInProgressReplicaDeletion() {
    return _inProgressTasks.get(ActionType.REPLICA_DELETION).size();
  }

  public int numPendingReplicaMove() {
    return _pendingProposals.get(ActionType.REPLICA_MOVEMENT).size();
  }

  public int numPendingLeadershipMove() {
    return _pendingProposals.get(ActionType.LEADERSHIP_MOVEMENT).size();
  }

  public int numPendingReplicaAddition() {
    return _pendingProposals.get(ActionType.REPLICA_ADDITION).size();
  }

  public int numPendingReplicaDeletion() {
    return _pendingProposals.get(ActionType.REPLICA_DELETION).size();
  }

  /**
   * Clear (1) dead tasks, (2) aborted tasks, (3) in progress tasks, and (4) pending proposals.
   */
  public void clear() {
    // Clear dead tasks.
    for (ActionType actionType : ActionType.cachedValues()) {
      _deadTasks.get(actionType).clear();
    }
    // Clear aborting tasks.
    for (ActionType actionType : ActionType.cachedValues()) {
      _abortingTasks.get(actionType).clear();
    }
    // Clear aborted tasks.
    for (ActionType actionType : ActionType.cachedValues()) {
      _abortedTasks.get(actionType).clear();
    }
    // Clear in progress tasks.
    for (ActionType actionType : ActionType.cachedValues()) {
      _inProgressTasks.get(actionType).clear();
    }
    // Clear pending proposals.
    for (ActionType actionType : ActionType.cachedValues()) {
      _pendingProposals.get(actionType).clear();
    }
  }
}
