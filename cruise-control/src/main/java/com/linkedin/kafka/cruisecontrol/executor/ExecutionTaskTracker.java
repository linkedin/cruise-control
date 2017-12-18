/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * A class for tracking the (1) dead tasks, (2) aborted tasks, (3) in progress tasks, and (4) pending proposals.
 */
public class ExecutionTaskTracker {
  // Dead tasks indicate cancelled/killed executions affecting the original state before the balancing action execution.
  private final Map<BalancingAction, Set<ExecutionTask>> _deadTasks;
  // Aborted tasks indicate tasks stopped due to (1) cancelled balancing actions not affecting the state before the
  // execution of balancing action or (2) deletion of topic partitions for which the balancing action was in progress.
  private final Map<BalancingAction, Set<ExecutionTask>> _abortedTasks;
  // Tasks in progress indicate the ongoing balancing action.
  private final Map<BalancingAction, Set<ExecutionTask>> _inProgressTasks;
  // Pending proposals indicate the phase before submitted proposals become executable task.
  private final Map<BalancingAction, Set<BalancingProposal>> _pendingProposals;

  ExecutionTaskTracker() {
    _deadTasks = new HashMap<>();
    for (BalancingAction balancingAction : BalancingAction.values()) {
      _deadTasks.put(balancingAction, new HashSet<>());
    }

    _abortedTasks = new HashMap<>();
    for (BalancingAction balancingAction : BalancingAction.values()) {
      _abortedTasks.put(balancingAction, new HashSet<>());
    }

    _inProgressTasks = new HashMap<>();
    for (BalancingAction balancingAction : BalancingAction.values()) {
      _inProgressTasks.put(balancingAction, new HashSet<>());
    }

    _pendingProposals = new HashMap<>();
    for (BalancingAction balancingAction : BalancingAction.values()) {
      _pendingProposals.put(balancingAction, new HashSet<>());
    }
  }

  /**
   * Get the set of dead tasks for the given balancing action.
   *
   * @param balancingAction The balancing action of the requested dead tasks.
   * @return The set of dead tasks for the given balancing action.
   */
  public Set<ExecutionTask> deadTasksFor(BalancingAction balancingAction) {
    Set<ExecutionTask> deadTasks = _deadTasks.get(balancingAction);
    if (deadTasks == null) {
      throw new IllegalStateException(String.format("Unrecognized balancing action: %s.", balancingAction));
    }
    return deadTasks;
  }

  /**
   * Get the set of aborted tasks for the given balancing action.
   *
   * @param balancingAction The balancing action of the requested aborted tasks.
   * @return The set of aborted tasks for the given balancing action.
   */
  public Set<ExecutionTask> abortedTasksFor(BalancingAction balancingAction) {
    Set<ExecutionTask> abortedTasks = _abortedTasks.get(balancingAction);
    if (abortedTasks == null) {
      throw new IllegalStateException(String.format("Unrecognized balancing action: %s.", balancingAction));
    }
    return abortedTasks;
  }

  /**
   * Get the set of in progress tasks for the given balancing action.
   *
   * @param balancingAction The balancing action of the requested in progress tasks.
   * @return The set of in progress tasks for the given balancing action.
   */
  public Set<ExecutionTask> inProgressTasksFor(BalancingAction balancingAction) {
    Set<ExecutionTask> inProgressTasks = _inProgressTasks.get(balancingAction);
    if (inProgressTasks == null) {
      throw new IllegalStateException(String.format("Unrecognized balancing action: %s.", balancingAction));
    }
    return inProgressTasks;
  }

  /**
   * Get the set of pending proposals for the given balancing action.
   *
   * @param balancingAction The balancing action of the requested pending proposals.
   * @return The set of pending proposals for the given balancing action.
   */
  public Set<BalancingProposal> pendingProposalsFor(BalancingAction balancingAction) {
    Set<BalancingProposal> pendingProposals = _pendingProposals.get(balancingAction);
    if (pendingProposals == null) {
      throw new IllegalStateException(String.format("Unrecognized balancing action: %s.", balancingAction));
    }
    return pendingProposals;
  }

  /**
   * Check if there is any task in progress.
   */
  public boolean hasTaskInProgress() {
    int numInProgressTasks = 0;
    for (BalancingAction balancingAction : BalancingAction.values()) {
      numInProgressTasks += _inProgressTasks.get(balancingAction).size();
    }

    return numInProgressTasks > 0;
  }

  /**
   * Get all the in progress execution tasks.
   */
  public Set<ExecutionTask> tasksInProgress() {
    Set<ExecutionTask> tasksInProgress = new HashSet<>();
    for (BalancingAction balancingAction : BalancingAction.values()) {
      tasksInProgress.addAll(_inProgressTasks.get(balancingAction));
    }

    return tasksInProgress;
  }

  /**
   * Get all the aborted execution tasks.
   */
  public Set<ExecutionTask> tasksAborted() {
    Set<ExecutionTask> tasksAborted = new HashSet<>();
    for (BalancingAction balancingAction : BalancingAction.values()) {
      tasksAborted.addAll(_abortedTasks.get(balancingAction));
    }

    return tasksAborted;
  }

  /**
   * Get all the dead execution tasks.
   */
  public Set<ExecutionTask> tasksDead() {
    Set<ExecutionTask> tasksDead = new HashSet<>();
    for (BalancingAction balancingAction : BalancingAction.values()) {
      tasksDead.addAll(_deadTasks.get(balancingAction));
    }

    return tasksDead;
  }

  public int numDeadReplicaMove() {
    return _deadTasks.get(BalancingAction.REPLICA_MOVEMENT).size();
  }

  public int numDeadLeadershipMove() {
    return _deadTasks.get(BalancingAction.LEADERSHIP_MOVEMENT).size();
  }

  public int numDeadReplicaAddition() {
    return _deadTasks.get(BalancingAction.REPLICA_ADDITION).size();
  }

  public int numDeadReplicaDeletion() {
    return _deadTasks.get(BalancingAction.REPLICA_DELETION).size();
  }

  public int numAbortedReplicaMove() {
    return _abortedTasks.get(BalancingAction.REPLICA_MOVEMENT).size();
  }

  public int numAbortedLeadershipMove() {
    return _abortedTasks.get(BalancingAction.LEADERSHIP_MOVEMENT).size();
  }

  public int numAbortedReplicaAddition() {
    return _abortedTasks.get(BalancingAction.REPLICA_ADDITION).size();
  }

  public int numAbortedReplicaDeletion() {
    return _abortedTasks.get(BalancingAction.REPLICA_DELETION).size();
  }

  public int numInProgressReplicaMove() {
    return _inProgressTasks.get(BalancingAction.REPLICA_MOVEMENT).size();
  }

  public int numInProgressLeadershipMove() {
    return _inProgressTasks.get(BalancingAction.LEADERSHIP_MOVEMENT).size();
  }

  public int numInProgressReplicaAddition() {
    return _inProgressTasks.get(BalancingAction.REPLICA_ADDITION).size();
  }

  public int numInProgressReplicaDeletion() {
    return _inProgressTasks.get(BalancingAction.REPLICA_DELETION).size();
  }

  public int numPendingReplicaMove() {
    return _pendingProposals.get(BalancingAction.REPLICA_MOVEMENT).size();
  }

  public int numPendingLeadershipMove() {
    return _pendingProposals.get(BalancingAction.LEADERSHIP_MOVEMENT).size();
  }

  public int numPendingReplicaAddition() {
    return _pendingProposals.get(BalancingAction.REPLICA_ADDITION).size();
  }

  public int numPendingReplicaDeletion() {
    return _pendingProposals.get(BalancingAction.REPLICA_DELETION).size();
  }

  /**
   * Clear (1) dead tasks, (2) aborted tasks, (3) in progress tasks, and (4) pending proposals.
   */
  public void clear() {
    // Clear dead tasks.
    for (BalancingAction balancingAction : BalancingAction.values()) {
      _deadTasks.get(balancingAction).clear();
    }
    _deadTasks.clear();
    // Clear aborted tasks.
    for (BalancingAction balancingAction : BalancingAction.values()) {
      _abortedTasks.get(balancingAction).clear();
    }
    _abortedTasks.clear();
    // Clear in progress tasks.
    for (BalancingAction balancingAction : BalancingAction.values()) {
      _inProgressTasks.get(balancingAction).clear();
    }
    _inProgressTasks.clear();
    // Clear pending proposals.
    for (BalancingAction balancingAction : BalancingAction.values()) {
      _pendingProposals.get(balancingAction).clear();
    }
    _pendingProposals.clear();
  }
}
