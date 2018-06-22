/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * A class for tracking the (1) dead tasks, (2) aborting/aborted tasks, (3) in progress tasks, and (4) pending proposals.
 *
 * This class is not thread-safe.
 */
public class ExecutionTaskTracker {
  private final Map<ExecutionTask.State, Set<ExecutionTask>> _replicaActionTasks;
  private final Map<ExecutionTask.State, Set<ExecutionTask>> _leaderActionTasks;
  private boolean _isKafkaAssignerMode;

  ExecutionTaskTracker() {
    List<ExecutionTask.State> states = ExecutionTask.State.cachedValues();
    _replicaActionTasks = new HashMap<>(states.size());
    _leaderActionTasks = new HashMap<>(states.size());

    for (ExecutionTask.State state : states) {
      _replicaActionTasks.put(state, new HashSet<>());
      _leaderActionTasks.put(state, new HashSet<>());
    }
    _isKafkaAssignerMode = false;
  }

  /**
   * Get the execution task with replica action set with the given task state.
   * @param taskState the task state to get the execution task for.
   * @return the replica action execution task set with the given task state.
   */
  public Set<ExecutionTask> taskForReplicaAction(ExecutionTask.State taskState) {
    return _replicaActionTasks.get(taskState);
  }

  /**
   * Get the execution task with leader action set with the given task state.
   * @param taskState the task state to get the execution task for.
   * @return the leader action execution task set with the given task state.
   */
  public Set<ExecutionTask> taskForLeaderAction(ExecutionTask.State taskState) {
    return _replicaActionTasks.get(taskState);
  }

  /**
   * Set the execution mode of the tasks to keep track of the ongoing execution mode via sensors.
   *
   * @param isKafkaAssignerMode True if kafka assigner mode, false otherwise.
   */
  public void setExecutionMode(boolean isKafkaAssignerMode) {
    _isKafkaAssignerMode = isKafkaAssignerMode;
  }

  /**
   * Get all the in progress execution tasks.
   */
  public Set<ExecutionTask> tasksInState(ExecutionTask.State state) {
    Set<ExecutionTask> tasksInProgress = new HashSet<>();
    tasksInProgress.addAll(_replicaActionTasks.get(state));
    tasksInProgress.addAll(_leaderActionTasks.get(state));
    return tasksInProgress;
  }

  public int numDeadReplicaAction() {
    return _replicaActionTasks.get(ExecutionTask.State.DEAD).size();
  }

  public int numDeadLeadershipAction() {
    return _leaderActionTasks.get(ExecutionTask.State.DEAD).size();
  }

  public int numAbortingReplicaAction() {
    return _replicaActionTasks.get(ExecutionTask.State.ABORTING).size();
  }

  public int numAbortingLeadershipAction() {
    return _leaderActionTasks.get(ExecutionTask.State.ABORTING).size();
  }

  public int numAbortedReplicaAction() {
    return _replicaActionTasks.get(ExecutionTask.State.ABORTED).size();
  }

  public int numAbortedLeadershipAction() {
    return _leaderActionTasks.get(ExecutionTask.State.ABORTED).size();
  }

  public int numInProgressReplicaAction() {
    return _replicaActionTasks.get(ExecutionTask.State.IN_PROGRESS).size();
  }

  public int isOngoingExecutionInKafkaAssignerMode() {
    // 1: execution in progress, 0 otherwise
    return _isKafkaAssignerMode && !_replicaActionTasks.get(ExecutionTask.State.IN_PROGRESS).isEmpty() ? 1 : 0;
  }

  public int isOngoingExecutionInNonKafkaAssignerMode() {
    // 1: execution in progress, 0 otherwise
    return !_isKafkaAssignerMode && !_replicaActionTasks.get(ExecutionTask.State.IN_PROGRESS).isEmpty() ? 1 : 0;
  }

  public int numInProgressLeadershipAction() {
    return _leaderActionTasks.get(ExecutionTask.State.IN_PROGRESS).size();
  }

  public int numPendingReplicaAction() {
    return _replicaActionTasks.get(ExecutionTask.State.PENDING).size();
  }

  public int numPendingLeadershipAction() {
    return _leaderActionTasks.get(ExecutionTask.State.PENDING).size();
  }

  /**
   * Clear the replica action and leader action tasks.
   */
  public void clear() {
    _replicaActionTasks.values().forEach(Set::clear);
    _leaderActionTasks.values().forEach(Set::clear);
  }
}
