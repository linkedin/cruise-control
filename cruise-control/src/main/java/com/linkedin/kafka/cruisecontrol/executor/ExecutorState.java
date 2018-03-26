/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.Collections;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;


public class ExecutorState {

  public enum State {
    NO_TASK_IN_PROGRESS,
    EXECUTION_STARTED,
    REPLICA_MOVEMENT_TASK_IN_PROGRESS,
    LEADER_MOVEMENT_TASK_IN_PROGRESS,
    STOPPING_EXECUTION
  }

  private final State _state;
  private final Set<ExecutionTask> _pendingPartitionMovements;
  private final int _numFinishedPartitionMovements;
  private final Set<ExecutionTask> _inProgressPartitionMovements;
  private final Set<ExecutionTask> _abortingPartitionMovements;
  private final Set<ExecutionTask> _abortedPartitionMovements;
  private final Set<ExecutionTask> _deadPartitionMovements;
  private final long _remainingDataToMoveInMB;
  private final long _finishedDataMovementInMB;

  private ExecutorState(State state,
                        int numFinishedPartitionMovements,
                        ExecutionTaskManager.ExecutionTasksSummary executionTasksSummary,
                        long finishedDataMovementInMB) {
    _state = state;
    _numFinishedPartitionMovements = numFinishedPartitionMovements;
    _pendingPartitionMovements = executionTasksSummary.remainingPartitionMovements();
    _inProgressPartitionMovements = executionTasksSummary.inProgressTasks();
    _abortingPartitionMovements = executionTasksSummary.abortingTasks();
    _abortedPartitionMovements = executionTasksSummary.abortedTasks();
    _deadPartitionMovements = executionTasksSummary.deadTasks();
    _remainingDataToMoveInMB = executionTasksSummary.remainingDataToMoveInMB();
    _finishedDataMovementInMB = finishedDataMovementInMB;
  }

  public static ExecutorState noTaskInProgress() {
    return new ExecutorState(State.NO_TASK_IN_PROGRESS,
                             0,
                             new ExecutionTaskManager.ExecutionTasksSummary(Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            0L),
                             0L);
  }

  public static ExecutorState executionStarted() {
    return new ExecutorState(State.EXECUTION_STARTED,
                             0,
                             new ExecutionTaskManager.ExecutionTasksSummary(Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            0L),
                             0L);
  }

  public static ExecutorState replicaMovementInProgress(int finishedPartitionMovements,
                                                        ExecutionTaskManager.ExecutionTasksSummary executionTasksSummary,
                                                        long finishedDataMovementInMB) {
    return new ExecutorState(State.REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                             finishedPartitionMovements, executionTasksSummary,
                             finishedDataMovementInMB);
  }

  public static ExecutorState replicaMovementInProgress(int finishedPartitionMovements,
                                                        Set<ExecutionTask> remainingPartitionMovements,
                                                        Set<ExecutionTask> inProgressTasks,
                                                        Set<ExecutionTask> abortingTasks,
                                                        Set<ExecutionTask> abortedTasks,
                                                        Set<ExecutionTask> deadTasks,
                                                        long remainingDataToMoveInMB,
                                                        long finishedDataMovementInMB) {
    return new ExecutorState(State.REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                             finishedPartitionMovements,
                             new ExecutionTaskManager.ExecutionTasksSummary(remainingPartitionMovements,
                                                                            inProgressTasks,
                                                                            abortingTasks,
                                                                            abortedTasks,
                                                                            deadTasks,
                                                                            remainingDataToMoveInMB),
                             finishedDataMovementInMB);
  }

  public static ExecutorState leaderMovementInProgress() {
    return new ExecutorState(State.LEADER_MOVEMENT_TASK_IN_PROGRESS,
                             0,
                             new ExecutionTaskManager.ExecutionTasksSummary(Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            0L),
                             0L);
  }

  public static ExecutorState stopping(int finishedPartitionMovements,
                                       ExecutionTaskManager.ExecutionTasksSummary executionTasksSummary,
                                       long finishedDataMovementInMB) {
    return new ExecutorState(State.STOPPING_EXECUTION,
                             finishedPartitionMovements, executionTasksSummary,
                             finishedDataMovementInMB);
  }

  public State state() {
    return _state;
  }

  public int numTotalPartitionMovements() {
    return _pendingPartitionMovements.size()
        + _inProgressPartitionMovements.size()
        + _numFinishedPartitionMovements
        + _abortingPartitionMovements.size()
        + _abortedPartitionMovements.size();
  }

  public long totalDataToMoveInMB() {
    return _remainingDataToMoveInMB + _finishedDataMovementInMB;
  }

  public int numFinishedPartitionMovements() {
    return _numFinishedPartitionMovements;
  }

  public Set<ExecutionTask> pendingPartitionMovements() {
    return _pendingPartitionMovements;
  }

  public Set<ExecutionTask> inProgressPartitionMovements() {
    return _inProgressPartitionMovements;
  }

  public Set<ExecutionTask> abortingPartitionMovements() {
    return _abortingPartitionMovements;
  }

  public Set<ExecutionTask> abortedPartitionMovements() {
    return _abortedPartitionMovements;
  }

  public Set<ExecutionTask> deadPartitionMovements() {
    return _deadPartitionMovements;
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> execState = new HashMap<>();
    switch (_state) {
      case NO_TASK_IN_PROGRESS:
      case EXECUTION_STARTED:
      case LEADER_MOVEMENT_TASK_IN_PROGRESS:
        execState.put("state", _state);
        break;
      case REPLICA_MOVEMENT_TASK_IN_PROGRESS:
      case STOPPING_EXECUTION:
        execState.put("state", _state);
        execState.put("numTotalPartitions", numTotalPartitionMovements());
        execState.put("totalDataToMove", totalDataToMoveInMB());
        execState.put("numFinishedPartitions", _numFinishedPartitionMovements);
        execState.put("finishedDataMovement", _finishedDataMovementInMB);
        execState.put("abortingPartitions", _abortingPartitionMovements);
        execState.put("abortedPartitions", _abortedPartitionMovements);
        execState.put("deadPartitions", _deadPartitionMovements);
        break;
      default:
        execState.put("state", _state);
        execState.put("error", "ILLEGAL_STATE_EXCEPTION");
        break;
    }
    return execState;
  }

  @Override
  public String toString() {
    switch (_state) {
      case NO_TASK_IN_PROGRESS:
      case EXECUTION_STARTED:
      case LEADER_MOVEMENT_TASK_IN_PROGRESS:
        return String.format("{state: %s}", _state);

      case REPLICA_MOVEMENT_TASK_IN_PROGRESS:
      case STOPPING_EXECUTION:
        return String.format("{state: %s, in-progress/aborting partitions: %d/%d, completed/total bytes(MB): %d/%d, "
                                 + "finished/aborted/dead/total partitions: %d/%d/%d/%d}",
                             _state, _inProgressPartitionMovements.size(), _abortingPartitionMovements.size(),
                             _finishedDataMovementInMB, totalDataToMoveInMB(), _numFinishedPartitionMovements,
                             _abortedPartitionMovements.size(), _deadPartitionMovements.size(),
                             numTotalPartitionMovements());
      default:
        throw new IllegalStateException("This should never happen");
    }
  }
}
