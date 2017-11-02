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
    LEADER_MOVEMENT_TASK_IN_PROGRESS;
  }

  private final State _state;
  private final Set<ExecutionTask> _pendingPartitionMovements;
  private final int _numFinishedPartitionMovements;
  private final Set<ExecutionTask> _inProgressPartitionMovements;
  private final long _remainingDataToMoveInMB;
  private final long _finishedDataMovementInMB;

  private ExecutorState(State state,
                        int numFinishedPartitionMovements,
                        Set<ExecutionTask> pendingPartitionMovements,
                        Set<ExecutionTask> inProgressPartitionMovements,
                        long remainingDataToMoveInMB,
                        long finishedDataMovementInMB) {
    _state = state;
    _pendingPartitionMovements = pendingPartitionMovements;
    _inProgressPartitionMovements = inProgressPartitionMovements;
    _numFinishedPartitionMovements = numFinishedPartitionMovements;
    _remainingDataToMoveInMB = remainingDataToMoveInMB;
    _finishedDataMovementInMB = finishedDataMovementInMB;
  }

  public static ExecutorState noTaskInProgress() {
    return new ExecutorState(State.NO_TASK_IN_PROGRESS,
                             0,
                             Collections.emptySet(),
                             Collections.emptySet(),
                             0L,
                             0L);
  }

  public static ExecutorState executionStarted() {
    return new ExecutorState(State.EXECUTION_STARTED,
                             0,
                             Collections.emptySet(),
                             Collections.emptySet(),
                             0L,
                             0L);
  }

  public static ExecutorState replicaMovementInProgress(int finishedPartitionMovements,
                                                        Set<ExecutionTask> pendingPartitionMovements,
                                                        Set<ExecutionTask> inProgressPartitionMovements,
                                                        long remainingDataToMoveInMB,
                                                        long finishedDataMovementInMB) {
    return new ExecutorState(State.REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                             finishedPartitionMovements,
                             pendingPartitionMovements,
                             inProgressPartitionMovements,
                             remainingDataToMoveInMB,
                             finishedDataMovementInMB);
  }

  public static ExecutorState leaderMovementInProgress() {
    return new ExecutorState(State.LEADER_MOVEMENT_TASK_IN_PROGRESS,
                             0,
                             Collections.emptySet(),
                             Collections.emptySet(),
                             0L,
                             0L);
  }

  public State state() {
    return _state;
  }

  public int numTotalPartitionMovements() {
    return _pendingPartitionMovements.size()
        + _inProgressPartitionMovements.size()
        + _numFinishedPartitionMovements;
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
        execState.put("state", _state);
        execState.put("numTotalPartitions", numTotalPartitionMovements());
        execState.put("totalDataToMove", totalDataToMoveInMB());
        execState.put("numFinishedPartitions", _numFinishedPartitionMovements);
        execState.put("finishedDataMovement", _finishedDataMovementInMB);
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
        return String.format("{state: %s, total partitions(MB): %d/%d, finished partitions: %d/%d}",
                             _state, _finishedDataMovementInMB, totalDataToMoveInMB(),
                             _numFinishedPartitionMovements, numTotalPartitionMovements());
      default:
        throw new IllegalStateException("This should never happen");
    }
  }
}
