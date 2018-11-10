/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;


public class ExecutorState {
  private static final String TRIGGERED_USER_TASK_ID = "triggeredUserTaskId";

  public enum State {
    NO_TASK_IN_PROGRESS,
    STARTING_EXECUTION,
    REPLICA_MOVEMENT_TASK_IN_PROGRESS,
    LEADER_MOVEMENT_TASK_IN_PROGRESS,
    STOPPING_EXECUTION
  }

  private final State _state;
  private final Set<ExecutionTask> _pendingPartitionMovements;
  private final Collection<ExecutionTask> _pendingLeadershipMovements;
  private final int _numFinishedPartitionMovements;
  private final int _numFinishedLeadershipMovements;
  private final Set<ExecutionTask> _inProgressPartitionMovements;
  private final Set<ExecutionTask> _abortingPartitionMovements;
  private final Set<ExecutionTask> _abortedPartitionMovements;
  private final Set<ExecutionTask> _deadPartitionMovements;
  private final long _remainingDataToMoveInMB;
  private final long _finishedDataMovementInMB;
  private final int _maximumConcurrentPartitionMovementsPerBroker;
  private final int _maximumConcurrentLeaderMovements;
  private final HttpServletRequest _triggeredUserRequest;

  private ExecutorState(State state,
                        int numFinishedPartitionMovements,
                        int numFinishedLeadershipMovements,
                        ExecutionTaskManager.ExecutionTasksSummary executionTasksSummary,
                        long finishedDataMovementInMB,
                        int maximumConcurrentPartitionMovementsPerBroker,
                        int maximumConcurrentLeaderMovements) {
    this(state,
         numFinishedPartitionMovements,
         numFinishedLeadershipMovements,
         executionTasksSummary,
         finishedDataMovementInMB,
         maximumConcurrentPartitionMovementsPerBroker,
         maximumConcurrentLeaderMovements,
         null);
  }

  private ExecutorState(State state,
                        int numFinishedPartitionMovements,
                        int numFinishedLeadershipMovements,
                        ExecutionTaskManager.ExecutionTasksSummary executionTasksSummary,
                        long finishedDataMovementInMB,
                        int maximumConcurrentPartitionMovementsPerBroker,
                        int maximumConcurrentLeaderMovements,
                        HttpServletRequest triggeredUserRequest) {
    _state = state;
    _numFinishedPartitionMovements = numFinishedPartitionMovements;
    _numFinishedLeadershipMovements = numFinishedLeadershipMovements;
    _pendingPartitionMovements = executionTasksSummary.remainingPartitionMovements();
    _pendingLeadershipMovements = executionTasksSummary.remainingLeadershipMovements();
    _inProgressPartitionMovements = executionTasksSummary.inProgressTasks();
    _abortingPartitionMovements = executionTasksSummary.abortingTasks();
    _abortedPartitionMovements = executionTasksSummary.abortedTasks();
    _deadPartitionMovements = executionTasksSummary.deadTasks();
    _remainingDataToMoveInMB = executionTasksSummary.remainingDataToMoveInMB();
    _finishedDataMovementInMB = finishedDataMovementInMB;
    _maximumConcurrentPartitionMovementsPerBroker = maximumConcurrentPartitionMovementsPerBroker;
    _maximumConcurrentLeaderMovements = maximumConcurrentLeaderMovements;
    _triggeredUserRequest = triggeredUserRequest;
  }

  public static ExecutorState noTaskInProgress() {
    return new ExecutorState(State.NO_TASK_IN_PROGRESS,
                             0,
                             0,
                             new ExecutionTaskManager.ExecutionTasksSummary(Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            0L),
                             0L,
                             0,
                             0);
  }

  public static ExecutorState executionStarted(HttpServletRequest triggeredUserRequest) {
    return new ExecutorState(State.STARTING_EXECUTION,
                             0,
                             0,
                             new ExecutionTaskManager.ExecutionTasksSummary(Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            0L),
                             0L,
                             0,
                             0,
                             triggeredUserRequest);
  }

  public static ExecutorState replicaMovementInProgress(int finishedPartitionMovements,
                                                        ExecutionTaskManager.ExecutionTasksSummary executionTasksSummary,
                                                        long finishedDataMovementInMB,
                                                        int maximumConcurrentPartitionMovementsPerBroker,
                                                        int maximumConcurrentLeaderMovements,
                                                        HttpServletRequest triggeredUserRequest) {
    return new ExecutorState(State.REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                             finishedPartitionMovements, 0, executionTasksSummary,
                             finishedDataMovementInMB, maximumConcurrentPartitionMovementsPerBroker,
                             maximumConcurrentLeaderMovements, triggeredUserRequest);
  }

  public static ExecutorState replicaMovementInProgress(int finishedPartitionMovements,
                                                        Set<ExecutionTask> remainingPartitionMovements,
                                                        Set<ExecutionTask> inProgressTasks,
                                                        Set<ExecutionTask> abortingTasks,
                                                        Set<ExecutionTask> abortedTasks,
                                                        Set<ExecutionTask> deadTasks,
                                                        long remainingDataToMoveInMB,
                                                        long finishedDataMovementInMB,
                                                        int maximumConcurrentPartitionMovementsPerBroker,
                                                        int maximumConcurrentLeaderMovements,
                                                        HttpServletRequest triggeredUserRequest) {
    return replicaMovementInProgress(finishedPartitionMovements,
                                     new ExecutionTaskManager.ExecutionTasksSummary(remainingPartitionMovements,
                                                                                    Collections.emptySet(),
                                                                                    inProgressTasks,
                                                                                    abortingTasks,
                                                                                    abortedTasks,
                                                                                    deadTasks,
                                                                                    remainingDataToMoveInMB),
                                     finishedDataMovementInMB,
                                     maximumConcurrentPartitionMovementsPerBroker,
                                     maximumConcurrentLeaderMovements,
                                     triggeredUserRequest);
  }

  public static ExecutorState leaderMovementInProgress(int finishedLeadershipMovements,
                                                       ExecutionTaskManager.ExecutionTasksSummary executionTasksSummary,
                                                       int maximumConcurrentPartitionMovementsPerBroker,
                                                       int maximumConcurrentLeaderMovements,
                                                       HttpServletRequest triggeredUserRequest) {
    return new ExecutorState(State.LEADER_MOVEMENT_TASK_IN_PROGRESS,
                             0,
                             finishedLeadershipMovements,
                             new ExecutionTaskManager.ExecutionTasksSummary(Collections.emptySet(),
                                                                            executionTasksSummary.remainingLeadershipMovements(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            0L),
                             0L,
                             maximumConcurrentPartitionMovementsPerBroker,
                             maximumConcurrentLeaderMovements,
                             triggeredUserRequest);
  }

  public static ExecutorState stopping(int finishedPartitionMovements,
                                       int finishedLeadershipMovements,
                                       ExecutionTaskManager.ExecutionTasksSummary executionTasksSummary,
                                       long finishedDataMovementInMB,
                                       int maximumConcurrentPartitionMovementsPerBroker,
                                       int maximumConcurrentLeaderMovements,
                                       HttpServletRequest triggeredUserRequest) {
    return new ExecutorState(State.STOPPING_EXECUTION,
                             finishedPartitionMovements,
                             finishedLeadershipMovements,
                             executionTasksSummary,
                             finishedDataMovementInMB,
                             maximumConcurrentPartitionMovementsPerBroker,
                             maximumConcurrentLeaderMovements,
                             triggeredUserRequest);
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

  public int numTotalLeadershipMovements() {
    return _numFinishedLeadershipMovements + _pendingLeadershipMovements.size();
  }

  public long totalDataToMoveInMB() {
    return _remainingDataToMoveInMB + _finishedDataMovementInMB;
  }

  public int numFinishedPartitionMovements() {
    return _numFinishedPartitionMovements;
  }

  public int numFinishedLeadershipMovements() {
    return _numFinishedLeadershipMovements;
  }

  public Set<ExecutionTask> pendingPartitionMovements() {
    return _pendingPartitionMovements;
  }

  public Collection<ExecutionTask> pendingLeadershipMovements() {
    return _pendingLeadershipMovements;
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

  private String triggeredUserTaskId(UserTaskManager userTaskManager) {
    if (_triggeredUserRequest == null || userTaskManager == null) {
      return "Initiated-by-AnomalyDetector";
    }

    UUID triggeredUserTaskId = userTaskManager.getUserTaskId(_triggeredUserRequest);
    return triggeredUserTaskId == null ? "Not-Yet-Available" : triggeredUserTaskId.toString();
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure(boolean verbose, UserTaskManager userTaskManager) {
    Map<String, Object> execState = new HashMap<>();
    execState.put("state", _state);
    switch (_state) {
      case NO_TASK_IN_PROGRESS:
        break;
      case STARTING_EXECUTION:
        execState.put(TRIGGERED_USER_TASK_ID, triggeredUserTaskId(userTaskManager));
        break;
      case LEADER_MOVEMENT_TASK_IN_PROGRESS:
        if (verbose) {
          List<Object> pendingLeadershipMovementList = new ArrayList<>();
          for (ExecutionTask task : _pendingLeadershipMovements) {
            pendingLeadershipMovementList.add(task.getJsonStructure());
          }
          execState.put("pendingLeadershipMovement", pendingLeadershipMovementList);
        }
        execState.put("numFinishedLeadershipMovements", _numFinishedLeadershipMovements);
        execState.put("numTotalLeadershipMovements", numTotalLeadershipMovements());
        execState.put("maximumConcurrentLeaderMovements", _maximumConcurrentLeaderMovements);
        execState.put(TRIGGERED_USER_TASK_ID, triggeredUserTaskId(userTaskManager));
        break;
      case REPLICA_MOVEMENT_TASK_IN_PROGRESS:
      case STOPPING_EXECUTION:
        execState.put("numTotalPartitionMovements", numTotalPartitionMovements());
        execState.put("numTotalLeadershipMovements", numTotalLeadershipMovements());
        execState.put("totalDataToMove", totalDataToMoveInMB());
        execState.put("numFinishedPartitionMovements", _numFinishedPartitionMovements);
        execState.put("numFinishedLeadershipMovements", _numFinishedLeadershipMovements);
        execState.put("finishedDataMovement", _finishedDataMovementInMB);
        execState.put("abortingPartitions", _abortingPartitionMovements);
        execState.put("abortedPartitions", _abortedPartitionMovements);
        execState.put("deadPartitions", _deadPartitionMovements);
        execState.put("maximumConcurrentLeaderMovements", _maximumConcurrentLeaderMovements);
        execState.put("maximumConcurrentPartitionMovementsPerBroker", _maximumConcurrentPartitionMovementsPerBroker);
        execState.put(TRIGGERED_USER_TASK_ID, triggeredUserTaskId(userTaskManager));
        if (verbose) {
          List<Object> inProgressPartitionMovementList = new ArrayList<>(_inProgressPartitionMovements.size());
          List<Object> abortingPartitionMovementList = new ArrayList<>(_abortingPartitionMovements.size());
          List<Object> abortedPartitionMovementList = new ArrayList<>(_abortedPartitionMovements.size());
          List<Object> deadPartitionMovementList = new ArrayList<>(_deadPartitionMovements.size());
          List<Object> pendingPartitionMovementList = new ArrayList<>(_pendingPartitionMovements.size());
          for (ExecutionTask task : _inProgressPartitionMovements) {
            inProgressPartitionMovementList.add(task.getJsonStructure());
          }
          for (ExecutionTask task : _abortingPartitionMovements) {
            abortingPartitionMovementList.add(task.getJsonStructure());
          }
          for (ExecutionTask task : _abortedPartitionMovements) {
            abortedPartitionMovementList.add(task.getJsonStructure());
          }
          for (ExecutionTask task : _deadPartitionMovements) {
            deadPartitionMovementList.add(task.getJsonStructure());
          }
          for (ExecutionTask task : _pendingPartitionMovements) {
            pendingPartitionMovementList.add(task.getJsonStructure());
          }
          execState.put("inProgressPartitionMovement", inProgressPartitionMovementList);
          execState.put("abortingPartitionMovement", abortingPartitionMovementList);
          execState.put("abortedPartitionMovement", abortedPartitionMovementList);
          execState.put("deadPartitionMovement", deadPartitionMovementList);
          execState.put(_state == State.STOPPING_EXECUTION ? "CancelledPartitionMovement" : "PendingPartitionMovement",
              pendingPartitionMovementList);
        }
        break;
      default:
        execState.put("error", "ILLEGAL_STATE_EXCEPTION");
        break;
    }

    return execState;
  }

  public void writeOutputStream(OutputStream out, UserTaskManager userTaskManager) throws IOException {
    switch (_state) {
      case NO_TASK_IN_PROGRESS:
        out.write(String.format("{state: %s}", _state).getBytes(StandardCharsets.UTF_8));
        break;
      case STARTING_EXECUTION:
        out.write(String.format("{state: %s, %s: %s}", _state, TRIGGERED_USER_TASK_ID, triggeredUserTaskId(userTaskManager))
                        .getBytes(StandardCharsets.UTF_8));
        break;
      case LEADER_MOVEMENT_TASK_IN_PROGRESS:
        out.write(String.format("{state: %s, finished/total leadership movements: %d/%d, "
                                + "maximum concurrent leadership movements: %d, %s: %s}", _state, _numFinishedLeadershipMovements,
                                numTotalLeadershipMovements(), _maximumConcurrentLeaderMovements, TRIGGERED_USER_TASK_ID,
                                triggeredUserTaskId(userTaskManager)).getBytes(StandardCharsets.UTF_8));
        break;
      case REPLICA_MOVEMENT_TASK_IN_PROGRESS:
      case STOPPING_EXECUTION:
        out.write(String.format("{state: %s, in-progress/aborting partitions: %d/%d, completed/total bytes(MB): %d/%d, "
                                + "finished/aborted/dead/total partitions: %d/%d/%d/%d, finished leadership movements: %d/%d, "
                                + "maximum concurrent leadership/per-broker partition movements: %d/%d, %s: %s}",
                                _state, _inProgressPartitionMovements.size(), _abortingPartitionMovements.size(),
                                _finishedDataMovementInMB, totalDataToMoveInMB(), _numFinishedPartitionMovements,
                                _abortedPartitionMovements.size(), _deadPartitionMovements.size(),
                                numTotalPartitionMovements(), _numFinishedLeadershipMovements, numTotalLeadershipMovements(),
                                _maximumConcurrentLeaderMovements, _maximumConcurrentPartitionMovementsPerBroker,
                                TRIGGERED_USER_TASK_ID, triggeredUserTaskId(userTaskManager))
                        .getBytes(StandardCharsets.UTF_8));
        break;
      default:
        throw new IllegalStateException("This should never happen");
    }
  }
}
