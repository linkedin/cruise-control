/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.State.*;


/**
 * A class that wraps the execution information of a balancing proposal
 *
 * The task state machine is the following:
 *
 * <pre>
 * PENDING ---&gt; IN_PROGRESS ------------&gt; COMPLETED
 *                  |
 *                  |
 *                  |----&gt; ABORTING ----&gt; ABORTED
 *                  |          |
 *                  |          v
 *                  |-------------------&gt; DEAD
 *
 * A newly created task is in <tt>PENDING</tt> state.
 * A <tt>PENDING</tt> task becomes <tt>IN_PROGRESS</tt> when it is drained from the {@link ExecutionTaskPlanner}
 * An <tt>IN_PROGRESS</tt> task becomes <tt>COMPLETED</tt> if the execution is done without error.
 * An <tt>IN_PROGRESS</tt> task becomes <tt>ABORTING</tt> if an error is encountered and the rollback is possible.
 * An <tt>IN_PROGRESS</tt> task becomes <tt>DEAD</tt> if an error is encountered and the rollback is not possible.
 * An <tt>ABORTING</tt> task becomes <tt>ABORTED</tt> if the rollback of the original task is successfully done.
 * An <tt>ABORTING</tt> task becomes <tt>DEAD</tt> if the rollback of the original task encountered an error.
 * </pre>
 */
public class ExecutionTask implements Comparable<ExecutionTask> {
  private static final Map<State, Set<State>> VALID_TRANSFER = new HashMap<>();
  private final TaskType _type;
  private final long _executionId;
  private final ExecutionProposal _proposal;
  private State _state;
  private long _startTime;
  private long _endTime;

  static {
    VALID_TRANSFER.put(PENDING, new HashSet<>(Collections.singleton(IN_PROGRESS)));
    VALID_TRANSFER.put(IN_PROGRESS, new HashSet<>(Arrays.asList(ABORTING, DEAD, COMPLETED)));
    VALID_TRANSFER.put(ABORTING, new HashSet<>(Arrays.asList(ABORTED, DEAD)));
    VALID_TRANSFER.put(COMPLETED, Collections.emptySet());
    VALID_TRANSFER.put(DEAD, Collections.emptySet());
    VALID_TRANSFER.put(ABORTED, Collections.emptySet());
  }

  /**
   * Construct an execution task.
   *
   * @param executionId The execution id of the proposal so we can keep track of the task when execute it.
   * @param proposal The corresponding balancing proposal of this task.
   * @param type the {@link TaskType} of this task.
   */
  public ExecutionTask(long executionId, ExecutionProposal proposal, TaskType type) {
    _executionId = executionId;
    _proposal = proposal;
    _state = State.PENDING;
    _type = type;
    _startTime = -1L;
    _endTime = -1L;

  }

  /**
   * Check if the state transfer is possible.
   * @param targetState the state to transfer to.
   * @return true if the transfer is valid, false otherwise.
   */
  public boolean canTransferToState(State targetState) {
    return VALID_TRANSFER.get(_state).contains(targetState);
  }

  /**
   * @return the valid target state to transfer to.
   */
  public Set<State> validTargetState() {
    return Collections.unmodifiableSet(VALID_TRANSFER.get(_state));
  }

  /**
   * @return the execution id of this execution task.
   */
  public long executionId() {
    return _executionId;
  }

  /**
   * @return the execution proposal of this execution task.
   */
  public ExecutionProposal proposal() {
    return _proposal;
  }

  /**
   * @return the task type of this execution task.
   */
  public TaskType type() {
    return _type;
  }

  /**
   * @return the state of the task.
   */
  public State state() {
    return this._state;
  }

  /**
   * @return the timestamp that the task started.
   */
  public long startTime() {
    return _startTime;
  }

  /**
   * @return the timestamp that the task finishes.
   */
  public long endTime() {
    return _endTime;
  }

  /**
   * Mark task in progress.
   */
  public void inProgress(long now) {
    ensureValidTransfer(IN_PROGRESS);
    this._state = IN_PROGRESS;
    _startTime = now;
  }

  /**
   * Kill the task.
   */
  public void kill(long now) {
    ensureValidTransfer(DEAD);
    this._state = DEAD;
    _endTime = now;
  }

  /**
   * Abort the task.
   */
  public void abort() {
    ensureValidTransfer(ABORTING);
    this._state = ABORTING;
  }

  /**
   * Change the task state to aborted.
   */
  public void aborted(long now) {
    ensureValidTransfer(ABORTED);
    this._state = ABORTED;
    _endTime = now;
  }

  /**
   * Change the task state to completed.
   */
  public void completed(long now) {
    ensureValidTransfer(COMPLETED);
    this._state = COMPLETED;
    _endTime = now;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof ExecutionTask && _executionId == ((ExecutionTask) o)._executionId;
  }

  @Override
  public int hashCode() {
    return (int) _executionId;
  }

  /**
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> executionStatsMap = new HashMap<>();
    executionStatsMap.put("executionId", _executionId);
    executionStatsMap.put("type", _type);
    executionStatsMap.put("state", _state);
    executionStatsMap.put("proposal", _proposal.getJsonStructure());
    return executionStatsMap;
  }

  private void ensureValidTransfer(State targetState) {
    if (!canTransferToState(targetState)) {
      throw new IllegalStateException("Cannot mark a task in " + _state + " to" + targetState + "state. The "
                                          + "valid target state are " + validTargetState());
    }
  }

  public enum TaskType {
    REPLICA_ACTION, LEADER_ACTION
  }

  public enum State {
    PENDING, IN_PROGRESS, ABORTING, ABORTED, DEAD, COMPLETED;

    private static final List<State> CACHED_VALUES =
        Arrays.asList(PENDING, IN_PROGRESS, ABORTING, ABORTED, DEAD, COMPLETED);

    public static List<State> cachedValues() {
      return CACHED_VALUES;
    }
  }

  @Override
  public String toString() {
    return String.format("{EXE_ID: %d, %s, %s, %s}", _executionId, _type, _proposal, _state);
  }

  @Override
  public int compareTo(ExecutionTask o) {
    return Long.compare(this._executionId, o._executionId);
  }
}
