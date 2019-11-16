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

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.toDateString;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.DATE_FORMAT;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.TIME_ZONE;
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
  private static final String EXECUTION_ID = "executionId";
  private static final String TYPE = "type";
  private static final String STATE = "state";
  private static final String PROPOSAL = "proposal";
  private static final String BROKER_ID = "brokerId";
  private static final Map<State, Set<State>> VALID_TRANSFER = new HashMap<>();
  private final TaskType _type;
  private final long _executionId;
  private final ExecutionProposal _proposal;
  // _brokerId is only relevant for intra-broker replica action, otherwise it will be -1.
  private final int _brokerId;
  private State _state;
  private long _startTimeMs;
  private long _endTimeMs;
  private long _alertTimeMs;
  private boolean _slowExecutionReported;

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
   * @param brokerId The broker to operate on if the task is of type {@link TaskType#INTRA_BROKER_REPLICA_ACTION}.
   * @param type The {@link TaskType} of this task.
   * @param executionAlertingThresholdMs The alerting threshold of task execution time. If the execution time exceeds this
   *                                     threshold, {@link #maybeReportExecutionTooSlow(long, ExecutorNotifier)} will be called.
   */
  public ExecutionTask(long executionId, ExecutionProposal proposal, Integer brokerId, TaskType type, long executionAlertingThresholdMs) {
    if (type != TaskType.INTRA_BROKER_REPLICA_ACTION && brokerId != null) {
      throw new IllegalArgumentException("Broker id is specified for non-intra-broker task.");
    }
    if (executionAlertingThresholdMs <= 0) {
      throw new IllegalArgumentException(String.format("Non-positive execution alerting threshold %d is set for task %d.",
                                                       executionAlertingThresholdMs, executionId));
    }
    _executionId = executionId;
    _proposal = proposal;
    _brokerId =  brokerId == null ? -1 : brokerId;
    _state = State.PENDING;
    _type = type;
    _startTimeMs = -1L;
    _endTimeMs = -1L;
    _alertTimeMs = executionAlertingThresholdMs;
    _slowExecutionReported = false;
  }

  public ExecutionTask(long executionId, ExecutionProposal proposal, TaskType type, long executionAlertingThresholdMs) {
    this(executionId, proposal, null, type, executionAlertingThresholdMs);
  }

  /**
   * Check if the state transfer is possible.
   * @param targetState the state to transfer to.
   * @return True if the transfer is valid, false otherwise.
   */
  public boolean canTransferToState(State targetState) {
    return VALID_TRANSFER.get(_state).contains(targetState);
  }

  /**
   * @return The valid target state to transfer to.
   */
  public Set<State> validTargetState() {
    return Collections.unmodifiableSet(VALID_TRANSFER.get(_state));
  }

  /**
   * @return The execution id of this execution task.
   */
  public long executionId() {
    return _executionId;
  }

  /**
   * @return The execution proposal of this execution task.
   */
  public ExecutionProposal proposal() {
    return _proposal;
  }

  /**
   * @return The task type of this execution task.
   */
  public TaskType type() {
    return _type;
  }

  /**
   * @return The state of the task.
   */
  public State state() {
    return this._state;
  }

  /**
   * @return The timestamp that the task started.
   */
  public long startTimeMs() {
    return _startTimeMs;
  }

  /**
   * @return The timestamp that the task finishes.
   */
  public long endTimeMs() {
    return _endTimeMs;
  }

  /**
   * @return The broker id for intra-broker replica movement task.
   */
  public int brokerId() {
    return _brokerId;
  }

  /**
   * Mark task in progress.
   *
   * @param now Current system time.
   */
  public void inProgress(long now) {
    ensureValidTransfer(IN_PROGRESS);
    this._state = IN_PROGRESS;
    _startTimeMs = now;
    _alertTimeMs += now;
  }

  /**
   * Kill the task.
   *
   * @param now Current system time.
   */
  public void kill(long now) {
    ensureValidTransfer(DEAD);
    this._state = DEAD;
    _endTimeMs = now;
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
   *
   * @param now Current system time.
   */
  public void aborted(long now) {
    ensureValidTransfer(ABORTED);
    this._state = ABORTED;
    _endTimeMs = now;
  }

  /**
   * Change the task state to completed.
   *
   * @param now Current system time.
   */
  public void completed(long now) {
    ensureValidTransfer(COMPLETED);
    this._state = COMPLETED;
    _endTimeMs = now;
  }

  /**
   * Send out an alert if the task's execution time exceeds alerting threshold.
   * Note the alert will be sent out only for the first time the slow execution is detected.
   *
   * @param now Current system time.
   * @param executorNotifier The notifier to send out alert.
   */
  public void maybeReportExecutionTooSlow(long now, ExecutorNotifier executorNotifier) {
    if (_slowExecutionReported) {
      return;
    }
    if ((_state == IN_PROGRESS || _state == ABORTING) && now > _alertTimeMs) {
      executorNotifier.sendAlert(String.format("Task [%s] starts at %s and it takes too long to finish.%nTask detail: %s.",
                                               _executionId, toDateString(_startTimeMs, DATE_FORMAT, TIME_ZONE), this));
      // Mute the task to prevent sending the same alert repeatedly.
      _slowExecutionReported = true;
    }
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
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> executionStatsMap = new HashMap<>();
    executionStatsMap.put(EXECUTION_ID, _executionId);
    executionStatsMap.put(TYPE, _type);
    executionStatsMap.put(STATE, _state);
    executionStatsMap.put(PROPOSAL, _proposal.getJsonStructure());
    if (_type == TaskType.INTRA_BROKER_REPLICA_ACTION) {
      executionStatsMap.put(BROKER_ID, _brokerId);
    }
    return executionStatsMap;
  }

  private void ensureValidTransfer(State targetState) {
    if (!canTransferToState(targetState)) {
      throw new IllegalStateException("Cannot mark a task in " + _state + " to" + targetState + "state. The "
                                          + "valid target state are " + validTargetState());
    }
  }

  public enum TaskType {
    INTER_BROKER_REPLICA_ACTION, INTRA_BROKER_REPLICA_ACTION, LEADER_ACTION;

    private static final List<TaskType> CACHED_VALUES = Collections.unmodifiableList(Arrays.asList(values()));

    /**
     * Use this instead of values() because values() creates a new array each time.
     * @return enumerated values in the same order as values()
     */
    public static List<TaskType> cachedValues() {
      return CACHED_VALUES;
    }
  }

  public enum State {
    PENDING, IN_PROGRESS, ABORTING, ABORTED, DEAD, COMPLETED;

    private static final List<State> CACHED_VALUES = Collections.unmodifiableList(Arrays.asList(values()));

    /**
     * Use this instead of values() because values() creates a new array each time.
     * @return enumerated values in the same order as values()
     */
    public static List<State> cachedValues() {
      return CACHED_VALUES;
    }
  }

  @Override
  public String toString() {
    switch (_type) {
      case INTRA_BROKER_REPLICA_ACTION:
        return String.format("{EXE_ID: %d, %s(%d), %s, %s}", _executionId, _type,  _brokerId, _proposal, _state);
      case INTER_BROKER_REPLICA_ACTION:
      case LEADER_ACTION:
        return String.format("{EXE_ID: %d, %s, %s, %s}", _executionId, _type, _proposal, _state);
      default:
        throw new IllegalStateException("Unknown task type " + _type);
    }
  }

  @Override
  public int compareTo(ExecutionTask o) {
    return Long.compare(this._executionId, o._executionId);
  }
}
