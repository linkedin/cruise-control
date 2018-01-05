/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.State.*;


/**
 * A class that wraps the execution information of a balancing proposal
 * 
 * The task state machine is the following:
 * 
 * <pre>
 * PENDING ---> IN_PROGRESS ------------> COMPLETED
 *                  |           
 *                  |           
 *                  |----> ABORTING ----> ABORTED
 *                  |          |
 *                  |          v
 *                  |-------------------> DEAD
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
  // The execution id of the proposal so we can keep track of the task when execute it.
  public final long executionId;
  // The corresponding balancing proposal of this task.
  public final BalancingProposal proposal;
  private volatile State _state;
  
  static {
    VALID_TRANSFER.put(PENDING, new HashSet<>(Collections.singleton(IN_PROGRESS)));
    VALID_TRANSFER.put(IN_PROGRESS, new HashSet<>(Arrays.asList(ABORTING, DEAD, COMPLETED)));
    VALID_TRANSFER.put(ABORTING, new HashSet<>(Arrays.asList(ABORTED, DEAD)));
    VALID_TRANSFER.put(COMPLETED, Collections.emptySet());
    VALID_TRANSFER.put(DEAD, Collections.emptySet());
    VALID_TRANSFER.put(ABORTED, Collections.emptySet());
  }
  
  public ExecutionTask(long executionId, BalancingProposal proposal) {
    this.executionId = executionId;
    this.proposal = proposal;
    this._state = State.PENDING;
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
   * @return The source broker of this execution task.
   */
  public Integer sourceBrokerId() {
    return proposal.sourceBrokerId();
  }

  /**
   * @return The destination broker of this execution task.
   */
  public Integer destinationBrokerId() {
    return proposal.destinationBrokerId();
  }

  /**
   * @return the state of the task.
   */
  public State state() {
    return this._state;
  }

  /**
   * Mark task in progress.
   */
  public void inProgress() {
    ensureValidTransfer(IN_PROGRESS);
    this._state = IN_PROGRESS;
  }

  /**
   * Kill the task.
   */
  public void kill() {
    ensureValidTransfer(DEAD);
    this._state = DEAD;
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
  public void aborted() {
    ensureValidTransfer(ABORTED);
    this._state = ABORTED;
  }

  /**
   * Change the task state to completed.
   */
  public void completed() {
    ensureValidTransfer(COMPLETED);
    this._state = COMPLETED;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof ExecutionTask && this.executionId == ((ExecutionTask) o).executionId;
  }

  @Override
  public int hashCode() {
    return (int) executionId;
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> executionStatsMap = new HashMap<>();
    executionStatsMap.put("executionId", executionId);
    executionStatsMap.put("proposal", proposal.getJsonStructure());
    return executionStatsMap;
  }
  
  private void ensureValidTransfer(State targetState) {
    if (!canTransferToState(targetState)) {
      throw new IllegalStateException("Cannot mark a task in " + _state + " to" + targetState + "state. The "
                                          + "valid target state are " + validTargetState());
    }
  }

  public enum State {
    PENDING, IN_PROGRESS, ABORTING, ABORTED, DEAD, COMPLETED
  }

  @Override
  public String toString() {
    return String.format("{EXE_ID: %d, %s, %s}", executionId, proposal, _state);
  }

  @Override
  public int compareTo(ExecutionTask o) {
    if (this.executionId > o.executionId) {
      return 1;
    } else if (this.executionId == o.executionId) {
      return 0;
    } else {
      return -1;
    }
  }
}
