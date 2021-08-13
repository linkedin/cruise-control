/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.common.utils.Utils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * A class to indicate how a provisioning action is handled
 */
public class ProvisionerState {
  private static final Map<State, Set<State>> VALID_TRANSFER = new HashMap<>();
  private State _state;
  private String _summary;
  private final long _createdMs;
  private long _updatedMs;

  static {
    VALID_TRANSFER.put(State.IN_PROGRESS, new HashSet<>(Collections.singleton(State.IN_PROGRESS)));
    VALID_TRANSFER.put(State.COMPLETED_WITH_ERROR, new HashSet<>(Arrays.asList(State.IN_PROGRESS, State.COMPLETED_WITH_ERROR)));
    VALID_TRANSFER.put(State.COMPLETED, new HashSet<>(Arrays.asList(State.IN_PROGRESS, State.COMPLETED_WITH_ERROR, State.COMPLETED)));
  }

  public ProvisionerState(State state, String summary) {
    _state = state;
    _summary = Utils.validateNotNull(summary, "ProvisionerState summary cannot be null.");
    _createdMs = System.currentTimeMillis();
    _updatedMs = _createdMs;
  }

  /**
   * Check if the state transfer is possible.
   * @param targetState the state to transfer to.
   * @return True if the transfer is valid, false otherwise.
   */
  public boolean canTransferToState(ProvisionerState.State targetState) {
    return VALID_TRANSFER.get(_state).contains(targetState);
  }

  /**
   * @return The state of the provisioning action.
   */
  public State state() {
    return _state;
  }

  /**
   * @return The summary of the provisioning action status.
   */
  public String summary() {
    return _summary;
  }

  /**
   * @return The time the provisioner state was created in milliseconds.
   */
  public long createdMs() {
    return _createdMs;
  }

  /**
   * @return The status update time of the provision state in milliseconds.
   */
  public long updatedMs() {
    return _updatedMs;
  }

  /**
   * Update the state and summary of the provisioning action
   *
   * @param state The new state of the provisioning action.
   * @param summary The new summary of the provisioning action status.
   * @throws IllegalArgumentException if the summary is null.
   * @throws IllegalStateException if the target state is not a valid target state.
   */
  public void update(State state, String summary) {
    if (canTransferToState(state)) {
      _state = state;
      _summary = Utils.validateNotNull(summary, "ProvisionerState summary cannot be null.");
      _updatedMs = System.currentTimeMillis();
    } else {
      throw new IllegalStateException("Cannot set the provisioner state from " + _state.toString() + " to " + state.toString()
                                      + ". The valid target states are " + Collections.unmodifiableSet(VALID_TRANSFER.get(_state)));
    }
  }

  public enum State {
    COMPLETED, COMPLETED_WITH_ERROR, IN_PROGRESS
  }

  @Override
  public String toString() {
    return String.format("ProvisionerState:{state: %s, summary: %s, createdMs: %d, updated: %d}",
                         _state.toString(),
                         _summary,
                         _createdMs,
                         _updatedMs);
  }
}
