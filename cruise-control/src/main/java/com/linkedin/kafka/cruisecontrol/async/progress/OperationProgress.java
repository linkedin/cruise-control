/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async.progress;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpSession;


/**
 * A class to track the progress of a task. This class is used to allow different users to trigger
 * an endpoint which may take a while for Cruise Control to respond, e.g. getting a complicated proposal.
 * Cruise Control will use {@link HttpSession} to keep track the progress of such requests and
 * report the progress to the users.
 */
public class OperationProgress {
  private boolean _mutable = true;
  private List<OperationStep> _steps = new ArrayList<>();
  private List<Long> _startTimes = new ArrayList<>();

  /**
   * Add a {@link OperationStep} to the progress.
   * @param step the operation step to add.
   */
  public synchronized void addStep(OperationStep step) {
    ensureMutable();
    _steps.add(step);
    _startTimes.add(System.currentTimeMillis());
  }

  /**
   * Refer this operation progress to another one. This is useful when multiple operations are waiting for the
   * same background task to finish.
   *
   * Once this OperationProgress is referring to another OperationProgress, this OperationProgress becomes immutable
   * to avoid accidental change of the referred OperationProgress.
   *
   * @param other the other operation progress to refer to.
   */
  public void refer(OperationProgress other) {
    // ensure the integrity and avoid dead lock.
    List<OperationStep> steps;
    List<Long> startTimes;
    synchronized (other) {
      steps = other._steps;
      startTimes = other._startTimes;
    }
    synchronized (this) {
      ensureMutable();
      this._steps = steps;
      this._startTimes = startTimes;
      this._mutable = false;
    }
  }

  /**
   * @return the list of operation steps in this operation progress.
   */
  public synchronized List<OperationStep> progress() {
    return Collections.unmodifiableList(_steps);
  }

  /**
   * Clear the progress.
   */
  public synchronized void clear() {
    ensureMutable();
    _steps.clear();
    _startTimes.clear();
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < _steps.size(); i++) {
      OperationStep step = _steps.get(i);
      long time = (i == _steps.size() - 1 ? System.currentTimeMillis() : _startTimes.get(i + 1)) - _startTimes.get(i);
      sb.append(String.format("(%6d ms) - (%3.1f%%) %s: %s%n",
                              time,  step.completionPercentage() * 100, step.name(), step.description()));
    }
    return sb.toString();
  }

  public synchronized Object[] getJsonArray() {
    Object[] progressArray = new Object[_steps.size()];
    for (int i = 0; i < _steps.size(); i++) {
      OperationStep step = _steps.get(i);
      long time = (i == _steps.size() - 1 ? System.currentTimeMillis() : _startTimes.get(i + 1)) - _startTimes.get(i);
      Map<String, Object> stepProgressMap = new HashMap<>();
      stepProgressMap.put("step", step.name());
      stepProgressMap.put("description", step.description());
      stepProgressMap.put("time-in-ms", time);
      stepProgressMap.put("completionPercentage", step.completionPercentage() * 100.0);
      progressArray[i] = stepProgressMap;
    }
    return progressArray;
  }

  private void ensureMutable() {
    if (!_mutable) {
      throw new IllegalStateException("Cannot change this operation progress because it is immutable.");
    }
  }
}
