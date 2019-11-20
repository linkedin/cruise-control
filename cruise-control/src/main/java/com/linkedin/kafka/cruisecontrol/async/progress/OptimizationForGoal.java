/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async.progress;

/**
 * Indicating the goal optimization is in progress.
 */
public class OptimizationForGoal implements OperationStep {
  private final String _goalName;
  private volatile boolean _completed = false;

  public OptimizationForGoal(String goalName) {
    _goalName = goalName;
  }

  /**
   * Mark the optimization process as done.
   */
  public void done() {
    _completed = true;
  }

  @Override
  public String name() {
    return "OPTIMIZING " + _goalName;
  }

  @Override
  public float completionPercentage() {
    return _completed ? 1.0f : 0.0f;
  }

  @Override
  public String description() {
    return "Optimizing goal " + _goalName;
  }
}
