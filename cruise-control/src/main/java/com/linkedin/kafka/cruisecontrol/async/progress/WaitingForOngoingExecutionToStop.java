/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async.progress;

/**
 * A step indicating that the cluster model generation is in progress.
 */
public class WaitingForOngoingExecutionToStop implements OperationStep {
  private volatile boolean _done = false;

  /**
   * Mark the step as finished.
   */
  public void done() {
    _done = true;
  }

  @Override
  public String name() {
    return "STOPPING_ONGOING_EXECUTION";
  }

  @Override
  public float completionPercentage() {
    return _done ? 1.0f : 0.0f;
  }

  @Override
  public String description() {
    return "Waiting for ongoing execution to stop.";
  }
}
