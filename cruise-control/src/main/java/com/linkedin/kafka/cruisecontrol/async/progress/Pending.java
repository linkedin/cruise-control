/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async.progress;

/**
 * Indicating that the operation is still in the queue and hasn't started execution yet.
 */
public class Pending implements OperationStep {
  private volatile boolean _done = false;

  @Override
  public String name() {
    return "PENDING";
  }

  @Override
  public float completionPercentage() {
    return _done ? 1.0f : 0.0f;
  }

  @Override
  public String description() {
    return "Operation enqueued, waiting to be executed.";
  }

  /**
   * Mark the pending process as done.
   */
  public void done() {
    _done = true;
  }
}
