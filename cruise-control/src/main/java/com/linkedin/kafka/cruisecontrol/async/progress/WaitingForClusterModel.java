/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async.progress;

public class WaitingForClusterModel implements OperationStep {
  private volatile boolean _done = false;

  @Override
  public String name() {
    return "WAITING_FOR_CLUSTER_MODEL";
  }

  /**
   * Mark the waiting for cluster model process as done.
   */
  public void done() {
    _done = true;
  }

  @Override
  public float completionPercentage() {
    return _done ? 1.0f : 0.0f;
  }

  @Override
  public String description() {
    return "The job requires a cluster model and it is waiting to get the cluster model lock.";
  }
}
