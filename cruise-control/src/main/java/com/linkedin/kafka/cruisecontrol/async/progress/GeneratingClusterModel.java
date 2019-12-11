/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async.progress;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * A step indicating that the cluster model generation is in progress.
 */
public class GeneratingClusterModel implements OperationStep {
  private final int _totalNumPartitions;
  private final AtomicInteger _populatedNumPartitions;

  public GeneratingClusterModel(int totalNumPartitions) {
    _totalNumPartitions = totalNumPartitions;
    _populatedNumPartitions = new AtomicInteger(0);
  }

  /**
   * Increment the counter for the number of partitions in the cluster for which the load information is populated.
   */
  public void incrementPopulatedNumPartitions() {
    _populatedNumPartitions.incrementAndGet();
  }

  @Override
  public String name() {
    return "GENERATING_CLUSTER_MODEL";
  }

  @Override
  public float completionPercentage() {
    return _totalNumPartitions <= 0 ? 1.0f : Math.min(1.0f, (float) _populatedNumPartitions.get() / _totalNumPartitions);
  }

  @Override
  public String description() {
    return "Generating the cluster model for the cluster.";
  }
}
