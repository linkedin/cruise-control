/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async.progress;

/**
 * This is the step when retrieving the workload snapshot from
 * {@link com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator}
 */
public class RetrievingMetrics implements OperationStep {
  private volatile boolean _done = false;

  /**
   * Mark the step as finished.
   */
  public void done() {
    _done = true;
  }

  @Override
  public String name() {
    return "AGGREGATING_METRICS";
  }

  @Override
  public float completionPercentage() {
    return _done ? 1.0f : 0.0f;
  }

  @Override
  public String description() {
    return "Retrieve the metrics of all the partitions from the aggregated metrics.";
  }
}
