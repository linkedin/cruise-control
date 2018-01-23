/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async.progress;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * This is the step when retrieving the workload snapshot from
 * {@link com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator}
 */
public class RetrievingMetrics implements OperationStep {
  private final int _totalNumTopics;
  private AtomicInteger _retrievedTopics;

  public RetrievingMetrics(int totalNumTopics) {
    _totalNumTopics = totalNumTopics;
    _retrievedTopics = new AtomicInteger(0);
  }

  /**
   * Mark the step as finished.
   */
  public void done() {
    _retrievedTopics.set(_totalNumTopics);
  }

  /**
   * Increment the number of retrieved topics by 1.
   */
  public void incrementRetrievedTopics() {
    _retrievedTopics.incrementAndGet();
  }

  @Override
  public String name() {
    return "AGGREGATING_METRICS";
  }

  @Override
  public float completionPercentage() {
    return _totalNumTopics <= 0 ? 1.0f : ((float) _retrievedTopics.get() / _totalNumTopics);
  }

  @Override
  public String description() {
    return "Retrieve the metrics of all the partitions from the aggregated metrics.";
  }
}
