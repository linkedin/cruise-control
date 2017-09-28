/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.resource.Resource;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetricSample<E> {
  private static final Logger LOG = LoggerFactory.getLogger(MetricSample.class);
  protected final E _entity;
  protected final Map<Resource, Double> _metrics;
  protected long _sampleTime;

  public MetricSample(E entity) {
    _entity = entity;
    _metrics = new HashMap<>();
    _sampleTime = -1L;
  }

  /**
   * Record a sample value for the given resource type.
   *
   * @param type        The resource type.
   * @param sampleValue the sample value.
   */
  public void record(Resource type, double sampleValue) {
    record(type, sampleValue, false);
  }

  /**
   * Record a sample value for the given resource type.
   * This is a package private function which allows metric fetcher to override the metric if necessary.
   * Currently it is only used when user enables auto cluster model coefficient training.
   *
   * When the update is from metric fetcher, it does not override the user specified value.
   *
   * @param type        The resource type.
   * @param sampleValue the sample value.
   * @param updateFromMetricFetcher indicate whether the update is from metric fetcher.
   */
  public void record(Resource type, double sampleValue, boolean updateFromMetricFetcher) {
    if (!updateFromMetricFetcher && _sampleTime >= 0) {
      throw new IllegalStateException("The metric sample has been closed.");
    }

    Double origValue = _metrics.putIfAbsent(type, sampleValue);
    if (!updateFromMetricFetcher && origValue != null) {
      throw new IllegalStateException("Trying to record sample value " + sampleValue + " for " + type +
                                          ", but there is already a value " + origValue + " recorded.");
    }
  }

  /**
   * Get the entity this metric sample is corresponding to.
   */
  public E entity() {
    return _entity;
  }

  /**
   * The time this sample was taken.
   */
  public long sampleTime() {
    return _sampleTime;
  }

  /**
   * The metric for the specified resource.
   */
  public Double metricFor(Resource resource) {
    return _metrics.get(resource);
  }

  /**
   * Close this metric sample. The timestamp will be used to determine which snapshot the metric sample will be in.
   */
  public void close(long closingTime) {
    if (closingTime < 0) {
      throw new IllegalArgumentException("The closing time cannot be negative.");
    }

    if (_sampleTime < 0) {
      _sampleTime = closingTime;
    }
  }

  /**
   * Validate the metric sample.
   */
  public boolean isValid() {
    boolean completeMetrics = _metrics.size() == Resource.values().length;
    if (!completeMetrics) {
      LOG.warn("The metric sample is discarded due to missing metrics. Sample: {}", this);
    }
    return completeMetrics;
  }
}
