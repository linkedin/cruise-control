/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.model.Entity;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class to host a set of metric values of a given entity.
 * @param <G> The aggregation group class of the entity.
 * @param <E> the entity class
 */
public class MetricSample<G, E extends Entity<G>> {
  private static final Logger LOG = LoggerFactory.getLogger(MetricSample.class);
  protected final E _entity;
  protected final Map<Integer, Double> _metrics;
  protected long _sampleTime;

  public MetricSample(E entity) {
    _entity = entity;
    _metrics = new HashMap<>();
    _sampleTime = -1L;
  }

  /**
   * Record a sample value for the given resource type.
   * This is a package private function which allows metric fetcher to override the metric if necessary.
   * Currently it is only used when user enables auto cluster model coefficient training.
   *
   * When the update is from metric fetcher, it does not override the user specified value.
   *
   * @param info        The {@link MetricInfo}
   * @param sampleValue the sample value.
   */
  public void record(MetricInfo info, double sampleValue) {
    if (_sampleTime >= 0) {
      throw new IllegalStateException("The metric sample has been closed.");
    }

    Double origValue = _metrics.putIfAbsent(info.id(), sampleValue);
    if (origValue != null) {
      throw new IllegalStateException("Trying to record sample value " + sampleValue + " for " + info.name() +
                                          ", but there is already a value " + origValue + " recorded.");
    }
  }

  /**
   * Record a sample value for the given metric name.
   * This is a package private function which allows metric fetcher to override the metric if necessary.
   * Currently it is only used when user enables auto cluster model coefficient training.
   *
   * When the update is from metric fetcher, it does not override the user specified value.
   *
   * @param metricName  The metric name.
   * @param sampleValue The sample value.
   * @param metricDef   The metric definition.
   */
  public void record(String metricName, double sampleValue, MetricDef metricDef) {
    if (_sampleTime >= 0) {
      throw new IllegalStateException("The metric sample has been closed.");
    }

    Double origValue = _metrics.putIfAbsent(metricDef.metricInfo(metricName).id(), sampleValue);
    if (origValue != null) {
      throw new IllegalStateException("Trying to record sample value " + sampleValue + " for " + metricName +
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
  public Double metricFor(int metricId) {
    return _metrics.getOrDefault(metricId, Double.NaN);
  }

  /**
   * @return all the metrics.
   */
  public Map<Integer, Double> allMetrics() {
    return Collections.unmodifiableMap(_metrics);
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
  public boolean isValid(MetricDef metricDef) {
    return _metrics.size() == metricDef.size();
  }

  @Override
  public String toString() {
    return String.format("(entity=%s,metrics=%s,sampleTime=%d)", _entity, _metrics, _sampleTime);
  }
}
