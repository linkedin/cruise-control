/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.model.Entity;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;


/**
 * A class to host a set of metric values of a given entity.
 * @param <G> The aggregation group class of the entity.
 * @param <E> the entity class
 */
public class MetricSample<G, E extends Entity<G>> {
  protected final E _entity;
  protected final Map<Short, Double> _valuesByMetricId;
  protected long _sampleTimeMs;

  public MetricSample(E entity) {
    _entity = entity;
    _valuesByMetricId = new HashMap<>();
    _sampleTimeMs = -1L;
  }

  /**
   * Record a sample value for the given metric info.
   *
   * @param info        The {@link MetricInfo}
   * @param sampleValue the sample value.
   */
  public void record(MetricInfo info, double sampleValue) {
    if (_sampleTimeMs >= 0) {
      throw new IllegalStateException("The metric sample has been closed.");
    }

    Double origValue = _valuesByMetricId.putIfAbsent(info.id(), sampleValue);
    if (origValue != null) {
      throw new IllegalStateException("Trying to record sample value " + sampleValue + " for " + info.name()
                                      + ", but there is already a value " + origValue + " recorded.");
    }
  }

  /**
   * @return The entity this metric sample is corresponding to.
   */
  public E entity() {
    return _entity;
  }

  /**
   * @return The time this sample was taken in milliseconds.
   */
  public long sampleTime() {
    return _sampleTimeMs;
  }

  /**
   * @param metricId Metric id.
   * @return The metric for the specified metric id.
   */
  public Double metricValue(short metricId) {
    return _valuesByMetricId.getOrDefault(metricId, Double.NaN);
  }

  /**
   * @return All the metrics.
   */
  public Map<Short, Double> allMetricValues() {
    return Collections.unmodifiableMap(_valuesByMetricId);
  }

  /**
   * Close this metric sample. The timestamp will be used to determine which window the metric sample will be in.
   * @param closingTimeMs closing time in milliseconds.
   */
  public void close(long closingTimeMs) {
    if (closingTimeMs < 0) {
      throw new IllegalArgumentException("Invalid closing time " + closingTimeMs
                                             + ". The closing time cannot be negative.");
    }

    if (_sampleTimeMs < 0) {
      _sampleTimeMs = closingTimeMs;
    }
  }

  /**
   * A method that can be overridden by subclasses to get prettier toString() format. If null is returned,
   * the toString() output will have the metricId instead of metric name, which is less readable.
   * @return The {@link MetricDef} used for toString() method.
   */
  protected MetricDef metricDefForToString() {
    return null;
  }

  /**
   * Validate the metric sample.
   *
   * @param metricDef The metric definitions.
   * @return {@code true} if valid, {@code false} otherwise.
   */
  public boolean isValid(MetricDef metricDef) {
    return _valuesByMetricId.size() == metricDef.size();
  }

  @Override
  public String toString() {
    MetricDef metricDef = metricDefForToString();
    if (metricDef == null) {
      return String.format("{entity=%s,sampleTime=%d,metrics=%s}", _entity, _sampleTimeMs, _valuesByMetricId);
    } else {
      StringJoiner sj = new StringJoiner(",", "{", "}");
      sj.add(String.format("entity=(%s)", _entity));
      sj.add(String.format("sampleTime=%d", _sampleTimeMs));
      for (MetricInfo metricInfo : metricDefForToString().all()) {
        sj.add(String.format("%s=%.3f", metricInfo.name(), _valuesByMetricId.get(metricInfo.id())));
      }
      return sj.toString();
    }
  }
}
