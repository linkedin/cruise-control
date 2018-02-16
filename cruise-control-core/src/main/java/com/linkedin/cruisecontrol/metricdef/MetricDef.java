/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.metricdef;

import com.linkedin.cruisecontrol.CruiseControlUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;


/**
 * The definition of metrics. Each metric will be assigned a metric id and used by the {@link MetricSampleAggregator}
 * to look up the metrics.
 */
public class MetricDef {
  private final AtomicInteger _nextIndex;
  private final Map<String, MetricInfo> _metricNameToIndex;
  private final SortedMap<Integer, MetricInfo> _indexToMetricInfo;
  private final Set<Integer> _metricsToPredict;

  public MetricDef() {
    _nextIndex = new AtomicInteger(0);
    _metricNameToIndex = new HashMap<>();
    _indexToMetricInfo = new TreeMap<>();
    _metricsToPredict = new HashSet<>();
  }

  /**
   * Define the metric.
   *
   * @param metricName the name of the metric
   * @param valueComputingStrategy the {@link ValueComputingStrategy} for this metric.
   * @return this MetricDef
   */
  public synchronized MetricDef define(String metricName,
                                       String valueComputingStrategy) {
    return define(metricName, valueComputingStrategy, false);
  }

  /**
   * Define the metric.
   *
   * @param metricName the name of the metric
   * @param valueComputingStrategy the {@link ValueComputingStrategy} for this metric.
   * @param toPredict whether the metric is a metric to be predicted.
   * @return this MetricDef
   */
  public synchronized MetricDef define(String metricName,
                                       String valueComputingStrategy,
                                       boolean toPredict) {
    CruiseControlUtils.ensureValidString("metricName", metricName);
    CruiseControlUtils.ensureValidString("valueComputingStrategy", valueComputingStrategy);

    _metricNameToIndex.compute(metricName, (k, v) -> {
      if (v != null) {
        throw new IllegalArgumentException("Metric " + metricName + " is already defined");
      }
      int metricId = _nextIndex.getAndIncrement();
      if (toPredict) {
        _metricsToPredict.add(metricId);
      }
      return new MetricInfo(metricName,
                            metricId,
                            ValueComputingStrategy.valueOf(valueComputingStrategy.toUpperCase()));
    });
    MetricInfo info = _metricNameToIndex.get(metricName);
    _indexToMetricInfo.put(info.id(), info);
    return this;
  }

  /**
   * Get the metric id from the metric name.
   * @param name the metric name.
   * @return the {@link MetricInfo} associated with the metric name.
   */
  public MetricInfo metricInfo(String name) {
    MetricInfo info = _metricNameToIndex.get(name);
    if (info == null) {
      throw new IllegalArgumentException("Metric name " + name + " is not defined. Currently defined metrics are "
                                             + _metricNameToIndex);
    }
    return info;
  }

  /**
   * @return the {@link MetricInfo} by id;
   */
  public MetricInfo metricInfo(int id) {
    if (id >= _nextIndex.get()) {
      throw new IllegalArgumentException("Metric Id " + id + " is not defined. Currently defined metrics are "
                                             + _indexToMetricInfo);
    }
    return _indexToMetricInfo.get(id);
  }

  /**
   * @return A set of metric ids that are to be predicted.
   */
  public Set<Integer> metricsToPredict() {
    return _metricsToPredict;
  }

  public Map<Integer, MetricInfo> all() {
    return Collections.unmodifiableMap(_indexToMetricInfo);
  }

  public int size() {
    return _metricNameToIndex.size();
  }
}
