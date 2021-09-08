/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.metricdef;

import com.linkedin.cruisecontrol.CruiseControlUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;


/**
 * The definition of metrics. Each metric will be assigned a metric id and used by the {@link MetricSampleAggregator}
 * to look up the metrics.
 *
 * Also, each metric can belong to a metric group. The metrics in the same metric group are expected to be of
 * the same type and unit. For example, the bytes in rate of the entire system may be divided into the bytes in
 * rate of a few subsystems. In this case, the metric bytes in rate of the subsystems can be defined within
 * the same metric group, so that they can be accumulated to the bytes in rate of the entire system.
 *
 * This class is supposed to be initialized only once and passed around after the creation for read only.
 */
public class MetricDef {
  private final AtomicInteger _nextIndex;
  private final Map<String, MetricInfo> _metricInfoByName;
  private final List<MetricInfo> _metricInfoByIndex;
  private final Map<String, List<MetricInfo>> _metricInfoByGroup;
  private final Set<Short> _metricsToPredict;
  private volatile boolean _doneDefinition = false;

  public MetricDef() {
    _nextIndex = new AtomicInteger(0);
    _metricInfoByName = new HashMap<>();
    _metricInfoByIndex = new ArrayList<>();
    _metricInfoByGroup = new HashMap<>();
    _metricsToPredict = new HashSet<>();
  }

  /**
   * Define the metric.
   *
   * @param metricName the name of the metric
   * @param group the metric group this metric belongs to.
   * @param valueComputingStrategy the {@link AggregationFunction} for this metric.
   * @return This MetricDef
   */
  public synchronized MetricDef define(String metricName, String group, String valueComputingStrategy) {
    return define(metricName, group, valueComputingStrategy, false);
  }

  /**
   * Define the metric.
   *
   * @param metricName the name of the metric
   * @param group the group the metric belongs to.
   * @param valueComputingStrategy the {@link AggregationFunction} for this metric.
   * @param toPredict whether the metric is a metric to be predicted.
   * @return This MetricDef
   */
  public synchronized MetricDef define(String metricName,
                                       String group,
                                       String valueComputingStrategy,
                                       boolean toPredict) {
    if (_doneDefinition) {
      throw new IllegalStateException("Cannot add definition after the metric definition is done.");
    }
    CruiseControlUtils.ensureValidString("metricName", metricName);
    CruiseControlUtils.ensureValidString("valueComputingStrategy", valueComputingStrategy);

    _metricInfoByName.compute(metricName, (k, v) -> {
      if (v != null) {
        throw new IllegalArgumentException("Metric " + metricName + " is already defined");
      }
      short metricId = getAndIncrementMetricId();
      if (toPredict) {
        _metricsToPredict.add(metricId);
      }
      return new MetricInfo(metricName, metricId, AggregationFunction.valueOf(valueComputingStrategy.toUpperCase()), group);
    });

    MetricInfo info = _metricInfoByName.get(metricName);
    _metricInfoByIndex.add(info.id(), info);
    if (group != null && !group.isEmpty()) {
      _metricInfoByGroup.computeIfAbsent(group, g -> new ArrayList<>()).add(info);
    }
    return this;
  }

  private short getAndIncrementMetricId() {
    int metricId = _nextIndex.getAndIncrement();
    if (metricId > Short.MAX_VALUE) {
      throw new IllegalStateException(String.format("Metric Ids beyond %d are not supported.", Short.MAX_VALUE));
    }
    return (short) metricId;
  }

  /**
   * Get all the metric info for the given group.
   * @param group the group to get the info.
   *
   * @return All the metric information in the given group.
   */
  public List<MetricInfo> metricInfoForGroup(String group) {
    return _metricInfoByGroup.getOrDefault(group, Collections.emptyList());
  }

  /**
   * Finish the metric definition and make the MetricDef immutable.
   */
  public synchronized void doneDefinition() {
    _doneDefinition = true;
  }

  /**
   * Get the metric id from the metric name.
   * @param name the metric name.
   * @return The {@link MetricInfo} associated with the metric name.
   */
  public MetricInfo metricInfo(String name) {
    MetricInfo info = _metricInfoByName.get(name);
    if (info == null) {
      throw new IllegalArgumentException("Metric name " + name + " is not defined. Currently defined metrics are "
                                             + _metricInfoByName);
    }
    return info;
  }

  /**
   * @param id The index corresponding to the requested {@link MetricInfo}.
   * @return The {@link MetricInfo} by id;
   */
  public MetricInfo metricInfo(short id) {
    if (id >= _nextIndex.get()) {
      throw new IllegalArgumentException("Metric Id " + id + " is not defined. Currently defined metrics are "
                                             + _metricInfoByIndex);
    }
    return _metricInfoByIndex.get(id);
  }

  /**
   * @return A set of metric ids that are to be predicted.
   */
  public Set<Short> metricsToPredict() {
    return Collections.unmodifiableSet(_metricsToPredict);
  }

  public List<MetricInfo> all() {
    return Collections.unmodifiableList(_metricInfoByIndex);
  }

  public int size() {
    return _metricInfoByName.size();
  }
}
