/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector;

import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import java.util.Collection;
import java.util.Map;


public interface MetricAnomalyAnalyzer<M extends Entity, N extends MetricAnomaly> {

  /**
   * Get a collection of metric anomalies for entities if an anomaly in their current aggregated metrics values is
   * detected for their metric ids, based on their history.
   *
   * @param metricAnomalyPercentileThreshold Metric anomaly percentile threshold.
   * @param metricsHistoryByBroker Metrics history by entity.
   * @param currentMetricsByBroker Current metrics by entity.
   * @return A collection of metric anomalies for entities if an anomaly in their current aggregated metrics values is
   * detected for their metric ids, based on their history.
   */
  public Collection<N> metricAnomalies(double metricAnomalyPercentileThreshold,
                                       Map<M, ValuesAndExtrapolations> metricsHistoryByBroker,
                                       Map<M, ValuesAndExtrapolations> currentMetricsByBroker);
}
