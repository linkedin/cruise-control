/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector.metricanomaly;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import java.util.Collection;
import java.util.Map;


/**
 * An interface to provide custom finders for metric anomalies.
 * @param <E> The entity for which metrics will be tracked.
 */
public interface MetricAnomalyFinder<E extends Entity> extends CruiseControlConfigurable {

  /**
   * Get a collection of metric anomalies for entities if an anomaly in their current aggregated metrics values is
   * detected for their metric ids, based on their history.
   *
   * @param metricsHistoryByEntity Metrics history by entity.
   * @param currentMetricsByEntity Current metrics by entity.
   * @return A collection of metric anomalies for entities if an anomaly in their current aggregated metrics values is
   * detected for their metric ids, based on their history.
   */
  Collection<MetricAnomaly<E>> metricAnomalies(Map<E, ValuesAndExtrapolations> metricsHistoryByEntity,
                                               Map<E, ValuesAndExtrapolations> currentMetricsByEntity);

  /**
   * Get the latest number of metric anomalies with the given type detected by this metric anomaly finder.
   *
   * @param type Metric anomaly type for which the latest number of metric anomalies is queried.
   * @return The latest number of metric anomalies with the given type detected by this metric anomaly finder.
   */
  int numAnomaliesOfType(MetricAnomalyType type);
}
