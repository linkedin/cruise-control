/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector.metricanomaly;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import java.util.Collection;
import java.util.Map;


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
}
