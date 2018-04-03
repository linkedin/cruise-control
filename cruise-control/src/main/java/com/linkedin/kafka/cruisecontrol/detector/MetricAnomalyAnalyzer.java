/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerEntity;
import java.util.Collection;
import java.util.Map;


public interface MetricAnomalyAnalyzer {

  /**
   * Get a collection of metric anomalies for brokers if an anomaly in their current aggregated metrics values is
   * detected for their metric ids, based on their history.
   *
   * @param metricsHistoryByBroker Metrics history by broker entity.
   * @param currentMetricsByBroker Current metrics by broker entity.
   * @return A collection of metric anomalies for brokers if an anomaly in their current aggregated metrics values is
   * detected for their metric ids, based on their history.
   */
  public Collection<MetricAnomaly> metricAnomalies(Map<BrokerEntity, ValuesAndExtrapolations> metricsHistoryByBroker,
                                                   Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker);
}
