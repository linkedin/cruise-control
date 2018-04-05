/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AbstractMetricAnomalyAnalyzer;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;


/**
 * Identifies whether there are metric anomalies in brokers for the selected metric ids.
 */
public class KafkaMetricAnomalyAnalyzer extends AbstractMetricAnomalyAnalyzer<BrokerEntity, KafkaMetricAnomaly> {
  private final List<Integer> _interestedMetrics;

  public KafkaMetricAnomalyAnalyzer(List<String> metricAnomalyAnalyzerMetrics) {
    super();
    _interestedMetrics = new ArrayList<>(metricAnomalyAnalyzerMetrics.size());
    metricAnomalyAnalyzerMetrics.stream().map(Integer::parseInt).forEach(_interestedMetrics::add);
  }

  /**
   * Check whether the given metricId is in the interested metrics for metric anomaly analysis.
   *
   * @param metricId The metric id to be checked for being interested or not.
   * @return True if the given metricId is in the interested metrics, false otherwise.
   */
  private boolean isInterested(Integer metricId) {
    return _interestedMetrics.contains(metricId);
  }

  @Override
  public KafkaMetricAnomaly createMetricAnomaly(long startTime,
                                                long endTime,
                                                String description,
                                                BrokerEntity entity,
                                                Integer metricId) {
    return new KafkaMetricAnomaly(startTime, endTime, description, entity, metricId);
  }

  /**
   * Get a collection of metric anomalies for brokers if an anomaly in their current aggregated metrics values is
   * detected for their interested metric ids, based on their history.
   *
   * @param anomalyPercentileThreshold Metric anomaly percentile threshold.
   * @param metricsHistoryByBroker Metrics history by broker entity.
   * @param currentMetricsByBroker Current metrics by broker entity.
   * @return A collection of metric anomalies for brokers if an anomaly in their current aggregated metrics values is
   * detected for their interested metric ids, based on their history.
   */
  @Override
  public Collection<KafkaMetricAnomaly> metricAnomalies(double anomalyPercentileThreshold,
                                                        Map<BrokerEntity, ValuesAndExtrapolations> metricsHistoryByBroker,
                                                        Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker) {
    Collection<KafkaMetricAnomaly> metricAnomalies =
        super.metricAnomalies(anomalyPercentileThreshold, metricsHistoryByBroker, currentMetricsByBroker);
    // Filter out uninterested metric anomalies.
    metricAnomalies.removeIf(metricAnomaly -> !isInterested(metricAnomaly.metricId()));
    return metricAnomalies;
  }
}
