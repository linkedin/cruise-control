/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AbstractMetricAnomalyAnalyzer;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerEntity;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Identifies whether there are metric anomalies in brokers for the selected metric ids.
 */
public class KafkaMetricAnomalyAnalyzer extends AbstractMetricAnomalyAnalyzer<BrokerEntity, KafkaMetricAnomaly> {
  public static final String KAFKA_CRUISE_CONTROL_OBJECT_CONFIG = "kafka.cruise.control.object";
  private KafkaCruiseControl _kafkaCruiseControl;
  private final Set<Integer> _interestedMetrics;

  public KafkaMetricAnomalyAnalyzer() {
    super();
    _interestedMetrics = new HashSet<>();
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
  public KafkaMetricAnomaly createMetricAnomaly(String description, BrokerEntity entity, Integer metricId, List<Long> windows) {
    return new KafkaMetricAnomaly(_kafkaCruiseControl, description, entity, metricId, windows);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _kafkaCruiseControl = (KafkaCruiseControl) configs.get(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
    // TODO: Enable the following check once we have getInstance without configuration.
    /*if (_kafkaCruiseControl == null) {
      throw new IllegalArgumentException("Kafka metric anomaly analyzer configuration is missing Cruise Control object.");
    }*/

    for (String metric : _metricAnomalyAnalyzerMetrics) {
      _interestedMetrics.add(Byte.toUnsignedInt(RawMetricType.valueOf(metric).id()));
    }
  }

  /**
   * Get a collection of metric anomalies for brokers if an anomaly in their current aggregated metrics values is
   * detected for their interested metric ids, based on their history.
   *
   * @param metricsHistoryByBroker Metrics history by broker entity.
   * @param currentMetricsByBroker Current metrics by broker entity.
   * @return A collection of metric anomalies for brokers if an anomaly in their current aggregated metrics values is
   * detected for their interested metric ids, based on their history.
   */
  @Override
  public Collection<KafkaMetricAnomaly> metricAnomalies(Map<BrokerEntity, ValuesAndExtrapolations> metricsHistoryByBroker,
                                                        Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker) {
    Collection<KafkaMetricAnomaly> metricAnomalies =
        super.metricAnomalies(metricsHistoryByBroker, currentMetricsByBroker);
    // Filter out uninterested metric anomalies.
    metricAnomalies.removeIf(metricAnomaly -> !isInterested(metricAnomaly.metricId()));
    return metricAnomalies;
  }
}
