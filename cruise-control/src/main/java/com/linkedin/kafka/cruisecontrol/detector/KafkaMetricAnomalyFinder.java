/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.config.CruiseControlConfig;
import com.linkedin.cruisecontrol.detector.metricanomaly.PercentileMetricAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerEntity;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_LOG_FLUSH_TIME_MS_MAX;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_LOG_FLUSH_TIME_MS_MEAN;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_MAX;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_MEAN;


/**
 * Identifies whether there are metric anomalies in brokers for the selected metric ids.
 */
public class KafkaMetricAnomalyFinder extends PercentileMetricAnomalyFinder<BrokerEntity> {
  private static final String DEFAULT_METRICS =
      new StringJoiner(",").add(BROKER_PRODUCE_LOCAL_TIME_MS_MAX.toString())
                           .add(BROKER_PRODUCE_LOCAL_TIME_MS_MEAN.toString())
                           .add(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX.toString())
                           .add(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN.toString())
                           .add(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX.toString())
                           .add(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN.toString())
                           .add(BROKER_LOG_FLUSH_TIME_MS_MAX.toString())
                           .add(BROKER_LOG_FLUSH_TIME_MS_MEAN.toString()).toString();
  private KafkaCruiseControl _kafkaCruiseControl;

  @Override
  protected String toMetricName(Integer metricId) {
    return KafkaMetricDef.brokerMetricDef().metricInfo(metricId).name();
  }

  @Override
  public KafkaMetricAnomaly createMetricAnomaly(String description, BrokerEntity entity, Integer metricId, List<Long> windows) {
    return new KafkaMetricAnomaly(_kafkaCruiseControl, description, entity, metricId, windows);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs) {
    String interestedMetrics = (String) configs.get(CruiseControlConfig.METRIC_ANOMALY_FINDER_METRICS_CONFIG);
    if (interestedMetrics == null || interestedMetrics.isEmpty()) {
      ((Map<String, Object>) configs).put(CruiseControlConfig.METRIC_ANOMALY_FINDER_METRICS_CONFIG, DEFAULT_METRICS);
    }
    super.configure(configs);
    _kafkaCruiseControl = (KafkaCruiseControl) configs.get(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
    if (_kafkaCruiseControl == null) {
      throw new IllegalArgumentException("Kafka metric anomaly analyzer configuration is missing Cruise Control object.");
    }
  }
}
