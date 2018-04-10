/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerEntity;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that holds Kafka metric anomalies.
 * A Kafka metric anomaly indicates unexpected rapid changes in metric values of a broker.
 */
public class KafkaMetricAnomaly implements MetricAnomaly<BrokerEntity> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricAnomaly.class);
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final String _description;
  private final BrokerEntity _brokerEntity;
  private final Integer _metricId;
  private final List<Long> _windows;

  /**
   * Kafka Metric anomaly
   *
   * @param kafkaCruiseControl The Kafka Cruise Control instance.
   * @param description The details on why this is identified as an anomaly.
   * @param brokerEntity The broker for which the anomaly was identified.
   * @param metricId The metric id  for which the anomaly was identified.
   * @param windows Thw list of windows tha the anomaly was observed.
   */
  public KafkaMetricAnomaly(KafkaCruiseControl kafkaCruiseControl,
                            String description,
                            BrokerEntity brokerEntity,
                            Integer metricId,
                            List<Long> windows) {
    _kafkaCruiseControl = kafkaCruiseControl;
    _description = description;
    _brokerEntity = brokerEntity;
    _metricId = metricId;
    _windows = windows;
  }

  /**
   * Get a list of windows for which a metric anomaly was observed.
   */
  @Override
  public List<Long> windows() {
    return _windows;
  }

  /**
   * Get the anomaly description.
   */
  @Override
  public String description() {
    return _description;
  }

  /**
   * Get the broker entity with metric anomaly.
   */
  @Override
  public BrokerEntity entity() {
    return _brokerEntity;
  }

  /**
   * Get the metric Id caused the metric anomaly.
   */
  @Override
  public Integer metricId() {
    return _metricId;
  }

  /**
   * Fix the anomaly with the system.
   */
  @Override
  public void fix() throws KafkaCruiseControlException {
    // TODO: Fix the cluster by removing the leadership from the brokers with metric anomaly (See PR#175: demote_broker).
    LOG.trace("Fix the cluster by removing the leadership from the broker: {}", _brokerEntity);
  }

  @Override
  public String toString() {
    return String.format("{%nMetric Anomaly windows: %s description: %s%n}", _windows, _description);
  }
}
