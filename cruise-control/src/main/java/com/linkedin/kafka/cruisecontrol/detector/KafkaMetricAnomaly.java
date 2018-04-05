/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.MetricAnomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerEntity;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * A class that holds Kafka metric anomalies.
 * A Kafka metric anomaly indicates unexpected rapid changes in metric values of a broker.
 */
public class KafkaMetricAnomaly implements MetricAnomaly<BrokerEntity, KafkaCruiseControl, KafkaCruiseControlException> {
  private final long _startTime;
  private final long _endTime;
  private final String _description;
  private final BrokerEntity _brokerEntity;
  private final Integer _metricId;

  /**
   * Kafka Metric anomaly
   *
   * @param startTime The start time of the anomaly.
   * @param endTime The last time that the anomaly was observed.
   * @param description The details on why this is identified as an anomaly.
   * @param brokerEntity The broker for which the anomaly was identified.
   * @param metricId The metric id  for which the anomaly was identified.
   */
  public KafkaMetricAnomaly(long startTime, long endTime, String description, BrokerEntity brokerEntity, Integer metricId) {
    _startTime = startTime;
    _endTime = endTime;
    _description = description;
    _brokerEntity = brokerEntity;
    _metricId = metricId;
  }

  /**
   * Get the start time of the metric anomaly observation.
   */
  @Override
  public long startTime() {
    return _startTime;
  }

  /**
   * Get the end time of the metric anomaly observation.
   */
  @Override
  public long endTime() {
    return _endTime;
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

  @Override
  public void fix(KafkaCruiseControl kafkaCruiseControl) throws KafkaCruiseControlException {
    // TODO: Fix the cluster by removing the leadership from the brokers with metric anomaly (See PR#175: demote_broker).
  }

  @Override
  public String toString() {
    Date startDate = new Date(_startTime);
    Date endDate = new Date(_endTime);
    DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    return String.format("{%nMetric Anomaly start: %s end: %s description: %s%n}",
                         format.format(startDate), format.format(endDate), _description);
  }
}
