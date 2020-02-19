/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomaly;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import java.util.Map;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_DESCRIPTION_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_BROKER_ENTITIES_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_FIXABLE_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.METRIC_ANOMALY;


/**
 * A class that holds Kafka metric anomalies.
 * A Kafka metric anomaly indicates unexpected rapid changes in metric values of a broker.
 */
public class KafkaMetricAnomaly extends KafkaAnomaly implements MetricAnomaly<BrokerEntity> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricAnomaly.class);
  protected String _description;
  protected Map<BrokerEntity, Long> _brokerEntitiesWithDetectionTimeMs;
  protected Boolean _fixable;

  public KafkaMetricAnomaly() {
  }

  @Override
  public String description() {
    return _description;
  }

  @Override
  public Map<BrokerEntity, Long> entities() {
    return _brokerEntitiesWithDetectionTimeMs;
  }

  /**
   * Whether the detected metric anomaly is fixable or not.
   *
   * @return True if the anomaly is fixable.
   */
  public boolean fixable() {
    return _fixable;
  }

  /**
   * Fix the anomaly with the system.
   */
  @Override
  public boolean fix() throws KafkaCruiseControlException {
    if (_fixable) {
      // TODO: Fix the cluster by removing the leadership from the brokers with metric anomaly (See PR#175: demote_broker).
      LOG.trace("Fix the cluster by removing the leadership from the brokers: {}", _brokerEntitiesWithDetectionTimeMs.keySet());
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s anomaly with id: %s. Anomaly description: %s", METRIC_ANOMALY, anomalyId(), _description);
  }

  /**
   * Get the optimization result of self healing process, or null if no optimization result is available.
   *
   * @param isJson True for JSON response, false otherwise.
   * @return The optimization result of self healing process, or null if no optimization result is available.
   */
  public String optimizationResult(boolean isJson) {
    if (_optimizationResult == null) {
      return null;
    }
    return isJson ? _optimizationResult.cachedJSONResponse() : _optimizationResult.cachedPlaintextResponse();
  }

  @Override
  public Supplier<String> reasonSupplier() {
    return () -> String.format("Self healing for %s: %s", METRIC_ANOMALY, this);
  }

  @Override
  public AnomalyType anomalyType() {
    return METRIC_ANOMALY;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _description = (String) configs.get(METRIC_ANOMALY_DESCRIPTION_OBJECT_CONFIG);
    _fixable = (Boolean) configs.get(METRIC_ANOMALY_FIXABLE_OBJECT_CONFIG);
    if (_fixable == null) {
      throw new IllegalArgumentException(String.format("Missing %s for metric anomaly.", METRIC_ANOMALY_FIXABLE_OBJECT_CONFIG));
    }
    _brokerEntitiesWithDetectionTimeMs = (Map<BrokerEntity, Long>) configs.get(METRIC_ANOMALY_BROKER_ENTITIES_OBJECT_CONFIG);
    if (_brokerEntitiesWithDetectionTimeMs == null || _brokerEntitiesWithDetectionTimeMs.isEmpty()) {
      throw new IllegalArgumentException("Missing broker entities for metric anomaly.");
    }
    _optimizationResult = null;
  }
}
