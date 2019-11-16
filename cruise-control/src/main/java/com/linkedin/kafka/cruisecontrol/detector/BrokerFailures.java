/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RemoveBrokersRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Map;
import java.util.UUID;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.toDateString;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType.BROKER_FAILURE;


/**
 * The broker failures that have been detected.
 */
public class BrokerFailures extends KafkaAnomaly implements CruiseControlConfigurable {
  protected Map<Integer, Long> _failedBrokers;
  protected String _anomalyId;
  protected RemoveBrokersRunnable _removeBrokersRunnable;

  /**
   * An anomaly to indicate broker failure(s).
   */
  public BrokerFailures() {
  }

  /**
   * @return The failed broker list and their failure time in millisecond.
   */
  public Map<Integer, Long> failedBrokers() {
    return _failedBrokers;
  }

  @Override
  public String anomalyId() {
    return _anomalyId;
  }

  @Override
  public boolean fix() throws KafkaCruiseControlException {
    // Fix the cluster by removing the failed brokers (mode: non-Kafka_assigner).
    if (_removeBrokersRunnable != null) {
      _optimizationResult = new OptimizationResult(_removeBrokersRunnable.removeBrokers(), null);
      // Ensure that only the relevant response is cached to avoid memory pressure.
      _optimizationResult.discardIrrelevantAndCacheJsonAndPlaintext();
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder().append("{\n");
    _failedBrokers.forEach((key, value) -> {
      sb.append("\tBroker ").append(key).append(" failed at ").append(toDateString(value)).append("\n");
    });
    sb.append("}");
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs) {
    KafkaCruiseControl kafkaCruiseControl = (KafkaCruiseControl) configs.get(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
    if (kafkaCruiseControl != null) {
      _failedBrokers = (Map<Integer, Long>) configs.get(BrokerFailureDetector.FAILED_BROKERS_OBJECT_CONFIG);
      if (_failedBrokers != null && _failedBrokers.isEmpty()) {
        throw new IllegalArgumentException("Missing broker ids for failed brokers.");
      }
      _anomalyId = UUID.randomUUID().toString();
      _optimizationResult = null;
      KafkaCruiseControlConfig config = kafkaCruiseControl.config();
      boolean allowCapacityEstimation = config.getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
      boolean excludeRecentlyDemotedBrokers = config.getBoolean(BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
      boolean excludeRecentlyRemovedBrokers = config.getBoolean(BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
      _removeBrokersRunnable = _failedBrokers != null
                               ? new RemoveBrokersRunnable(kafkaCruiseControl,
                                                           _failedBrokers.keySet(),
                                                           getSelfHealingGoalNames(config),
                                                           allowCapacityEstimation,
                                                           excludeRecentlyDemotedBrokers,
                                                           excludeRecentlyRemovedBrokers,
                                                           _anomalyId,
                                                           String.format("Self healing for %s: %s", BROKER_FAILURE, this))
                               : null;
    }
  }
}
