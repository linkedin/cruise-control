/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RemoveBrokersRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Map;
import java.util.function.Supplier;

import static com.linkedin.cruisecontrol.CruiseControlUtils.utcDateFor;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.extractKafkaCruiseControlObjectFromConfig;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.BROKER_FAILURE;


/**
 * The broker failures that have been detected.
 */
public class BrokerFailures extends KafkaAnomaly {
  protected Map<Integer, Long> _failedBrokers;
  protected RemoveBrokersRunnable _removeBrokersRunnable;
  protected boolean _fixable;
  protected int _brokerFailureCheckWithDelayRetryCount;

  /**
   * An anomaly to indicate broker failure(s).
   */
  public BrokerFailures() {
    _detectionTimeMs = 0;
  }

  /**
   * @return The failed broker list and their failure time in millisecond.
   */
  public Map<Integer, Long> failedBrokers() {
    return _failedBrokers;
  }

  /**
   * Whether detected broker failures are fixable or not.
   * If there are too many broker failures at the same time, the anomaly is taken as unfixable and needs human intervention.
   *
   * @return {@code true} is detected broker failures are fixable.
   */
  public boolean fixable() {
    return _fixable;
  }
  
  public int anomalyFixCheckRetryCount() {
    return _brokerFailureCheckWithDelayRetryCount;
  }

  @Override
  public boolean fix() throws KafkaCruiseControlException {
    boolean hasProposalsToFix = false;
    // Fix the cluster by removing the failed brokers (mode: non-Kafka_assigner).
    if (_removeBrokersRunnable != null && _fixable) {
      _optimizationResult = new OptimizationResult(_removeBrokersRunnable.computeResult(), null);
      hasProposalsToFix = hasProposalsToFix();
      // Ensure that only the relevant response is cached to avoid memory pressure.
      _optimizationResult.discardIrrelevantAndCacheJsonAndPlaintext();
    }
    return hasProposalsToFix;
  }

  @Override
  public AnomalyType anomalyType() {
    return BROKER_FAILURE;
  }

  @Override
  public Supplier<String> reasonSupplier() {
    return () -> String.format("Self healing for %s: %s", BROKER_FAILURE, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder().append("{");
    sb.append(_fixable ? "Fixable " : " Unfixable ");
    sb.append("broker failures detected: {");
    if (_failedBrokers != null) {
      _failedBrokers.forEach((key, value) -> sb.append("Broker ").append(key).append(" failed at ").append(utcDateFor(value)).append(",\t"));
      sb.setLength(sb.length() - 2);
    }
    sb.append("}}");
    return sb.toString();
  }

  /**
   * Configure the current retry count number for the broker failure check with delay.
   * @param configs The configuration map.
   */
  protected void configureBrokerFailureCheckWithDelayRetryCount(Map<String, ?> configs) {
    if (configs.containsKey(AbstractBrokerFailureDetector.BROKER_FAILURE_CHECK_WITH_DELAY_RETRY_COUNT)) {
      _brokerFailureCheckWithDelayRetryCount = (int) configs.get(AbstractBrokerFailureDetector.BROKER_FAILURE_CHECK_WITH_DELAY_RETRY_COUNT);
    } else {
      // If unset we use the default value as 0.
      _brokerFailureCheckWithDelayRetryCount = 0;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    KafkaCruiseControl kafkaCruiseControl = extractKafkaCruiseControlObjectFromConfig(configs, BROKER_FAILURE);
    _failedBrokers = (Map<Integer, Long>) configs.get(AbstractBrokerFailureDetector.FAILED_BROKERS_OBJECT_CONFIG);
    if (_failedBrokers != null && _failedBrokers.isEmpty()) {
      throw new IllegalArgumentException("Missing broker ids for failed brokers anomaly.");
    }
    _fixable = (Boolean) configs.get(AbstractBrokerFailureDetector.BROKER_FAILURES_FIXABLE_CONFIG);
    configureBrokerFailureCheckWithDelayRetryCount(configs);
    _optimizationResult = null;
    KafkaCruiseControlConfig config = kafkaCruiseControl.config();
    boolean allowCapacityEstimation = config.getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
    boolean excludeRecentlyDemotedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
    boolean excludeRecentlyRemovedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
    _removeBrokersRunnable = _failedBrokers != null ? new RemoveBrokersRunnable(kafkaCruiseControl,
                                                                                _failedBrokers.keySet(),
                                                                                getSelfHealingGoalNames(config),
                                                                                allowCapacityEstimation,
                                                                                excludeRecentlyDemotedBrokers,
                                                                                excludeRecentlyRemovedBrokers,
                                                                                _anomalyId.toString(),
                                                                                reasonSupplier(),
                                                                                stopOngoingExecution())
                                                    : null;
    }
}
