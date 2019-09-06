/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RemoveBrokersRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.toDateString;


/**
 * The broker failures that have been detected.
 */
public class BrokerFailures extends KafkaAnomaly {
  private static final String ID_PREFIX = AnomalyType.BROKER_FAILURE.toString();
  private final Map<Integer, Long> _failedBrokers;
  private final String _anomalyId;
  private final RemoveBrokersRunnable _removeBrokersRunnable;

  /**
   * An anomaly to indicate broker failure(s).
   *
   * @param kafkaCruiseControl The Kafka Cruise Control instance.
   * @param failedBrokers Failed broker ids by the detection time, or null for {@link AnomalyDetectorUtils#SHUTDOWN_ANOMALY}
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @param excludeRecentlyDemotedBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
   * @param excludeRecentlyRemovedBrokers Exclude recently removed brokers from proposal generation for replica transfer.
   * @param selfHealingGoals Goals used for self healing. An empty list indicates the default goals.
   */
  public BrokerFailures(KafkaCruiseControl kafkaCruiseControl,
                        Map<Integer, Long> failedBrokers,
                        boolean allowCapacityEstimation,
                        boolean excludeRecentlyDemotedBrokers,
                        boolean excludeRecentlyRemovedBrokers,
                        List<String> selfHealingGoals) {
    _failedBrokers = failedBrokers;
    if (_failedBrokers != null && _failedBrokers.isEmpty()) {
      throw new IllegalArgumentException("Missing broker ids for failed brokers.");
    }
    _anomalyId = String.format("%s-%s", ID_PREFIX, UUID.randomUUID().toString().substring(ID_PREFIX.length() + 1));
    _optimizationResult = null;
    _removeBrokersRunnable = _failedBrokers != null ? new RemoveBrokersRunnable(kafkaCruiseControl,
                                                                                _failedBrokers.keySet(),
                                                                                selfHealingGoals,
                                                                                allowCapacityEstimation,
                                                                                excludeRecentlyDemotedBrokers,
                                                                                excludeRecentlyRemovedBrokers,
                                                                                _anomalyId)
                                                    : null;
  }

  /**
   * Get the failed broker list and their failure time in millisecond.
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
}
