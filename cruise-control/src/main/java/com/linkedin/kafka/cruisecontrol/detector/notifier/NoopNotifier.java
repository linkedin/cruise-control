/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import com.linkedin.kafka.cruisecontrol.detector.GoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomaly;
import java.util.Map;


/**
 * A no-op notifier.
 */
public class NoopNotifier implements AnomalyNotifier {

  @Override
  public void configure(Map<String, ?> configs) {

  }

  @Override
  public AnomalyNotificationResult onGoalViolation(GoalViolations goalViolations) {
    return AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onBrokerFailure(BrokerFailures brokerFailures) {
    return AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onMetricAnomaly(KafkaMetricAnomaly metricAnomaly) {
    return AnomalyNotificationResult.ignore();
  }
}
