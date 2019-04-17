/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import com.linkedin.kafka.cruisecontrol.detector.DiskFailures;
import com.linkedin.kafka.cruisecontrol.detector.GoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomaly;
import java.util.HashMap;
import java.util.Map;


/**
 * A no-op notifier.
 */
public class NoopNotifier implements AnomalyNotifier {
  private final Map<AnomalyType, Boolean> _selfHealingEnabled;

  public NoopNotifier() {
    _selfHealingEnabled = new HashMap<>(AnomalyType.cachedValues().size());
  }

  @Override
  public void configure(Map<String, ?> configs) {
    AnomalyType.cachedValues().forEach(anomalyType -> _selfHealingEnabled.put(anomalyType, false));
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

  @Override
  public AnomalyNotificationResult onDiskFailure(DiskFailures diskFailures) {
    return AnomalyNotificationResult.ignore();
  }

  @Override
  public Map<AnomalyType, Boolean> selfHealingEnabled() {
    return _selfHealingEnabled;
  }

  @Override
  public boolean setSelfHealingFor(AnomalyType anomalyType, boolean isSelfHealingEnabled) {
    return false;
  }
}
