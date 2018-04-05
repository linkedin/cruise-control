/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import com.linkedin.kafka.cruisecontrol.detector.GoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomaly;
import java.util.Map;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.toDateString;


/**
 * This class implements a logic of self-healing when anomaly is detected.
 *
 * The goal violations will trigger an immediate fix.
 *
 * The broker failures are handled in the following way:
 * 1. If a broker disappears from a cluster at timestamp <b>T</b>, the detector will start down counting.
 * 2. If the broker did not re-join the cluster within broker.failure.detection.threshold.ms since <b>T</b>,
 *    the broker will be defined as dead and an alert will be triggered.
 * 3. If the broker did not re-join the cluster within broker.failure.self.healing.threshold.ms since <b>T</b>, an auto
 *    remediation will be performed.
 *
 */
public class SelfHealingNotifier implements AnomalyNotifier {
  public static final String BROKER_FAILURE_ALERT_THRESHOLD_MS_CONFIG = "broker.failure.alert.threshold.ms";
  public static final String SELF_HEALING_ENABLED_CONFIG = "self.healing.enabled";
  public static final String BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS_CONFIG = "broker.failure.self.healing.threshold.ms";
  static final long DEFAULT_ALERT_THRESHOLD_MS = 900000;
  static final long DEFAULT_AUTO_FIX_THRESHOLD_MS = 1800000;

  private static final Logger LOG = LoggerFactory.getLogger(SelfHealingNotifier.class);
  protected final Time _time;
  protected boolean _selfHealingEnabled;
  protected long _brokerFailureAlertThresholdMs;
  protected long _selfHealingThresholdMs;

  public SelfHealingNotifier() {
    _time = new SystemTime();
  }

  /**
   * Package private constructor for unit test.
   */
  SelfHealingNotifier(Time time) {
    _time = time;
  }

  @Override
  public AnomalyNotificationResult onGoalViolation(GoalViolations goalViolations) {
    alert(goalViolations, _selfHealingEnabled, System.currentTimeMillis());
    return _selfHealingEnabled ? AnomalyNotificationResult.fix() : AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onMetricAnomaly(KafkaMetricAnomaly metricAnomaly) {
    alert(metricAnomaly, _selfHealingEnabled, System.currentTimeMillis());
    return _selfHealingEnabled ? AnomalyNotificationResult.fix() : AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onBrokerFailure(BrokerFailures brokerFailures) {
    long earliestFailureTime = Long.MAX_VALUE;
    for (long t : brokerFailures.failedBrokers().values()) {
      earliestFailureTime = Math.min(earliestFailureTime, t);
    }
    long now = _time.milliseconds();
    long alertTime = earliestFailureTime + _brokerFailureAlertThresholdMs;
    long selfHealingTime = earliestFailureTime + _selfHealingThresholdMs;
    AnomalyNotificationResult result = null;
    if (now < alertTime) {
      // Not reaching alerting threshold yet.
      long delay = alertTime - now;
      result = AnomalyNotificationResult.check(delay);
    } else if (now >= alertTime && now < selfHealingTime) {
      // Reached alert threshold. Alert but do not fix.
      alert(brokerFailures, false, selfHealingTime);
      long delay = selfHealingTime - now;
      result = AnomalyNotificationResult.check(delay);
    } else if (now >= selfHealingTime) {
      // Reached auto fix threshold. Alert and fix if self healing is enabled.
      alert(brokerFailures, _selfHealingEnabled, selfHealingTime);
      result = _selfHealingEnabled ? AnomalyNotificationResult.fix() : AnomalyNotificationResult.ignore();
    }
    return result;
  }

  /**
   * Alert on broker failures.
   * @param anomaly the failed brokers.
   * @param autoFixTriggered whether the auto fix has been triggered or not.
   */
  public void alert(Object anomaly, boolean autoFixTriggered, long selfHealingStartTime) {
    if (_selfHealingEnabled) {
      LOG.warn("Anomaly detected {}, plan self-healing start time {}.", anomaly, toDateString(selfHealingStartTime));
    } else {
      LOG.warn("Anomaly detected {}. Self healing is disabled.", anomaly);
    }
    if (autoFixTriggered) {
      LOG.warn("Self-healing has been triggered.");
    }
  }

  @Override
  public void configure(Map<String, ?> config) {
    String alertThreshold = (String) config.get(BROKER_FAILURE_ALERT_THRESHOLD_MS_CONFIG);
    _brokerFailureAlertThresholdMs = alertThreshold == null ? DEFAULT_ALERT_THRESHOLD_MS : Long.parseLong(alertThreshold);
    String fixThreshold = (String) config.get(BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS_CONFIG);
    _selfHealingThresholdMs = fixThreshold == null ? DEFAULT_AUTO_FIX_THRESHOLD_MS : Long.parseLong(fixThreshold);
    if (_brokerFailureAlertThresholdMs > _selfHealingThresholdMs) {
      throw new IllegalArgumentException(String.format("The failure detection threshold %d cannot be larger than "
                                                           + "the auto fix threshold. %d",
                                                       _brokerFailureAlertThresholdMs, _selfHealingThresholdMs));
    }
    String selfHealingEnabledString = (String) config.get(SELF_HEALING_ENABLED_CONFIG);
    _selfHealingEnabled = selfHealingEnabledString != null && Boolean.parseBoolean(selfHealingEnabledString);
  }
}
