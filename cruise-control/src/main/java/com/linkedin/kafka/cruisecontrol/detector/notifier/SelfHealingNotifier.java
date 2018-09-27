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
 * <li>The goal violations will trigger an immediate fix.</li>
 * <li>The metric anomalies will trigger an immediate fix.</li>
 * <li>The broker failures are handled in the following way:
 *   <ol><li>If a broker disappears from a cluster at timestamp <b>T</b>, the detector will start down counting.</li>
 *   <li>If the broker did not re-join the cluster within broker.failure.detection.threshold.ms since <b>T</b>,
 *      the broker will be defined as dead and an alert will be triggered.</li>
 *   <li>If the broker did not re-join the cluster within broker.failure.self.healing.threshold.ms since <b>T</b>, an auto
 *      remediation will be performed.</li>
 *   </ol>
 * </li>
 *
 * <li>{@link #SELF_HEALING_ENABLED_CONFIG}: Enable self healing for all anomaly detectors, unless the particular
 * anomaly detector is explicitly disabled.</li>
 * <li>{@link #SELF_HEALING_BROKER_FAILURE_ENABLED_CONFIG}: Enable self healing for broker failure detector.</li>
 * <li>{@link #SELF_HEALING_GOAL_VIOLATION_ENABLED_CONFIG}: Enable self healing for goal violation detector.</li>
 * <li>{@link #SELF_HEALING_METRIC_ANOMALY_ENABLED_CONFIG}: Enable self healing for metric anomaly detector.</li>
 */
public class SelfHealingNotifier implements AnomalyNotifier {
  public static final String BROKER_FAILURE_ALERT_THRESHOLD_MS_CONFIG = "broker.failure.alert.threshold.ms";
  public static final String SELF_HEALING_ENABLED_CONFIG = "self.healing.enabled";
  public static final String SELF_HEALING_BROKER_FAILURE_ENABLED_CONFIG = "self.healing.broker.failure.enabled";
  public static final String SELF_HEALING_GOAL_VIOLATION_ENABLED_CONFIG = "self.healing.goal.violation.enabled";
  public static final String SELF_HEALING_METRIC_ANOMALY_ENABLED_CONFIG = "self.healing.metric.anomaly.enabled";
  public static final String BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS_CONFIG = "broker.failure.self.healing.threshold.ms";
  static final long DEFAULT_ALERT_THRESHOLD_MS = 900000;
  static final long DEFAULT_AUTO_FIX_THRESHOLD_MS = 1800000;

  private static final Logger LOG = LoggerFactory.getLogger(SelfHealingNotifier.class);
  protected final Time _time;
  protected boolean _selfHealingBrokerFailureEnabled;
  protected boolean _selfHealingGoalViolationEnabled;
  protected boolean _selfHealingMetricAnomalyEnabled;
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
    alert(goalViolations, _selfHealingGoalViolationEnabled, System.currentTimeMillis(), AnomalyType.GOAL_VIOLATION);
    return _selfHealingGoalViolationEnabled ? AnomalyNotificationResult.fix() : AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onMetricAnomaly(KafkaMetricAnomaly metricAnomaly) {
    alert(metricAnomaly, _selfHealingMetricAnomalyEnabled, System.currentTimeMillis(), AnomalyType.METRIC_ANOMALY);
    return _selfHealingMetricAnomalyEnabled ? AnomalyNotificationResult.fix() : AnomalyNotificationResult.ignore();
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
    } else if (now < selfHealingTime) {
      // Reached alert threshold. Alert but do not fix.
      alert(brokerFailures, false, selfHealingTime, AnomalyType.BROKER_FAILURE);
      long delay = selfHealingTime - now;
      result = AnomalyNotificationResult.check(delay);
    } else {
      // Reached auto fix threshold. Alert and fix if self healing is enabled.
      alert(brokerFailures, _selfHealingBrokerFailureEnabled, selfHealingTime, AnomalyType.BROKER_FAILURE);
      result = _selfHealingBrokerFailureEnabled ? AnomalyNotificationResult.fix() : AnomalyNotificationResult.ignore();
    }
    return result;
  }

  /**
   * Alert on anomaly.
   *
   * @param anomaly Detected anomaly.
   * @param autoFixTriggered True if auto fix has been triggered, false otherwise.
   * @param selfHealingStartTime The time that the self healing started.
   * @param anomalyType Type of anomaly.
   */
  public void alert(Object anomaly, boolean autoFixTriggered, long selfHealingStartTime, AnomalyType anomalyType) {
    boolean selfHealingEnabled;
    switch (anomalyType) {
      case BROKER_FAILURE:
        selfHealingEnabled = _selfHealingBrokerFailureEnabled;
        break;
      case GOAL_VIOLATION:
        selfHealingEnabled = _selfHealingGoalViolationEnabled;
        break;
      case METRIC_ANOMALY:
        selfHealingEnabled = _selfHealingMetricAnomalyEnabled;
        break;
      default:
        throw new IllegalArgumentException("Illegal anomaly type " + anomalyType + " is provided.");
    }

    LOG.warn("{} detected {}. Self healing {}.", anomalyType, anomaly,
             selfHealingEnabled ? String.format("start time %s", toDateString(selfHealingStartTime)) : "is disabled");

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
    // Global config for self healing.
    String selfHealingEnabledString = (String) config.get(SELF_HEALING_ENABLED_CONFIG);
    boolean selfHealingAllEnabled = Boolean.parseBoolean(selfHealingEnabledString);
    // Per anomaly detector configs for self healing.
    String selfHealingBrokerFailureEnabledString = (String) config.get(SELF_HEALING_BROKER_FAILURE_ENABLED_CONFIG);
    _selfHealingBrokerFailureEnabled = selfHealingBrokerFailureEnabledString == null
                                       ? selfHealingAllEnabled : Boolean.parseBoolean(selfHealingBrokerFailureEnabledString);
    String selfHealingGoalViolationEnabledString = (String) config.get(SELF_HEALING_GOAL_VIOLATION_ENABLED_CONFIG);
    _selfHealingGoalViolationEnabled = selfHealingGoalViolationEnabledString == null
                                       ? selfHealingAllEnabled : Boolean.parseBoolean(selfHealingGoalViolationEnabledString);
    String selfHealingMetricAnomalyEnabledString = (String) config.get(SELF_HEALING_METRIC_ANOMALY_ENABLED_CONFIG);
    _selfHealingMetricAnomalyEnabled = selfHealingMetricAnomalyEnabledString == null
                                       ? selfHealingAllEnabled : Boolean.parseBoolean(selfHealingMetricAnomalyEnabledString);
  }
}
