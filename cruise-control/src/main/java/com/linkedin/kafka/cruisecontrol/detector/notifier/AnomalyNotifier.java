/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import com.linkedin.kafka.cruisecontrol.detector.GoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomaly;


public interface AnomalyNotifier extends CruiseControlConfigurable {

  /**
   * When a particular goal is violated this method will be called..
   *
   * @param goalViolations The detected goal violations.
   * @return The notification result that asks Cruise Control to perform one of the following behaviors: ignore, fix or
   * perform a delayed check.
   */
  AnomalyNotificationResult onGoalViolation(GoalViolations goalViolations);

  /**
   * The method will be called when a broker failure has been detected.
   *
   * @param brokerFailures the detected broker failures
   * @return The notification result that asks Cruise Control to perform one of the following behaviors: ignore, fix or
   * perform a delayed check.
   */
  AnomalyNotificationResult onBrokerFailure(BrokerFailures brokerFailures);

  /**
   * This method is called when a metric anomaly is detected.
   *
   * @param metricAnomaly the detected metric anomaly.
   * @return The notification result that asks Cruise Control to perform one of the following behaviors: ignore, fix or
   * perform a delayed check.
   */
  AnomalyNotificationResult onMetricAnomaly(KafkaMetricAnomaly metricAnomaly);
}
