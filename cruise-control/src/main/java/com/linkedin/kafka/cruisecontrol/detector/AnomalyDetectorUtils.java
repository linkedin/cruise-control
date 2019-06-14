/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;


/**
 * A util class for Anomaly Detectors.
 */
public class AnomalyDetectorUtils {
  public static final Anomaly SHUTDOWN_ANOMALY = new BrokerFailures(null,
                                                                    null,
                                                                    true,
                                                                    true,
                                                                    true,
                                                                    null);

  private AnomalyDetectorUtils() {
  }

  static AnomalyType getAnomalyType(Anomaly anomaly) {
    if (anomaly instanceof GoalViolations) {
      return AnomalyType.GOAL_VIOLATION;
    } else if (anomaly instanceof BrokerFailures) {
      return AnomalyType.BROKER_FAILURE;
    } else if (anomaly instanceof KafkaMetricAnomaly) {
      return AnomalyType.METRIC_ANOMALY;
    } else {
      throw new IllegalStateException("Unrecognized type for anomaly " + anomaly);
    }
  }
}
