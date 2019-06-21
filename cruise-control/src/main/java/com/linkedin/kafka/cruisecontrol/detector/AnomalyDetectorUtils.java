/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import java.util.ArrayList;
import java.util.List;


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

  /**
   * Get a list of names for goals {@link KafkaCruiseControlConfig#SELF_HEALING_GOALS_CONFIG} in the order of priority.
   */
  static List<String> getSelfHealingGoalNames(KafkaCruiseControlConfig config) {
    List<Goal> goals = config.getConfiguredInstances(KafkaCruiseControlConfig.SELF_HEALING_GOALS_CONFIG, Goal.class);
    List<String> selfHealingGoalNames = new ArrayList<>(goals.size());
    for (Goal goal : goals) {
      selfHealingGoalNames.add(goal.name());
    }
    return selfHealingGoalNames;
  }
}
