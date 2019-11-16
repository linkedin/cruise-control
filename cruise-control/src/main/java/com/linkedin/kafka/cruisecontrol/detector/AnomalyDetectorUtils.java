/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A util class for Anomaly Detectors.
 */
public class AnomalyDetectorUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectorUtils.class);
  public static final String KAFKA_CRUISE_CONTROL_OBJECT_CONFIG = "kafka.cruise.control.object";
  public static final long MAX_METADATA_WAIT_MS = 60000L;
  public static final Anomaly SHUTDOWN_ANOMALY = new BrokerFailures();

  private AnomalyDetectorUtils() {
  }

  static AnomalyType getAnomalyType(Anomaly anomaly) {
    if (anomaly instanceof GoalViolations) {
      return AnomalyType.GOAL_VIOLATION;
    } else if (anomaly instanceof BrokerFailures) {
      return AnomalyType.BROKER_FAILURE;
    } else if (anomaly instanceof KafkaMetricAnomaly) {
      return AnomalyType.METRIC_ANOMALY;
    } else if (anomaly instanceof DiskFailures) {
      return AnomalyType.DISK_FAILURE;
    } else {
      throw new IllegalStateException("Unrecognized type for anomaly " + anomaly);
    }
  }

  /**
   * @return A list of names for goals {@link KafkaCruiseControlConfig#SELF_HEALING_GOALS_CONFIG} in the order of priority.
   */
  public static List<String> getSelfHealingGoalNames(KafkaCruiseControlConfig config) {
    List<Goal> goals = config.getConfiguredInstances(KafkaCruiseControlConfig.SELF_HEALING_GOALS_CONFIG, Goal.class);
    List<String> selfHealingGoalNames = new ArrayList<>(goals.size());
    for (Goal goal : goals) {
      selfHealingGoalNames.add(goal.name());
    }
    return selfHealingGoalNames;
  }

  /**
   * Skip anomaly detection if any of the following is true:
   * <ul>
   * <li>Load monitor is not ready.</li>
   * <li>There is an ongoing execution.</li>
   * </ul>
   *
   * @return True to skip anomaly detection, false otherwise.
   */
  static boolean shouldSkipAnomalyDetection(KafkaCruiseControl kafkaCruiseControl) {
    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = kafkaCruiseControl.getLoadMonitorTaskRunnerState();
    if (!ViolationUtils.isLoadMonitorReady(loadMonitorTaskRunnerState)) {
      LOG.info("Skipping anomaly detection because load monitor is in {} state.", loadMonitorTaskRunnerState);
      return true;
    }

    ExecutorState.State executionState = kafkaCruiseControl.executionState();
    if (executionState != ExecutorState.State.NO_TASK_IN_PROGRESS) {
      LOG.info("Skipping anomaly detection because the executor is in {} state.", executionState);
      return true;
    }

    return false;
  }
}
