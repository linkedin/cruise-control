/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState.SubState.EXECUTOR;


/**
 * A util class for Anomaly Detectors.
 */
public class AnomalyDetectorUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectorUtils.class);
  public static final long MAX_METADATA_WAIT_MS = 60000L;

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
   * Skip anomaly detection if any of the following is true:
   * <ul>
   * <li>Load monitor is not ready.</li>
   * <li>There is an ongoing execution.</li>
   * </ul>
   *
   * @return True to skip anomaly detection, false otherwise.
   */
  static boolean shouldSkipAnomalyDetection(LoadMonitor loadMonitor,
                                            KafkaCruiseControl kafkaCruiseControl) {
    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = loadMonitor.taskRunnerState();
    if (!ViolationUtils.isLoadMonitorReady(loadMonitorTaskRunnerState)) {
      LOG.info("Skipping anomaly detection because load monitor is in {} state.", loadMonitorTaskRunnerState);
      return true;
    }

    ExecutorState.State executorState = kafkaCruiseControl.state(
        new OperationProgress(), Collections.singleton(EXECUTOR)).executorState().state();
    if (executorState != ExecutorState.State.NO_TASK_IN_PROGRESS) {
      LOG.info("Skipping anomaly detection because the executor is in {} state.", executorState);
      return true;
    }

    return false;
  }
}
