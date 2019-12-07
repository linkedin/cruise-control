/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;

/**
 * A util class for anomalies.
 */
public class AnomalyUtils {

  private AnomalyUtils() {

  }

  /**
   * Extract {@link KafkaCruiseControl} instance from configs.
   *
   * @param configs The configs used to configure anomaly instance.
   * @param anomalyType The type of anomaly.
   * @return The extracted {@link KafkaCruiseControl} instance.
   */
  public static KafkaCruiseControl extractKafkaCruiseControlObjectFromConfig(Map<String, ?> configs,
                                                                            AnomalyType anomalyType) {
    KafkaCruiseControl kafkaCruiseControl = (KafkaCruiseControl) configs.get(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
    if (kafkaCruiseControl == null) {
      throw new IllegalArgumentException(String.format("Missing %s when creating anomaly of type %s.",
                                                       KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, anomalyType));
    }
    return kafkaCruiseControl;
  }

  /**
   * Check whether the load monitor state is ready -- i.e. not in loading or bootstrapping state.
   *
   * @param loadMonitorTaskRunnerState Load monitor task runner state.
   * @return True if the load monitor is ready, false otherwise.
   */
  public static boolean isLoadMonitorReady(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState) {
    return !(loadMonitorTaskRunnerState == LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING
           || loadMonitorTaskRunnerState == LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.BOOTSTRAPPING);
  }
}
