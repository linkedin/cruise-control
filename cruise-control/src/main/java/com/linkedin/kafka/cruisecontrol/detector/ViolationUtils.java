/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;


public class ViolationUtils {

  private ViolationUtils() {

  }

  /**
   * Check whether the load monitor state is unavailable -- i.e. loading or bootstrapping state.
   *
   * @param loadMonitorTaskRunnerState Load monitor task runner state.
   * @return True if the load monitor is unavailable, false otherwise.
   */
  public static boolean isUnavailableState(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState) {
    return loadMonitorTaskRunnerState == LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING
           || loadMonitorTaskRunnerState == LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.BOOTSTRAPPING;
  }
}
