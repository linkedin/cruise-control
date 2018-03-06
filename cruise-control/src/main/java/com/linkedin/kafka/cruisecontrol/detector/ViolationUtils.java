/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Collection;


public class ViolationUtils {

  private ViolationUtils() {

  }

  /**
   * Check whether the load monitor state is unavailable (loading or bootstrapping state). Get the state if the load
   * monitor is unavailable, null otherwise.
   *
   * @param loadMonitor Load monitor
   * @return The state if the load monitor is unavailable, null otherwise.
   */
  public static LoadMonitorTaskRunner.LoadMonitorTaskRunnerState isUnavailableState(LoadMonitor loadMonitor) {
    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = loadMonitor.taskRunnerState();
    if (loadMonitorTaskRunnerState == LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING ||
        loadMonitorTaskRunnerState == LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.BOOTSTRAPPING) {
      return loadMonitorTaskRunnerState;
    }
    return null;
  }

  /**
   * Check whether the completeness requirements are satisfied for the given goals.
   *
   * @param loadMonitor Load monitor of the cluster.
   * @param goals Goals for which the completeness requirements will be checked. an empty collection of goals represents
   *              all goals.
   * @return True if the completeness requirements are satisfied for the given goals, false otherwise.
   */
  public static boolean meetCompletenessRequirements(LoadMonitor loadMonitor, Collection<Goal> goals) {
    for (Goal goal : goals) {
      if (!loadMonitor.meetCompletenessRequirements(goal.clusterModelCompletenessRequirements())) {
        return false;
      }
    }
    return true;
  }
}
