/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Collection;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ViolationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ViolationUtils.class);

  private ViolationUtils() {

  }

  public static boolean isLoadingOrBootstrapping(LoadMonitor loadMonitor, String skipMsg) {
    // loadMonitor is null for unit tests.
    if (loadMonitor != null) {
      LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = loadMonitor.taskRunnerState();
      if (loadMonitorTaskRunnerState == LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING ||
          loadMonitorTaskRunnerState == LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.BOOTSTRAPPING) {
        LOG.info("Skipping {} because load monitor is in {} state.", skipMsg, loadMonitorTaskRunnerState);
        return true;
      }
    }
    return false;
  }

  public static boolean meetCompletenessRequirements(LoadMonitor loadMonitor, Collection<Goal> goals, String skipMsg) {
    // loadMonitor is null for unit tests.
    if (loadMonitor != null) {
      for (Goal goal : goals) {
        if (loadMonitor.meetCompletenessRequirements(goal.clusterModelCompletenessRequirements())) {
          LOG.debug("Skipping {} for {} because load completeness requirement is not met.",
                    skipMsg, goals.stream().map(Goal::name).collect(Collectors.toList()));
          return false;
        }
      }
    }
    return true;
  }
}
