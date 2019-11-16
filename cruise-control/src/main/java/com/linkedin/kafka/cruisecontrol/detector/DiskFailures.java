/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.FixOfflineReplicasRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.toDateString;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType.DISK_FAILURE;

/**
 * The disk failures that have been detected.
 */
public class DiskFailures extends KafkaAnomaly {
  private final Map<Integer, Map<String, Long>> _failedDisksByBroker;
  private final String _anomalyId;
  private final FixOfflineReplicasRunnable _fixOfflineReplicasRunnable;

  public DiskFailures(KafkaCruiseControl kafkaCruiseControl,
                      Map<Integer, Map<String, Long>> failedDisksByBroker,
                      boolean allowCapacityEstimation,
                      boolean excludeRecentlyDemotedBrokers,
                      boolean excludeRecentlyRemovedBrokers,
                      List<String> selfHealingGoals) {
    if (failedDisksByBroker == null || failedDisksByBroker.isEmpty()) {
      throw new IllegalArgumentException("Unable to create disk failure anomaly with no failed disk specified.");
    }
    _failedDisksByBroker = failedDisksByBroker;
    _anomalyId = UUID.randomUUID().toString();
    _optimizationResult = null;
    _fixOfflineReplicasRunnable = new FixOfflineReplicasRunnable(kafkaCruiseControl, selfHealingGoals, allowCapacityEstimation,
                                                                 excludeRecentlyDemotedBrokers, excludeRecentlyRemovedBrokers, _anomalyId,
                                                                 String.format("Self healing for %s: %s", DISK_FAILURE, this));
  }

  /**
   * @return The failed disks and their failure time in millisecond grouped by broker.
   */
  public Map<Integer, Map<String, Long>> failedDisks() {
    return _failedDisksByBroker;
  }

  @Override
  public String anomalyId() {
    return _anomalyId;
  }

  @Override
  public boolean fix() throws KafkaCruiseControlException {
    // Fix the cluster by moving replicas off the dead disks.
    _optimizationResult = new OptimizationResult(_fixOfflineReplicasRunnable.fixOfflineReplicas(), null);
    // Ensure that only the relevant response is cached to avoid memory pressure.
    _optimizationResult.discardIrrelevantAndCacheJsonAndPlaintext();
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder().append("{\n");
    _failedDisksByBroker.forEach((brokerId, failures) -> {
      failures.forEach((logdir, eventTime) -> {
        sb.append(String.format("\tDisk %s on broker %d failed at %s\n ", logdir, brokerId, toDateString(eventTime)));
      });
    });
    sb.append("}");
    return sb.toString();
  }
}