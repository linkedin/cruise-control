/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.toDateString;

/**
 * The disk failures that have been detected.
 */
public class DiskFailures extends KafkaAnomaly {
  private static final String ID_PREFIX = AnomalyType.DISK_FAILURE.toString();
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final Map<Integer, Map<String, Long>> _failedDisksByBroker;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final boolean _excludeRecentlyRemovedBrokers;
  private final boolean _allowCapacityEstimation;
  private final String _anomalyId;
  private final List<String> _selfHealingGoals;

  public DiskFailures(KafkaCruiseControl kafkaCruiseControl,
                      Map<Integer, Map<String, Long>> failedDisksByBroker,
                      boolean allowCapacityEstimation,
                      boolean excludeRecentlyDemotedBrokers,
                      boolean excludeRecentlyRemovedBrokers,
                      List<String> selfHealingGoals) {
    if (failedDisksByBroker == null || failedDisksByBroker.isEmpty()) {
      throw new IllegalArgumentException("Unable to create disk failure anomaly with no failed disk specified.");
    }
    _kafkaCruiseControl = kafkaCruiseControl;
    _failedDisksByBroker = failedDisksByBroker;
    _allowCapacityEstimation = allowCapacityEstimation;
    _excludeRecentlyDemotedBrokers = excludeRecentlyDemotedBrokers;
    _excludeRecentlyRemovedBrokers = excludeRecentlyRemovedBrokers;
    _anomalyId = String.format("%s-%s", ID_PREFIX, UUID.randomUUID().toString().substring(ID_PREFIX.length() + 1));
    _optimizationResult = null;
    _selfHealingGoals = selfHealingGoals;
  }

  /**
   * Get the failed disks and their failure time in millisecond grouped by broker.
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
    _optimizationResult = new OptimizationResult(_kafkaCruiseControl.fixOfflineReplicas(false,
                                                                                        _selfHealingGoals,
                                                                                        null,
                                                                                        new OperationProgress(),
                                                                                        _allowCapacityEstimation,
                                                                                        null,
                                                                                        null,
                                                                                        false,
                                                                                        null,
                                                                                        null,
                                                                                        null,
                                                                                        _anomalyId,
                                                                                        _excludeRecentlyDemotedBrokers,
                                                                                        _excludeRecentlyRemovedBrokers),
                                                 null);
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