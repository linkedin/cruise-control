/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

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

  public DiskFailures(KafkaCruiseControl kafkaCruiseControl,
                      Map<Integer, Map<String, Long>> failedDisksByBroker,
                      boolean allowCapacityEstimation,
                      boolean excludeRecentlyDemotedBrokers,
                      boolean excludeRecentlyRemovedBrokers) {
    _kafkaCruiseControl = kafkaCruiseControl;
    _failedDisksByBroker = failedDisksByBroker;
    _allowCapacityEstimation = allowCapacityEstimation;
    _excludeRecentlyDemotedBrokers = excludeRecentlyDemotedBrokers;
    _excludeRecentlyRemovedBrokers = excludeRecentlyRemovedBrokers;
    _anomalyId = String.format("%s-%s", ID_PREFIX, UUID.randomUUID().toString().substring(ID_PREFIX.length() + 1));
    _optimizationResult = null;
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
    if (_failedDisksByBroker != null && !_failedDisksByBroker.isEmpty()) {
      _optimizationResult = new OptimizationResult(_kafkaCruiseControl.fixOfflineReplicas(false,
                                                                                          Collections.emptyList(),
                                                                                          null,
                                                                                          new OperationProgress(),
                                                                                          _allowCapacityEstimation,
                                                                                          null,
                                                                                          null,
                                                                                          false,
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
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder().append("{\n");
    _failedDisksByBroker.forEach((brokerId, failures) -> {
      failures.forEach((logdir, eventTime) -> {
        Date date = new Date(eventTime);
        DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        sb.append(String.format("\tDisk %s on broker %d failed at %s\n ", logdir, brokerId, format.format(date)));
      });
    });
    sb.append("}");
    return sb.toString();
  }
}