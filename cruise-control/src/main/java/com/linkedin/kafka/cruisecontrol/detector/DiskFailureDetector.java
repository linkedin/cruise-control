/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.MAX_METADATA_WAIT_MS;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.shouldSkipAnomalyDetection;

/**
 * This class detects disk failures.
 **/
public class DiskFailureDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DiskFailureDetector.class);
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final AdminClient _adminClient;
  private final Queue<Anomaly> _anomalies;
  private final boolean _allowCapacityEstimation;
  private int _lastCheckedClusterGeneration;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final boolean _excludeRecentlyRemovedBrokers;
  private final List<String> _selfHealingGoals;
  private final KafkaCruiseControlConfig _config;

  public DiskFailureDetector(AdminClient adminClient,
                             Queue<Anomaly> anomalies,
                             KafkaCruiseControl kafkaCruiseControl,
                             List<String> selfHealingGoals) {
    _adminClient = adminClient;
    _anomalies = anomalies;
    _lastCheckedClusterGeneration = -1;
    _kafkaCruiseControl = kafkaCruiseControl;
    KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
    _allowCapacityEstimation = config.getBoolean(KafkaCruiseControlConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
    _excludeRecentlyDemotedBrokers = config.getBoolean(KafkaCruiseControlConfig.BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
    _excludeRecentlyRemovedBrokers = config.getBoolean(KafkaCruiseControlConfig.BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
    _selfHealingGoals = selfHealingGoals;
    _config = config;
  }

  /**
   * Skip disk failure detection if any of the following is true:
   * <ul>
   * <li>Cluster model generation has not changed since the last disk failure check.</li>
   * <li>There are dead brokers in the cluster, {@link BrokerFailureDetector} should take care of the anomaly.</li>
   * <li>{@link AnomalyDetectorUtils#shouldSkipAnomalyDetection(KafkaCruiseControl)} returns true.
   * </ul>
   *
   * @return True to skip disk failure detection based on the current state, false otherwise.
   */
  private boolean shouldSkipDiskFailureDetection() {
    int currentClusterGeneration = _kafkaCruiseControl.loadMonitor().clusterModelGeneration().clusterGeneration();
    if (currentClusterGeneration == _lastCheckedClusterGeneration) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping disk failure detection because the model generation hasn't changed. Current model generation {}",
                  _kafkaCruiseControl.loadMonitor().clusterModelGeneration());
      }
      return true;
    }
    _lastCheckedClusterGeneration = currentClusterGeneration;

    Set<Integer> deadBrokers = _kafkaCruiseControl.loadMonitor().deadBrokersWithReplicas(MAX_METADATA_WAIT_MS);
    if (!deadBrokers.isEmpty()) {
      LOG.debug("Skipping disk failure detection because there are dead broker in the cluster, dead broker: {}", deadBrokers);
      return true;
    }

    return shouldSkipAnomalyDetection(_kafkaCruiseControl);
  }

  @Override
  public void run() {
    try {
      if (shouldSkipDiskFailureDetection()) {
        return;
      }
      Map<Integer, Map<String, Long>> failedDisksByBroker = new HashMap<>();
      Set<Integer> aliveBrokers = _kafkaCruiseControl.kafkaCluster().nodes().stream().mapToInt(Node::id).boxed().collect(Collectors.toSet());
      _adminClient.describeLogDirs(aliveBrokers).values().forEach((broker, future) -> {
        try {
          future.get(_config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS).forEach((logdir, info) -> {
            if (info.error != Errors.NONE) {
              failedDisksByBroker.putIfAbsent(broker, new HashMap<>());
              failedDisksByBroker.get(broker).put(logdir, _kafkaCruiseControl.timeMs());
            }
          });
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
          LOG.warn("Retrieving logdir information for broker {} encountered exception {}.", broker, e);
        }
      });
      if (!failedDisksByBroker.isEmpty()) {
        _anomalies.add(new DiskFailures(_kafkaCruiseControl,
                                        failedDisksByBroker,
                                        _allowCapacityEstimation,
                                        _excludeRecentlyDemotedBrokers,
                                        _excludeRecentlyRemovedBrokers,
                                        _selfHealingGoals));
      }
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
    } finally {
      LOG.debug("Disk failure detection finished.");
    }
  }
}