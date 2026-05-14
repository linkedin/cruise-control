/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Helper class for managing Kafka's intra-broker replication throttle (log dir reassignment throttle).
 * This uses the broker config {@code replica.alter.log.dirs.io.max.bytes.per.second} to throttle
 * intra-broker replica movements (KIP-113).
 */
class IntraBrokerReplicationThrottleHelper {
  private static final Logger LOG = LoggerFactory.getLogger(IntraBrokerReplicationThrottleHelper.class);
  static final String REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG = "replica.alter.log.dirs.io.max.bytes.per.second";
  public static final long CLIENT_REQUEST_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
  static final int RETRIES = 30;

  private final AdminClient _adminClient;
  private final Long _throttleRate;
  private final int _retries;
  private final Set<Integer> _throttledBrokers;
  // Tracks the original throttle value per broker before this execution overwrote it (null means no prior value)
  private final Map<Integer, String> _originalThrottleValues;

  IntraBrokerReplicationThrottleHelper(AdminClient adminClient, Long throttleRate) {
    this(adminClient, throttleRate, RETRIES);
  }

  // for testing
  IntraBrokerReplicationThrottleHelper(AdminClient adminClient, Long throttleRate, int retries) {
    _adminClient = adminClient;
    _throttleRate = throttleRate;
    _retries = retries;
    _throttledBrokers = new HashSet<>();
    _originalThrottleValues = new HashMap<>();
  }

  void setThrottles(List<ExecutionTask> tasksToExecute)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (throttlingEnabled()) {
      LOG.info("Setting an intra-broker rebalance throttle of {} bytes/sec", _throttleRate);
      Set<Integer> participatingBrokers = getParticipatingBrokers(tasksToExecute);
      for (int broker : participatingBrokers) {
        setThrottledRateIfNecessary(broker);
      }
    }
  }

  void clearThrottles(List<ExecutionTask> completedTasks, List<ExecutionTask> inProgressTasks)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (throttlingEnabled()) {
      Set<Integer> brokersWithCompletedTasks = getParticipatingBrokers(
          completedTasks.stream()
              .filter(this::shouldRemoveThrottleForTask)
              .collect(Collectors.toList()));

      Set<Integer> brokersWithInProgressTasks = getParticipatingBrokers(
          inProgressTasks.stream()
              .filter(this::taskIsInProgress)
              .collect(Collectors.toList()));

      Set<Integer> brokersToRemoveThrottlesFrom = new TreeSet<>(brokersWithCompletedTasks);
      brokersToRemoveThrottlesFrom.removeAll(brokersWithInProgressTasks);

      LOG.info("Removing intra-broker replica movement throttles from brokers: {}", brokersToRemoveThrottlesFrom);
      for (int broker : brokersToRemoveThrottlesFrom) {
        removeThrottledRateFromBroker(broker);
      }
    }
  }

  /**
   * Remove throttle from all brokers that were throttled during this execution.
   * Used as a final cleanup to ensure no throttle configs are left behind.
   * Attempts all brokers even if some fail, then reports failures.
   */
  void clearAllThrottles() throws ExecutionException, InterruptedException, TimeoutException {
    if (throttlingEnabled() && !_throttledBrokers.isEmpty()) {
      LOG.info("Final cleanup: removing intra-broker throttles from all participating brokers: {}", _throttledBrokers);
      List<Integer> failedBrokers = new java.util.ArrayList<>();
      Exception firstException = null;
      for (int broker : _throttledBrokers) {
        try {
          removeThrottledRateFromBroker(broker);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
          LOG.warn("Failed to remove intra-broker throttle from broker {}", broker, e);
          failedBrokers.add(broker);
          if (firstException == null) {
            firstException = e;
          }
        }
      }
      _throttledBrokers.clear();
      if (firstException != null) {
        LOG.error("Failed to remove intra-broker throttles from brokers: {}", failedBrokers);
        if (firstException instanceof ExecutionException) {
          throw (ExecutionException) firstException;
        } else if (firstException instanceof InterruptedException) {
          throw (InterruptedException) firstException;
        } else {
          throw (TimeoutException) firstException;
        }
      }
    }
  }

  private boolean throttlingEnabled() {
    return _throttleRate != null;
  }

  private boolean shouldRemoveThrottleForTask(ExecutionTask task) {
    return task.state() != ExecutionTaskState.IN_PROGRESS
        && task.state() != ExecutionTaskState.PENDING
        && task.type() == ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION;
  }

  private boolean taskIsInProgress(ExecutionTask task) {
    return task.state() == ExecutionTaskState.IN_PROGRESS
        && task.type() == ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION;
  }

  private Set<Integer> getParticipatingBrokers(List<ExecutionTask> tasks) {
    Set<Integer> participatingBrokers = new TreeSet<>();
    for (ExecutionTask task : tasks) {
      participatingBrokers.add(task.brokerId());
    }
    return participatingBrokers;
  }

  private void setThrottledRateIfNecessary(int brokerId) throws ExecutionException, InterruptedException, TimeoutException {
    if (_throttleRate == null) {
      throw new IllegalStateException("Throttle rate cannot be null");
    }
    Config brokerConfigs = getBrokerConfigs(brokerId);
    ConfigEntry currThrottleRate = brokerConfigs.get(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG);
    // Record the original value only the first time we touch this broker.
    // Add to _throttledBrokers immediately after recording, so that if changeBrokerConfigs succeeds
    // but waitForConfigs throws, the broker is still tracked for cleanup.
    if (!_originalThrottleValues.containsKey(brokerId)) {
      if (currThrottleRate != null
          && currThrottleRate.source() == ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG) {
        _originalThrottleValues.put(brokerId, currThrottleRate.value());
        LOG.debug("Recorded pre-existing dynamic broker throttle for broker {}: {}", brokerId, currThrottleRate.value());
      } else {
        _originalThrottleValues.put(brokerId, null);
      }
      _throttledBrokers.add(brokerId);
    }
    if (currThrottleRate == null || !currThrottleRate.value().equals(String.valueOf(_throttleRate))) {
      LOG.debug("Setting {} to {} bytes/second for broker {}", REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
          _throttleRate, brokerId);
      List<AlterConfigOp> ops = Collections.singletonList(
          new AlterConfigOp(new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
              String.valueOf(_throttleRate)), AlterConfigOp.OpType.SET));
      changeBrokerConfigs(brokerId, ops);
    }
  }

  private void removeThrottledRateFromBroker(int brokerId)
      throws ExecutionException, InterruptedException, TimeoutException {
    Config brokerConfigs = getBrokerConfigs(brokerId);
    ConfigEntry currThrottle = brokerConfigs.get(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG);
    if (currThrottle == null) {
      return;
    }
    if (currThrottle.source() == ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG) {
      LOG.debug("Skipping removal for static intra-broker throttle rate: {} on broker {}", currThrottle, brokerId);
      return;
    }
    String originalValue = _originalThrottleValues.get(brokerId);
    if (originalValue != null) {
      // Restore the pre-existing operator-configured throttle value
      LOG.debug("Restoring pre-existing intra-broker throttle rate {} on broker {}", originalValue, brokerId);
      List<AlterConfigOp> ops = Collections.singletonList(
          new AlterConfigOp(new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, originalValue),
              AlterConfigOp.OpType.SET));
      changeBrokerConfigs(brokerId, ops);
    } else {
      LOG.debug("Removing intra-broker throttle rate: {} on broker {}", currThrottle, brokerId);
      List<AlterConfigOp> ops = Collections.singletonList(
          new AlterConfigOp(new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, null),
              AlterConfigOp.OpType.DELETE));
      changeBrokerConfigs(brokerId, ops);
    }
  }

  private Config getBrokerConfigs(int brokerId) throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    Map<ConfigResource, Config> configs = _adminClient.describeConfigs(Collections.singletonList(cf)).all()
        .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    return configs.get(cf);
  }

  void changeBrokerConfigs(int brokerId, Collection<AlterConfigOp> ops)
      throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    Map<ConfigResource, Collection<AlterConfigOp>> configs = Collections.singletonMap(cf, ops);
    _adminClient.incrementalAlterConfigs(configs).all()
        .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    waitForConfigs(cf, ops);
  }

  void waitForConfigs(ConfigResource cf, Collection<AlterConfigOp> ops) {
    Map<String, String> expectedConfigs = ops.stream()
        .collect(HashMap::new, (m, o) -> m.put(o.configEntry().name(), o.configEntry().value()), HashMap::putAll);
    boolean retryResponse = CruiseControlMetricsUtils.retry(() -> {
      try {
        Config currentConfigs = _adminClient.describeConfigs(Collections.singletonList(cf)).all()
            .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).get(cf);
        return !configsEqual(currentConfigs, expectedConfigs);
      } catch (ExecutionException | InterruptedException | TimeoutException e) {
        LOG.warn("Failed to verify config propagation for {}, will retry", cf, e);
        return true;
      }
    }, _retries);
    if (!retryResponse) {
      throw new IllegalStateException("The following configs " + ops + " were not applied to " + cf + " within the time limit");
    }
  }

  static boolean configsEqual(Config configs, Map<String, String> expectedValues) {
    for (Map.Entry<String, String> entry : expectedValues.entrySet()) {
      ConfigEntry configEntry = configs.get(entry.getKey());
      if (configEntry == null || configEntry.value() == null || configEntry.value().isEmpty()) {
        if (entry.getValue() != null) {
          return false;
        }
      } else if (entry.getValue() == null && configEntry.source() != ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG) {
        LOG.debug("Config {} has non-broker-specific source {}, treating DELETE as successful", entry.getKey(), configEntry.source());
      } else if (!Objects.equals(entry.getValue(), configEntry.value())) {
        return false;
      }
    }
    return true;
  }
}
