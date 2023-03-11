/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.concurrency;

import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * The summary of the currency execution concurrency. It provides the current min/max/avg value of the broker concurrency of different type.
 */
public class ExecutionConcurrencySummary {
  private final Map<Integer, Integer> _interBrokerPartitionMovementConcurrency;
  private final Map<Integer, Integer> _intraBrokerPartitionMovementConcurrency;
  private final Map<Integer, Integer> _leadershipMovementConcurrency;
  private final boolean _initialized;

  public ExecutionConcurrencySummary(boolean initialized,
                                     Map<Integer, Integer> interBrokerPartitionMovementConcurrency,
                                     Map<Integer, Integer> intraBrokerPartitionMovementConcurrency,
                                     Map<Integer, Integer> leadershipMovementConcurrency) {
    _initialized = initialized;
    _interBrokerPartitionMovementConcurrency = new HashMap<>(interBrokerPartitionMovementConcurrency);
    _intraBrokerPartitionMovementConcurrency = new HashMap<>(intraBrokerPartitionMovementConcurrency);
    _leadershipMovementConcurrency = new HashMap<>(leadershipMovementConcurrency);
  }

  /**
   * Get the min broker execution concurrency of the cluster
   * @param concurrencyType the concurrency type of the min execution concurrency
   * @return the min broker execution concurrency of the cluster. If not initialized, return 0.
   */
  public synchronized int getMinExecutionConcurrency(ConcurrencyType concurrencyType) {
    if (!_initialized) {
      return 0;
    }

    sanityCheckValidity();
    switch (concurrencyType) {
      case INTER_BROKER_REPLICA:
        return Collections.min(_interBrokerPartitionMovementConcurrency.values());
      case INTRA_BROKER_REPLICA:
        return Collections.min(_intraBrokerPartitionMovementConcurrency.values());
      case LEADERSHIP:
        return Collections.min(_leadershipMovementConcurrency.values());
      default:
        throw new IllegalArgumentException("Unsupported concurrency type " + concurrencyType + " is provided.");
    }
  }

  /**
   * Get the max broker execution concurrency of the cluster
   * @param concurrencyType the concurrency type of the max execution concurrency
   * @return the max broker execution concurrency of the cluster. If not initialized, return 0.
   */
  public synchronized int getMaxExecutionConcurrency(ConcurrencyType concurrencyType) {
    if (!_initialized) {
      return 0;
    }

    sanityCheckValidity();
    switch (concurrencyType) {
      case INTER_BROKER_REPLICA:
        return Collections.max(_interBrokerPartitionMovementConcurrency.values());
      case INTRA_BROKER_REPLICA:
        return Collections.max(_intraBrokerPartitionMovementConcurrency.values());
      case LEADERSHIP:
        return Collections.max(_leadershipMovementConcurrency.values());
      default:
        throw new IllegalArgumentException("Unsupported concurrency type " + concurrencyType + " is provided.");
    }
  }

  /**
   * Get the avg broker execution concurrency of the cluster
   * @param concurrencyType the concurrency type of the avg execution concurrency
   * @return the avg broker execution concurrency of the cluster. If not initialized, return 0.
   */
  public synchronized double getAvgExecutionConcurrency(ConcurrencyType concurrencyType) {
    if (!_initialized) {
      return 0;
    }

    sanityCheckValidity();
    switch (concurrencyType) {
      case INTER_BROKER_REPLICA:
        return _interBrokerPartitionMovementConcurrency.values().stream().mapToDouble(d -> d).average()
                                                       .orElse(-1);
      case INTRA_BROKER_REPLICA:
        return _intraBrokerPartitionMovementConcurrency.values().stream().mapToDouble(d -> d).average()
                                                       .orElse(-1);
      case LEADERSHIP:
        return _leadershipMovementConcurrency.values().stream().mapToDouble(d -> d).average()
                                             .orElse(-1);
      default:
        throw new IllegalArgumentException("Unsupported concurrency type " + concurrencyType + " is provided.");
    }
  }

  private void sanityCheckValidity() {
    if (_interBrokerPartitionMovementConcurrency.isEmpty()
        || _intraBrokerPartitionMovementConcurrency.isEmpty()
        || _leadershipMovementConcurrency.isEmpty()) {
      throw new IllegalArgumentException("Broker concurrency is not populated.");
    }
  }
}

