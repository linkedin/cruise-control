/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Flags to indicate the type of an anomaly.
 *
 * <ul>
 * <li>{@link #GOAL_VIOLATION}: Violation of anomaly detection goals.</li>
 * <li>{@link #BROKER_FAILURE}: Fail-stop failure of brokers.</li>
 * <li>{@link #METRIC_ANOMALY}: Abnormal changes in broker metrics.</li>
 * <li>{@link #DISK_FAILURE}: Fail-stop failure of disks.</li>
 * </ul>
 */
public enum AnomalyType {
  GOAL_VIOLATION,
  BROKER_FAILURE,
  METRIC_ANOMALY,
  DISK_FAILURE;

  private static final List<AnomalyType> CACHED_VALUES = Collections.unmodifiableList(Arrays.asList(values()));

  /**
   * Use this instead of values() because values() creates a new array each time.
   * @return enumerated values in the same order as values()
   */
  public static List<AnomalyType> cachedValues() {
    return CACHED_VALUES;
  }
}
