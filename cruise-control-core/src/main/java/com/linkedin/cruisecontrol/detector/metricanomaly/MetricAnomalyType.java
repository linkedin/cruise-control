/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector.metricanomaly;

import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * Flags to indicate if a {@link MetricAnomalyFinder} identified brokers as anomaly suspects, recent anomalies, or persistent anomalies.
 * A {@link MetricAnomalyFinder#metricAnomalies(Map, Map)} may report all or a selected subset of metric anomaly types
 * (e.g. {@link #RECENT} and {@link #PERSISTENT}, but not {@link #SUSPECT}).
 *
 * <ul>
 *   <li>{@link #SUSPECT}: The broker is a metric anomaly suspect, but there is not yet enough evidence to conclude either way.</li>
 *   <li>{@link #RECENT}: The broker has recently been identified with a metric anomaly.</li>
 *   <li>{@link #PERSISTENT}: The broker continues to be identified with a metric anomaly for a prolonged period.</li>
 * </ul>
 */
public enum MetricAnomalyType {
  SUSPECT, RECENT, PERSISTENT;

  private static final List<MetricAnomalyType> CACHED_VALUES = List.of(values());

  /**
   * Use this instead of values() because values() creates a new array each time.
   * @return enumerated values in the same order as values()
   */
  public static List<MetricAnomalyType> cachedValues() {
    return Collections.unmodifiableList(CACHED_VALUES);
  }
}
