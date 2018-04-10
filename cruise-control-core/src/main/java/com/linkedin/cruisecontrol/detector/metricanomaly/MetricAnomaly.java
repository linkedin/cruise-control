/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector.metricanomaly;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.model.Entity;
import java.util.List;


public interface MetricAnomaly<E extends Entity> extends Anomaly {
  /**
   * Get a list of windows for which a metric anomaly was observed.
   */
  List<Long> windows();

  /**
   * Get the metric anomaly description.
   */
  String description();

  /**
   * Get the entity with metric anomaly.
   */
  E entity();

  /**
   * Get the metric Id caused the metric anomaly.
   */
  Integer metricId();
}
