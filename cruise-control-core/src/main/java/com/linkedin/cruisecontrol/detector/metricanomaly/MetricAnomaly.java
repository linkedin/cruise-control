/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector.metricanomaly;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.model.Entity;
import java.util.List;


public interface MetricAnomaly<E extends Entity> extends Anomaly {
  /**
   * @return A list of windows for which a metric anomaly was observed.
   */
  List<Long> windows();

  /**
   * @return The metric anomaly description.
   */
  String description();

  /**
   * @return The entity with metric anomaly.
   */
  E entity();

  /**
   * @return The metric Id caused the metric anomaly.
   */
  Short metricId();
}
