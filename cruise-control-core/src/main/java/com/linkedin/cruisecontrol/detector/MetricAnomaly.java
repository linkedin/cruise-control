/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector;

import com.linkedin.cruisecontrol.model.Entity;
import java.util.List;


public interface MetricAnomaly<E extends Entity> extends Anomaly {
  /**
   * Get a list of windows for which a metric anomaly was observed.
   */
  public List<Long> windows();

  /**
   * Get the metric anomaly description.
   */
  public String description();

  /**
   * Get the entity with metric anomaly.
   */
  public E entity();

  /**
   * Get the metric Id caused the metric anomaly.
   */
  public Integer metricId();
}
