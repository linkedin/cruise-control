/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector.metricanomaly;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.model.Entity;
import java.util.Map;


/**
 * The interface for a metric anomaly.
 *
 * @param <E> The type of entity with metric anomaly.
 */
public interface MetricAnomaly<E extends Entity> extends Anomaly {
  /**
   * Get the metric anomaly description.
   *
   * @return  The description string.
   */
  String description();

  /**
   * Get the entities with metric anomaly detected.
   *
   * @return  A map of entities with metric anomaly detected to detection time.
   */
  Map<E, Long> entities();
}
