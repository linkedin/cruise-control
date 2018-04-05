/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector;

import com.linkedin.cruisecontrol.model.Entity;


public interface MetricAnomaly<M extends Entity, T, AnomalyException extends Exception> extends Anomaly<T, AnomalyException> {
  /**
   * Get the start time of the metric anomaly observation.
   */
  public long startTime();

  /**
   * Get the end time of the metric anomaly observation.
   */
  public long endTime();

  /**
   * Get the metric anomaly description.
   */
  public String description();

  /**
   * Get the entity with metric anomaly.
   */
  public M entity();

  /**
   * Get the metric Id caused the metric anomaly.
   */
  public Integer metricId();
}
