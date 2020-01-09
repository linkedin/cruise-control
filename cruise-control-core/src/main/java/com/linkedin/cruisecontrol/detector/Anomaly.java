/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector;

/**
 * The interface for an anomaly.
 */
public interface Anomaly {

  /**
   * @return A unique identifier for the anomaly.
   */
  String anomalyId();

  /**
   * Get the type of anomaly.
   *
   * @return The type of anomaly.
   */
  AnomalyType anomalyType();

  /**
   * Get the detection time of anomaly.
   *
   * @return The detection time of anomaly.
   */
  long detectionTimeMs();

  /**
   * Fix the anomaly with the system.
   *
   * @return True if fix was started successfully (i.e. there is actual work towards a fix), false otherwise.
   */
  boolean fix() throws Exception;

  /**
   * Get the optimization result of self healing process, or null if no optimization result is available.
   *
   * @param isJson True for JSON response, false otherwise.
   * @return The optimization result of self healing process, or null if no optimization result is available.
   */
  String optimizationResult(boolean isJson);
}
