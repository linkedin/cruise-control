/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector;

/**
 * The interface for an anomaly type.
 */
public interface AnomalyType {

  /**
   * Get the priority of the anomaly type.
   *
   * @return The priority value.
   */
  int priority();
}
