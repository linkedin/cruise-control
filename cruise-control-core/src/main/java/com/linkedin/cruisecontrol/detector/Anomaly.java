/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector;

/**
 * The interface for an anomaly.
 */
public interface Anomaly<T, AnomalyException extends Exception> {

  /**
   * Fix the anomaly with the system.
   *
   * @param instance the system instance for which the anomaly is intended to be fixed.
   */
  public void fix(T instance) throws AnomalyException;
}
