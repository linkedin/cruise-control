/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getAnomalyType;


/**
 * The interface for a Kafka anomaly.
 */
abstract class KafkaAnomaly implements Anomaly {
  protected OptimizationResult _optimizationResult;

  /**
   * Get the optimization result of self healing process, or null if no optimization result is available.
   *
   * @param isJson True for JSON response, false otherwise.
   * @return The optimization result of self healing process, or null if no optimization result is available.
   */
  String optimizationResult(boolean isJson) {
    if (_optimizationResult == null) {
      return null;
    }
    return isJson ? _optimizationResult.cachedJSONResponse() : _optimizationResult.cachedPlaintextResponse();
  }

  @Override
  public String toString() {
    return String.format("%s anomaly with id: %s", getAnomalyType(this), anomalyId());
  }
}
