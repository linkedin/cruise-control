/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;


/**
 * The interface for a Kafka anomaly.
 */
public abstract class KafkaAnomaly implements Anomaly, CruiseControlConfigurable {
  protected OptimizationResult _optimizationResult;
  protected long _detectionTimeMs;

  @Override
  public String optimizationResult(boolean isJson) {
    if (_optimizationResult == null) {
      return null;
    }
    return isJson ? _optimizationResult.cachedJSONResponse() : _optimizationResult.cachedPlaintextResponse();
  }

  @Override
  public long detectionTimeMs() {
    return _detectionTimeMs;
  }

  @Override
  public String toString() {
    return String.format("%s anomaly with id: %s", anomalyType(), anomalyId());
  }

  @Override
  public void configure(Map<String, ?> configs) {
    Long detectionTimeMs = (Long) configs.get(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG);
    if (detectionTimeMs == null) {
      throw new IllegalArgumentException(String.format("Missing %s when creating anomaly of type %s.",
                                                       ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, anomalyType()));
    } else {
      _detectionTimeMs = detectionTimeMs;
    }
  }
}
