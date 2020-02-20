/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;


/**
 * The interface for a Kafka anomaly.
 */
public abstract class KafkaAnomaly implements Anomaly, CruiseControlConfigurable {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaAnomaly.class);
  protected OptimizationResult _optimizationResult;
  protected long _detectionTimeMs;
  protected UUID _anomalyId;

  @Override
  public String optimizationResult(boolean isJson) {
    if (_optimizationResult == null) {
      return null;
    }
    return isJson ? _optimizationResult.cachedJSONResponse() : _optimizationResult.cachedPlaintextResponse();
  }

  /**
   * Checks whether the optimization result has any proposals to fix.
   *
   * @return True if {@link #_optimizationResult} has proposals to fix, false otherwise.
   */
  protected boolean hasProposalsToFix() {
    if (_optimizationResult == null) {
      throw new IllegalArgumentException("Attempt to check proposals before generating or after discarding them.");
    }
    boolean hasProposalsToFix = !_optimizationResult.optimizerResult().goalProposals().isEmpty();
    if (!hasProposalsToFix) {
      LOG.info("Skip fixing the anomaly due to inability to optimize combined self-healing goals ({})."
               + " Consider expanding the cluster or relaxing the combined goal requirements.",
               _optimizationResult.optimizerResult().statsByGoalName().keySet());
    }

    return hasProposalsToFix;
  }

  /**
   * @return A reason supplier that enables lazy evaluation.
   */
  public abstract Supplier<String> reasonSupplier();

  @Override
  public long detectionTimeMs() {
    return _detectionTimeMs;
  }

  @Override
  public String toString() {
    return String.format("%s anomaly with id: %s", anomalyType(), anomalyId());
  }

  @Override
  public String anomalyId() {
    return _anomalyId.toString();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    _anomalyId = UUID.randomUUID();
    Long detectionTimeMs = (Long) configs.get(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG);
    if (detectionTimeMs == null) {
      throw new IllegalArgumentException(String.format("Missing %s when creating anomaly of type %s.",
                                                       ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, anomalyType()));
    } else {
      _detectionTimeMs = detectionTimeMs;
    }
  }
}
