/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import java.util.Map;
import java.util.HashMap;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.toPrettyDuration;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.GOAL_VIOLATION;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.METRIC_ANOMALY;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.BROKER_FAILURE;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.DISK_FAILURE;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseExternalFields;

@JsonResponseClass
public class AnomalyMetrics {
  @JsonResponseField
  private static final String MEAN_TIME_BETWEEN_ANOMALIES_MS = "meanTimeBetweenAnomaliesMs";
  @JsonResponseField
  private static final String MEAN_TIME_TO_START_FIX_MS = "meanTimeToStartFixMs";
  @JsonResponseField
  private static final String ONGOING_ANOMALY_DURATION_MS = "ongoingAnomalyDurationMs";
  @JsonResponseField
  private static final String NUM_SELF_HEALING_STARTED = "numSelfHealingStarted";

  private final MeanTimeBetweenAnomaliesMs _meanTimeBetweenAnomaliesMs;
  private final double _meanTimeToStartFixMs;
  private final long _numSelfHealingStarted;
  private final long _ongoingAnomalyDurationMs;

  /**
   * <ol>
   *   <li>Mean time between anomalies: The mean time between detected anomalies, while there is no ongoing execution.</li>
   *   <li>Mean time to start fix: Mean time between the detection of earliest ongoing anomaly and start of a proposal
   *   execution. This metric covers executions started both by anomaly detector and as a result of on-demand requests.
   *   This metric excludes the time of the current ongoing unfixed anomaly.</li>
   *   <li>Number of self healing started: Successful self-healing fixes started by anomaly detector.</li>
   *   <li>Ongoing anomaly duration: The time (in milliseconds) from the start of the earliest unfixed (ongoing) anomaly
   *   to the current time for which no fix has been started. 0, if there is no unfixed (ongoing) anomaly.</li>
   * </ol>
   *
   * @param meanTimeBetweenAnomaliesMs Mean time between anomalies by the corresponding type.
   * @param meanTimeToStartFixMs Mean time to start fix for any anomaly.
   * @param numSelfHealingStarted Number of fixes started by the anomaly detector as a result of self healing.
   * @param ongoingAnomalyDurationMs The duration of the ongoing (unfixed/unfixable) anomaly if there is any, 0 otherwise.
   */
  public AnomalyMetrics(Map<AnomalyType, Double> meanTimeBetweenAnomaliesMs,
                        double meanTimeToStartFixMs,
                        long numSelfHealingStarted,
                        long ongoingAnomalyDurationMs) {
    if (meanTimeBetweenAnomaliesMs == null) {
      throw new IllegalArgumentException("Attempt to set meanTimeBetweenAnomaliesMs with null.");
    }
    for (AnomalyType anomalyType : KafkaAnomalyType.cachedValues()) {
      if (!meanTimeBetweenAnomaliesMs.containsKey(anomalyType)) {
        throw new IllegalArgumentException(anomalyType + " is missing in meanTimeBetweenAnomaliesMs metric.");
      }
    }
    _meanTimeBetweenAnomaliesMs = new MeanTimeBetweenAnomaliesMs(meanTimeBetweenAnomaliesMs);
    _meanTimeToStartFixMs = meanTimeToStartFixMs;
    _numSelfHealingStarted = numSelfHealingStarted;
    _ongoingAnomalyDurationMs = ongoingAnomalyDurationMs;
  }

  /**
   * @return Mean time to start a fix.
   */
  public double meanTimeToStartFixMs() {
    return _meanTimeToStartFixMs;
  }

  /**
   * @return Mean time between anomalies.
   */
  public MeanTimeBetweenAnomaliesMs meanTimeBetweenAnomaliesMs() {
    return _meanTimeBetweenAnomaliesMs;
  }

  /**
   * @return Number of self healing operations started.
   */
  public long numSelfHealingStarted() {
    return _numSelfHealingStarted;
  }

  /**
   * @return The duration of ongoing anomaly in milliseconds.
   */
  public long ongoingAnomalyDurationMs() {
    return _ongoingAnomalyDurationMs;
  }

  /**
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> metrics = new HashMap<>(4);
    metrics.put(MEAN_TIME_BETWEEN_ANOMALIES_MS, meanTimeBetweenAnomaliesMs().getMap());
    metrics.put(MEAN_TIME_TO_START_FIX_MS, meanTimeToStartFixMs());
    metrics.put(NUM_SELF_HEALING_STARTED, numSelfHealingStarted());
    metrics.put(ONGOING_ANOMALY_DURATION_MS, ongoingAnomalyDurationMs());
    return metrics;
  }

  @Override
  public String toString() {
    return String.format("{meanTimeBetweenAnomalies:{%s:%s, %s:%s, %s:%s, %s:%s}, "
                         + "meanTimeToStartFix:%s, numSelfHealingStarted:%d, ongoingAnomalyDuration=%s}",
                         GOAL_VIOLATION, toPrettyDuration(_meanTimeBetweenAnomaliesMs.getMap().get(GOAL_VIOLATION)),
                         BROKER_FAILURE, toPrettyDuration(_meanTimeBetweenAnomaliesMs.getMap().get(BROKER_FAILURE)),
                         METRIC_ANOMALY, toPrettyDuration(_meanTimeBetweenAnomaliesMs.getMap().get(METRIC_ANOMALY)),
                         DISK_FAILURE, toPrettyDuration(_meanTimeBetweenAnomaliesMs.getMap().get(DISK_FAILURE)),
                         toPrettyDuration(_meanTimeToStartFixMs), _numSelfHealingStarted,
                         toPrettyDuration(_ongoingAnomalyDurationMs));
  }

  @JsonResponseClass
  @JsonResponseExternalFields(KafkaAnomalyType.class)
  protected static class MeanTimeBetweenAnomaliesMs {
    protected Map<AnomalyType, Double> _meanTimeBetweenAnomaliesMs;
    
    MeanTimeBetweenAnomaliesMs(Map<AnomalyType, Double> meanTimes) {
      _meanTimeBetweenAnomaliesMs = meanTimes;
    }

    protected Map<AnomalyType, Double> getMap() {
      return _meanTimeBetweenAnomaliesMs;
    }
  }
}
