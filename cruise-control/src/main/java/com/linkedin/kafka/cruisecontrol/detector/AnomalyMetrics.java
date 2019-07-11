/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import java.util.Map;


public class AnomalyMetrics {
  private final Map<AnomalyType, Double> _meanTimeBetweenAnomalies;
  private final double _meanTimeToStartFix;
  private final long _numSelfHealingStarted;
  private final long _ongoingAnomalyDurationMs;

  /**
   * <ol>
   *   <li>Mean time between anomalies: The mean time between detected anomalies, while there is no ongoing execution.</li>
   *   <li>Mean time to start fix: Mean time between the detection of earliest ongoing anomaly and start of a proposal
   *   execution. This metric covers executions started both by anomaly detector and as a result of on-demand requests.
   *   This metric excludes the time of the current ongoing unfixed/ </li>
   *   <li>Number of self healing started: Successful self-healing fixes started by anomaly detector</li>
   *   <li>Ongoing anomaly duration: The time (in milliseconds) from the start of the earliest unfixed (ongoing) anomaly
   *   to the current time for which no fix has been started. 0, if there is no ongoing anomaly.</li>
   * </ol>
   *
   * @param meanTimeBetweenAnomalies Mean time between anomalies by the corresponding type.
   * @param meanTimeToStartFix Mean time to start fix for any anomaly.
   * @param numSelfHealingStarted Number of fixes started by the anomaly detector as a result of self healing.
   * @param ongoingAnomalyDurationMs The duration of the ongoing (unfixed/unfixable) anomaly if there is any, 0 otherwise.
   */
  public AnomalyMetrics(Map<AnomalyType, Double> meanTimeBetweenAnomalies,
                        double meanTimeToStartFix,
                        long numSelfHealingStarted,
                        long ongoingAnomalyDurationMs) {
    if (meanTimeBetweenAnomalies == null) {
      throw new IllegalArgumentException("Attempt to set meanTimeBetweenAnomalies with null.");
    }
    _meanTimeBetweenAnomalies = meanTimeBetweenAnomalies;
    _meanTimeToStartFix = meanTimeToStartFix;
    _numSelfHealingStarted = numSelfHealingStarted;
    _ongoingAnomalyDurationMs = ongoingAnomalyDurationMs;
  }

  public double meanTimeToStartFix() {
    return _meanTimeToStartFix;
  }

  public Map<AnomalyType, Double> meanTimeBetweenAnomalies() {
    return _meanTimeBetweenAnomalies;
  }

  public long numSelfHealingStarted() {
    return _numSelfHealingStarted;
  }

  public long ongoingAnomalyDurationMs() {
    return _ongoingAnomalyDurationMs;
  }
}
