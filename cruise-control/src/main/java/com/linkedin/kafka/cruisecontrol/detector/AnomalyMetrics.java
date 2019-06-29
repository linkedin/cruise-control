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

  /**
   * <ol>
   *   <li>Mean time between anomalies: The mean time between detected anomalies, while there is no ongoing execution.</li>
   *   <li>Mean time to start fix: Mean time between the detection of earliest ongoing anomaly and start of a proposal
   *   execution. This metric covers executions started both by anomaly detector and as a result of on-demand requests.</li>
   *   <li>Number of self healing started: Successful self-healing fixes started by anomaly detector</li>
   * </ol>
   *
   * @param meanTimeBetweenAnomalies Mean time between anomalies by the corresponding type.
   * @param meanTimeToStartFix Mean time to start fix for any anomaly.
   * @param numSelfHealingStarted Number of fixes started by the anomaly detector as a result of self healing.
   */
  public AnomalyMetrics(Map<AnomalyType, Double> meanTimeBetweenAnomalies, double meanTimeToStartFix, long numSelfHealingStarted) {
    if (meanTimeBetweenAnomalies == null) {
      throw new IllegalArgumentException("Attempt to set meanTimeBetweenAnomalies with null.");
    }
    _meanTimeBetweenAnomalies = meanTimeBetweenAnomalies;
    _meanTimeToStartFix = meanTimeToStartFix;
    _numSelfHealingStarted = numSelfHealingStarted;
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
}
