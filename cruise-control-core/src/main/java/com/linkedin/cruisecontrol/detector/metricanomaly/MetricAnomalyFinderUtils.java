/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.cruisecontrol.detector.metricanomaly;

/**
 * A util class for metric anomaly finders.
 */
public class MetricAnomalyFinderUtils {

  public static final double SIGNIFICANT_METRIC_VALUE_THRESHOLD = 1;

  private MetricAnomalyFinderUtils() {

  }

  /**
   * Check whether there are enough samples to calculate requested percentile.
   *
   * @param sampleCount Number of samples.
   * @param upperPercentile The requested upper percentile.
   * @param lowerPercentile The requested lower percentile.
   * @return True if there is enough samples, false otherwise.
   */
  public static boolean isDataSufficient(int sampleCount,
                                         double upperPercentile,
                                         double lowerPercentile) {
    if (upperPercentile >= 100.0 || upperPercentile <= 0.0 || lowerPercentile >= 100.0 || lowerPercentile <= 0.0) {
      throw new IllegalArgumentException("Invalid percentile.");
    }

    int minNumValues = (int) Math.ceil(100 / (upperPercentile > 50.0 ? (100 - upperPercentile) : upperPercentile));
    minNumValues = Math.max(minNumValues, (int) Math.ceil(100 / (lowerPercentile > 50.0 ? (100 - lowerPercentile) : lowerPercentile)));

    return sampleCount >= minNumValues;
  }
}
