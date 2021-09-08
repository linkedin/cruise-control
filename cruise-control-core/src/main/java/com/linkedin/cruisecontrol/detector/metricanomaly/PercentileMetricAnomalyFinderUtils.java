/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.cruisecontrol.detector.metricanomaly;

/**
 * A util class for metric anomaly finders.
 */
public final class PercentileMetricAnomalyFinderUtils {

  // Ensure that no metric anomaly is generated unless the upper percentile metric value is a significant metric value.
  public static final double SIGNIFICANT_METRIC_VALUE_THRESHOLD = 1;

  private PercentileMetricAnomalyFinderUtils() {

  }

  /**
   * Check whether there are enough samples to calculate requested percentile accurately.
   * If the percentile is larger than 50.0, check that there is at least {@code 100.0 / (100.0 - percentile)} samples;
   * otherwise check there is at least {@code 100.0 / percentile} samples.
   *
   * @param sampleCount Number of samples.
   * @param upperPercentile The requested upper percentile.
   * @param lowerPercentile The requested lower percentile.
   * @return {@code true} if there is enough samples, {@code false} otherwise.
   */
  public static boolean isDataSufficient(int sampleCount,
                                         double upperPercentile,
                                         double lowerPercentile) {
    int minNumValues = (int) Math.ceil(100 / (upperPercentile > 50.0 ? (100 - upperPercentile) : upperPercentile));
    minNumValues = Math.max(minNumValues, (int) Math.ceil(100 / (lowerPercentile > 50.0 ? (100 - lowerPercentile) : lowerPercentile)));

    return sampleCount >= minNumValues;
  }
}
