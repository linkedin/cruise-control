/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.metricdef.AggregationFunction;
import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;


public final class CruiseControlUnitTestUtils {
  public static final String METRIC1 = "m1";
  public static final String METRIC2 = "m2";
  public static final String METRIC3 = "m3";
  public static final String CPU_USAGE = "CPU_USAGE";

  private CruiseControlUnitTestUtils() {

  }

  /**
   * Populate sample aggregator with the given parameters.
   * @param numWindows The number of windows.
   * @param numSamplesPerWindow The number of samples per window.
   * @param metricSampleAggregator Metric sample aggregator.
   * @param entity Entity for which metric samples will be generated.
   * @param startingWindow Staring window.
   * @param windowMs Window size in milliseconds.
   * @param metricDef The definition of metrics.
   * @param <G> The aggregation entity group class.
   * @param <E> The entity class.
   */
  public static <G, E extends Entity<G>> void populateSampleAggregator(int numWindows,
                                                                       int numSamplesPerWindow,
                                                                       MetricSampleAggregator<G, E> metricSampleAggregator,
                                                                       E entity,
                                                                       int startingWindow,
                                                                       long windowMs,
                                                                       MetricDef metricDef) {
    for (int i = startingWindow; i < numWindows + startingWindow; i++) {
      for (int j = 0; j < numSamplesPerWindow; j++) {
        MetricSample<G, E> sample = new MetricSample<>(entity);
        for (MetricInfo metricInfo : metricDef.all()) {
          double sampleValue = i * 10 + j;
          if (metricInfo.name().equals(CPU_USAGE)) {
            sampleValue /= 100.0;
          }
          sample.record(metricInfo, sampleValue);
        }
        sample.close(i * windowMs + 1);
        metricSampleAggregator.addSample(sample);
      }
    }
  }

  public static MetricDef getMetricDef() {
    return new MetricDef().define(METRIC1, null, AggregationFunction.AVG.name())
                          .define(METRIC2, null, AggregationFunction.MAX.name())
                          .define(METRIC3, null, AggregationFunction.LATEST.name());
  }
}
