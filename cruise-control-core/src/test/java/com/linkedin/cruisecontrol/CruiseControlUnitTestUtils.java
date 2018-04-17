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


public class CruiseControlUnitTestUtils {
  public static final String METRIC1 = "m1";
  public static final String METRIC2 = "m2";
  public static final String METRIC3 = "m3";

  private CruiseControlUnitTestUtils() {

  }

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
          sample.record(metricInfo, i * 10 + j);
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
