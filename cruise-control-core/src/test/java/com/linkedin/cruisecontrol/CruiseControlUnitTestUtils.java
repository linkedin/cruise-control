/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol;

import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;
import com.linkedin.cruisecontrol.resource.Resource;


public class CruiseControlUnitTestUtils {

  private CruiseControlUnitTestUtils() {

  }

  public static <E> void populateSampleAggregator(int numSnapshots, 
                                                  int numSamplesPerSnapshot, 
                                                  MetricSampleAggregator<E> metricSampleAggregator, 
                                                  E entity, 
                                                  int startingSnapshotWindow, 
                                                  long snapshotWindowMs) {
    for (int i = startingSnapshotWindow; i < numSnapshots + startingSnapshotWindow; i++) {
      for (int j = 0; j < numSamplesPerSnapshot; j++) {
        MetricSample<E> sample = new MetricSample(entity);
        sample.record(Resource.DISK, i * 10 + j);
        sample.record(Resource.CPU, i * 10 + j);
        sample.record(Resource.NW_IN, i * 10 + j);
        sample.record(Resource.NW_OUT, i * 10 + j);
        sample.close(i * snapshotWindowMs + 1);
        metricSampleAggregator.addSample(sample);
      }
    }
  }
}
