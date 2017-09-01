/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.task;

import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.SampleStore;


public class SampleLoadingTask implements Runnable {
  private final SampleStore _sampleStore;
  private final MetricSampleAggregator _metricSampleAggregator;
  private final LoadMonitorTaskRunner _loadMonitorTaskRunner;

  SampleLoadingTask(SampleStore sampleStore,
                    MetricSampleAggregator metricSampleAggregator,
                    LoadMonitorTaskRunner loadMonitorTaskRunner) {
    _sampleStore = sampleStore;
    _metricSampleAggregator = metricSampleAggregator;
    _loadMonitorTaskRunner = loadMonitorTaskRunner;
  }

  @Override
  public void run() {
    try {
      _sampleStore.loadSamples(new SampleStore.SampleLoader(_metricSampleAggregator));
      ModelParameters.updateModelCoefficient();
      _metricSampleAggregator.refreshCompletenessCache();
    } finally {
      // The sample loading task is run before the load monitor starts.
      _loadMonitorTaskRunner.setState(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING);
    }
  }
}
