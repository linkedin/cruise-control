/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.task;

import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.SampleStore;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaBrokerMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;

public class SampleLoadingTask implements Runnable {
  private final SampleStore _sampleStore;
  private final KafkaPartitionMetricSampleAggregator _partitionMetricSampleAggregator;
  private final KafkaBrokerMetricSampleAggregator _brokerMetricSampleAggregator;
  private final LoadMonitorTaskRunner _loadMonitorTaskRunner;

  SampleLoadingTask(SampleStore sampleStore,
                    KafkaPartitionMetricSampleAggregator partitionMetricSampleAggregator,
                    KafkaBrokerMetricSampleAggregator brokerMetricSampleAggregator,
                    LoadMonitorTaskRunner loadMonitorTaskRunner) {
    _sampleStore = sampleStore;
    _partitionMetricSampleAggregator = partitionMetricSampleAggregator;
    _brokerMetricSampleAggregator = brokerMetricSampleAggregator;
    _loadMonitorTaskRunner = loadMonitorTaskRunner;
  }

  @Override
  public void run() {
    try {
      _sampleStore.loadSamples(new SampleStore.SampleLoader(_partitionMetricSampleAggregator,
                                                            _brokerMetricSampleAggregator));
      ModelParameters.updateModelCoefficient();
    } finally {
      // The sample loading task is run before the load monitor starts regardless of any ongoing execution.
      _loadMonitorTaskRunner.compareAndSetState(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING,
                                                LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING);
    }
  }
}
