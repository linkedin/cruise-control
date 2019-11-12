/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.task;

import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.SampleStore;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaBrokerMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State.NO_TASK_IN_PROGRESS;


public class SampleLoadingTask implements Runnable {
  private final SampleStore _sampleStore;
  private final KafkaPartitionMetricSampleAggregator _partitionMetricSampleAggregator;
  private final KafkaBrokerMetricSampleAggregator _brokerMetricSampleAggregator;
  private final LoadMonitorTaskRunner _loadMonitorTaskRunner;
  private final Executor _executor;

  SampleLoadingTask(SampleStore sampleStore,
                    KafkaPartitionMetricSampleAggregator partitionMetricSampleAggregator,
                    KafkaBrokerMetricSampleAggregator brokerMetricSampleAggregator,
                    LoadMonitorTaskRunner loadMonitorTaskRunner,
                    Executor executor) {
    _sampleStore = sampleStore;
    _partitionMetricSampleAggregator = partitionMetricSampleAggregator;
    _brokerMetricSampleAggregator = brokerMetricSampleAggregator;
    _loadMonitorTaskRunner = loadMonitorTaskRunner;
    if (executor == null) {
      throw new IllegalArgumentException("Executor is not provided.");
    }
    _executor = executor;
  }

  @Override
  public void run() {
    try {
      _sampleStore.loadSamples(new SampleStore.SampleLoader(_partitionMetricSampleAggregator,
                                                            _brokerMetricSampleAggregator));
      ModelParameters.updateModelCoefficient();
    } finally {
      // If Cruise Control has triggered operations before loading is finished, transfer monitor state to paused state;
      // otherwise transfer to running state.
      synchronized (_loadMonitorTaskRunner) {
        if (_executor.state().state() != NO_TASK_IN_PROGRESS) {
          _loadMonitorTaskRunner.compareAndSetState(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING,
                                                    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.PAUSED);
          _loadMonitorTaskRunner.setReasonOfLatestPauseOrResume("Paused-By-Cruise-Control-Before-Starting-Execution");
        } else {
          _loadMonitorTaskRunner.compareAndSetState(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING,
                                                    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING);
        }
      }
    }
  }
}
