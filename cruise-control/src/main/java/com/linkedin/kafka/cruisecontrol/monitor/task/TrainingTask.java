/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.task;

import com.linkedin.kafka.cruisecontrol.model.LinearRegressionModelParameters;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.SampleStore;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A task that is responsible for train the load model.
 */
class TrainingTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TrainingTask.class);
  private final Time _time;
  private final LoadMonitorTaskRunner _loadMonitorTaskRunner;
  private final MetricFetcherManager _metricFetcherManager;
  private final SampleStore _sampleStore;
  private final long _samplingIntervalMs;
  private final long _configuredSnapshotWindowMs;
  private final long _trainingStartMs;
  private final long _trainingEndMs;
  private long _nextSamplingStartingMs;
  private long _nextSamplingEndMs;

  TrainingTask(Time time,
               LoadMonitorTaskRunner loadMonitorTaskRunner,
               MetricFetcherManager metricFetcherManager,
               SampleStore sampleStore,
               long configuredSnapshotWindowMs,
               long samplingIntervalMs,
               long trainingStartMs,
               long trainingEndMs) {
    _time = time;
    _loadMonitorTaskRunner = loadMonitorTaskRunner;
    _metricFetcherManager = metricFetcherManager;
    _sampleStore = sampleStore;
    _configuredSnapshotWindowMs = configuredSnapshotWindowMs;
    _samplingIntervalMs = samplingIntervalMs;
    _trainingStartMs = trainingStartMs;
    _trainingEndMs = trainingEndMs;
    _nextSamplingStartingMs = trainingStartMs;
    _nextSamplingEndMs = trainingStartMs + _samplingIntervalMs;
  }

  private boolean isDone() {
    return ModelParameters.updateModelCoefficient() || _nextSamplingEndMs >= _trainingEndMs;
  }

  @Override
  public void run() {
    LOG.info("Starting load model training task for time range [{}, {}]", _trainingStartMs, _trainingEndMs);
    long trainingTaskStartingMs = _time.milliseconds();
    try {
      do {
        _metricFetcherManager.fetchBrokerMetricSamples(_nextSamplingStartingMs,
                                                       _nextSamplingEndMs,
                                                       _samplingIntervalMs,
                                                       _sampleStore);
        _nextSamplingStartingMs += _configuredSnapshotWindowMs;
        _nextSamplingEndMs = Math.min(_trainingEndMs, _nextSamplingStartingMs + _configuredSnapshotWindowMs);
      } while (!isDone());
    } catch (Throwable t) {
      LOG.error("load model training task got exception.", t);
    } finally {
      _loadMonitorTaskRunner.compareAndSetState(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.TRAINING,
                                                LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING);
      LOG.info("Load monitor finished training with time range [{}, {}] in {} seconds. Coefficients: {}, {}, {}",
               _trainingStartMs, _trainingEndMs,
               (_time.milliseconds() - trainingTaskStartingMs) / 1000,
               ModelParameters.getCoefficient(LinearRegressionModelParameters.ModelCoefficient.LEADER_BYTES_IN),
               ModelParameters.getCoefficient(LinearRegressionModelParameters.ModelCoefficient.LEADER_BYTES_OUT),
               ModelParameters.getCoefficient(LinearRegressionModelParameters.ModelCoefficient.FOLLOWER_BYTES_IN));
    }
  }
}
