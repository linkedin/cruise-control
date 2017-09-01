/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.task;

import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.SampleStore;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The task responsible for metric sampling. This task runs periodically.
 */
class SamplingTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(SamplingTask.class);
  private final long _samplingIntervalMs;
  private final Time _time;
  private final MetadataClient _metadataClient;
  private final LoadMonitorTaskRunner _loadMonitorTaskRunner;
  private final MetricFetcherManager _metricFetcherManager;
  private final SampleStore _sampleStore;
  private long _lastSamplingPeriodEndTimeMs;

  SamplingTask(long samplingIntervalMs,
               MetadataClient metadataClient,
               LoadMonitorTaskRunner loadMonitorTaskRunner,
               MetricFetcherManager metricFetcherManager,
               SampleStore sampleStore,
               Time time) {
    _samplingIntervalMs = samplingIntervalMs;
    _time = time;
    _metadataClient = metadataClient;
    _loadMonitorTaskRunner = loadMonitorTaskRunner;
    _metricFetcherManager = metricFetcherManager;
    _sampleStore = sampleStore;
    _lastSamplingPeriodEndTimeMs = _time.milliseconds() - _samplingIntervalMs;
  }

  public void run() {
    long now = _time.milliseconds();
    if (_loadMonitorTaskRunner.compareAndSetState(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING,
                                                  LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.SAMPLING)) {
      long samplingPeriodEndMs = now;
      try {
        boolean hasSamplingError;
        long deadline = _time.milliseconds() + _samplingIntervalMs;
        do {
          _metadataClient.refreshMetadata();
          samplingPeriodEndMs = _time.milliseconds();

          hasSamplingError = _metricFetcherManager.fetchPartitionMetricSamples(_lastSamplingPeriodEndTimeMs,
                                                                               samplingPeriodEndMs,
                                                                               deadline - now,
                                                                               _sampleStore);

          if (!hasSamplingError) {
            _lastSamplingPeriodEndTimeMs = samplingPeriodEndMs;
          }
          now = _time.milliseconds();
          if (now > deadline) {
            throw new TimeoutException();
          }
        } while (hasSamplingError);
      } catch (TimeoutException e) {
        LOG.warn("Sampling did not finish in {} ms, skipping this sampling interval.", _samplingIntervalMs);
        // Advance the last sampling period end time.
        _lastSamplingPeriodEndTimeMs = samplingPeriodEndMs;
      } catch (Throwable t) {
        LOG.error("Uncaught exception in sampling", t);
        throw t;
      } finally {
        _loadMonitorTaskRunner.compareAndSetState(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.SAMPLING,
                                                  LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING);
      }
    } else {
      LOG.info("Skip sampling because the load monitor is in {} state.", _loadMonitorTaskRunner.state());
      // Something else is in progress, we advance the end time to avoid a big metric fetch after bootstrap finishes.
      // Otherwise we may see some memory issue.
      _lastSamplingPeriodEndTimeMs = now - _samplingIntervalMs;
    }
  }
}
