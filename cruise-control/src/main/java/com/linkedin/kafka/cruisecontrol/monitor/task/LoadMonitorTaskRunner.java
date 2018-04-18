/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.task;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.SampleStore;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaBrokerMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.*;


/**
 * A class that is responsible for running all the LoadMonitor tasks.
 */
public class LoadMonitorTaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(LoadMonitorTaskRunner.class);

  private final Time _time;
  private final MetricFetcherManager _metricFetcherManager;
  private final KafkaPartitionMetricSampleAggregator _partitionMetricSampleAggregator;
  private final KafkaBrokerMetricSampleAggregator _brokerMetricSampleAggregator;
  private final MetadataClient _metadataClient;
  private final SampleStore _sampleStore;
  private final ScheduledExecutorService _samplingScheduler;
  private final long _samplingIntervalMs;
  // The following two configuration is actually for MetricSampleAggregator, the MetricFetcherManager uses it to
  // check if a bootstrap is done or not.
  private final int _configuredNumWindows;
  private final long _configuredWindowMs;

  private AtomicReference<LoadMonitorTaskRunnerState> _state;
  private volatile double _bootstrapProgress;

  public enum LoadMonitorTaskRunnerState {
    NOT_STARTED, RUNNING, PAUSED, SAMPLING, BOOTSTRAPPING, TRAINING, LOADING
  }

  /**
   * Private constructor to avoid duplicate code.
   *
   * @param config The load monitor configurations.
   * @param partitionMetricSampleAggregator The {@link KafkaPartitionMetricSampleAggregator} to aggregate partition metrics.
   * @param brokerMetricSampleAggregator The {@link KafkaBrokerMetricSampleAggregator} to aggregate broker metrics.
   * @param metadataClient The metadata of the cluster.
   * @param metricDef The metric definitions.
   * @param time The time object.
   * @param dropwizardMetricRegistry The metric registry that holds all the metrics for monitoring Cruise Control.
   */
  public LoadMonitorTaskRunner(KafkaCruiseControlConfig config,
                               KafkaPartitionMetricSampleAggregator partitionMetricSampleAggregator,
                               KafkaBrokerMetricSampleAggregator brokerMetricSampleAggregator,
                               MetadataClient metadataClient,
                               MetricDef metricDef,
                               Time time,
                               MetricRegistry dropwizardMetricRegistry) {
    this(config,
        new MetricFetcherManager(config, partitionMetricSampleAggregator, brokerMetricSampleAggregator, metadataClient,
                                 metricDef, time, dropwizardMetricRegistry),
        partitionMetricSampleAggregator,
        brokerMetricSampleAggregator,
        metadataClient,
        time);
  }

  /**
   * Package private constructor for unit test duplicate code.
   *
   * @param config The load monitor configurations.
   * @param metricFetcherManager the metric fetcher manager.
   * @param partitionMetricSampleAggregator The {@link KafkaPartitionMetricSampleAggregator} to aggregate partition metrics.
   * @param brokerMetricSampleAggregator The {@link KafkaBrokerMetricSampleAggregator} to aggregate broker metrics.
   * @param metadataClient      The metadata of the cluster.
   * @param time          The time object.
   */
  LoadMonitorTaskRunner(KafkaCruiseControlConfig config,
                        MetricFetcherManager metricFetcherManager,
                        KafkaPartitionMetricSampleAggregator partitionMetricSampleAggregator,
                        KafkaBrokerMetricSampleAggregator brokerMetricSampleAggregator,
                        MetadataClient metadataClient,
                        Time time) {
    _time = time;
    _metricFetcherManager = metricFetcherManager;
    _partitionMetricSampleAggregator = partitionMetricSampleAggregator;
    _brokerMetricSampleAggregator = brokerMetricSampleAggregator;
    _metadataClient = metadataClient;
    _sampleStore = config.getConfiguredInstance(KafkaCruiseControlConfig.SAMPLE_STORE_CLASS_CONFIG, SampleStore.class);
    long samplingIntervalMs = config.getLong(KafkaCruiseControlConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG);

    _samplingScheduler =
        Executors.newScheduledThreadPool(2, new KafkaCruiseControlThreadFactory("SamplingScheduler", true, LOG));
    _samplingIntervalMs = samplingIntervalMs;
    _configuredNumWindows = config.getInt(KafkaCruiseControlConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG);
    _configuredWindowMs = config.getLong(KafkaCruiseControlConfig.PARTITION_METRICS_WINDOW_MS_CONFIG);

    _state = new AtomicReference<>(NOT_STARTED);
    _bootstrapProgress = -1.0;
  }

  /**
   * Bootstrap to load the workload snapshot from the stored MetricSamples from external source.
   * This function does not refresh metadata and does not retry.
   *
   * @param startMs the starting time of the period to bootstrap.
   * @param endMs the end time of the period to bootstrap.
   * @param clearMetrics clear the existing metric samples.
   */
  public void bootstrap(long startMs, long endMs, boolean clearMetrics) {

    if (_state.compareAndSet(RUNNING, BOOTSTRAPPING)) {
      _samplingScheduler.submit(new BootstrapTask(startMs, endMs, clearMetrics, _metadataClient,
          _partitionMetricSampleAggregator,
                                                  this, _metricFetcherManager, _sampleStore, _configuredNumWindows,
                                                  _configuredWindowMs, _samplingIntervalMs, _time));
    } else {
      throw new IllegalStateException("Cannot bootstrap because the load monitor in " + _state.get() + " state.");
    }
  }

  /**
   * Bootstrap to load the workload snapshot from the stored MetricSamples from external source.
   * This function does not refresh metadata and does not retry.
   *
   * @param startMs the starting time of the period to bootstrap.
   * @param clearMetrics clear the existing metric samples.
   */
  public void bootstrap(long startMs, boolean clearMetrics) {

    if (_state.compareAndSet(RUNNING, BOOTSTRAPPING)) {
      _samplingScheduler.submit(new BootstrapTask(startMs, clearMetrics, _metadataClient,
          _partitionMetricSampleAggregator,
                                                  this, _metricFetcherManager, _sampleStore, _configuredNumWindows,
                                                  _configuredWindowMs, _samplingIntervalMs, _time));
    } else {
      throw new IllegalStateException("Cannot bootstrap because the load monitor in " + _state.get() + " state.");
    }
  }

  /**
   * Bootstrap to load the workload snapshot from the stored MetricSamples from external source.
   * This function does not refresh metadata and does not retry.
   *
   * @param clearMetrics clear the existing metric samples.
   */
  public void bootstrap(boolean clearMetrics) {
    if (_state.compareAndSet(RUNNING, BOOTSTRAPPING)) {
      _samplingScheduler.submit(new BootstrapTask(clearMetrics, _metadataClient, _partitionMetricSampleAggregator,
                                                  this, _metricFetcherManager, _sampleStore, _configuredNumWindows,
                                                  _configuredWindowMs, _samplingIntervalMs, _time));
    } else {
      throw new IllegalStateException("Cannot bootstrap because the load monitor is in " + _state.get() + " state.");
    }
  }

  /**
   * Get the bootstrap progress.
   */
  public double bootStrapProgress() {
    return _bootstrapProgress;
  }

  /**
   * Load the samples from sample store. This task has to be executed before the load monitor actually starts.
   */
  private void loadSamples() {
    if (_state.compareAndSet(RUNNING, LOADING)) {
      _samplingScheduler.submit(new SampleLoadingTask(_sampleStore,
                                                      _partitionMetricSampleAggregator,
                                                      _brokerMetricSampleAggregator,
                                                      this));
    } else {
      throw new IllegalStateException("Cannot load samples because the load monitor is in "
                                          + _state.get() + " state.");
    }
  }

  public double sampleLoadingProgress() {
    return _sampleStore.sampleLoadingProgress();
  }

  /**
   * Train the cluster model
   * @param startMs the starting time of the training period.
   * @param endMs the end time of the training period.
   */
  public void train(long startMs, long endMs) {
    if (_state.compareAndSet(RUNNING, TRAINING)) {
      _samplingScheduler.submit(new TrainingTask(_time, this, _metricFetcherManager, _sampleStore, _configuredWindowMs, _samplingIntervalMs, startMs, endMs));
    } else {
      throw new IllegalStateException("Cannot start model training because the load monitor is in "
                                          + _state.get() + " state.");
    }
  }

  /**
   * Get the state of the task runner.
   */
  public LoadMonitorTaskRunnerState state() {
    return _state.get();
  }

  /**
   * Start the metric fetchers and sampling scheduler thread.
   */
  public void start() {
    start(false);
  }

  /**
   * Start the metric fetchers and sampling scheduler thread. Package private for testing.
   * @param skipLoadingSamples whether skip the sampling loading task or not.
   */
  void start(boolean skipLoadingSamples) {
    if (!_state.compareAndSet(NOT_STARTED, RUNNING)) {
      throw new IllegalStateException("Cannot start the task runner because the load monitor is in "
                                          + _state.get() + " state.");
    }
    if (!skipLoadingSamples) {
      loadSamples();
    }
    _samplingScheduler.scheduleAtFixedRate(new SamplingTask(_samplingIntervalMs, _metadataClient,
                                                            this, _metricFetcherManager, _sampleStore, _time),
                                           0L,
                                           _samplingIntervalMs,
                                           TimeUnit.MILLISECONDS);
  }

  /**
   * Shutdown the task runner.
   */
  public void shutdown() {
    LOG.info("Shutting down load monitor task runner.");
    _samplingScheduler.shutdown();

    try {
      _samplingScheduler.awaitTermination(1000, TimeUnit.MILLISECONDS);
      if (!_samplingScheduler.isTerminated()) {
        LOG.warn("The sampling scheduler failed to shutdown in " + _samplingIntervalMs + " ms.");
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for metric fetcher manager to shutdown.");
    }
    _metricFetcherManager.shutdown();
    _sampleStore.close();
    LOG.info("Load monitor task runner shutdown completed.");
  }

  /**
   * Pause the scheduled sampling tasks..
   */
  public void pauseSampling() {
    if (_state.get() != PAUSED && !_state.compareAndSet(RUNNING, PAUSED)) {
      throw new IllegalStateException("Cannot pause the load monitor because it is in " + _state.get() + " state.");
    }
  }

  /**
   * Resume the scheduled sampling tasks.
   */
  public void resumeSampling() {
    if (_state.get() != RUNNING && !_state.compareAndSet(PAUSED, RUNNING)) {
      throw new IllegalStateException("Cannot resume the load monitor because it is in " + _state.get() + " state");
    }
  }


  boolean compareAndSetState(LoadMonitorTaskRunnerState expectedState, LoadMonitorTaskRunnerState newState) {
    return _state.compareAndSet(expectedState, newState);
  }

  void setState(LoadMonitorTaskRunnerState newState) {
    _state.set(newState);
  }

  void setBootstrapProgress(double progress) {
    _bootstrapProgress = progress;
  }
}
