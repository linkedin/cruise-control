/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class manages the metric fetchers. It periodically kicks off the sampling and refreshes the metadata as well.
 */
public class MetricFetcherManager {
  private static final Logger LOG = LoggerFactory.getLogger(MetricFetcherManager.class);

  private final Time _time;
  private final MetricSampleAggregator _metricSampleAggregator;
  private final MetadataClient _metadataClient;
  private final int _numMetricFetchers;
  private final List<MetricSampler> _metricSamplers;
  private final MetricSamplerPartitionAssignor _partitionAssignor;
  private final ExecutorService _samplingExecutor;
  // The following two configuration is actually for MetricSampleAggregator, the MetricFetcherManager uses it to
  // check if a bootstrap is done or not.
  private final boolean _useLinearRegressionModel;
  private final MetricRegistry _dropwizardMetricRegistry;
  // The below two members keep track last time the sampling threads were executed
  private final Timer _partitionSamplesFetcherTimer;
  private final Meter _partitionSamplesFetcherFailureRate;
  private final Timer _trainingSamplesFetcherTimer;
  private final Meter _trainingSamplesFetcherFailureRate;

  /**
   * Create a metric fetcher manager.
   * See {@link #MetricFetcherManager(KafkaCruiseControlConfig, MetricSampleAggregator, MetadataClient, Time, MetricRegistry, List)}
   */
  public MetricFetcherManager(KafkaCruiseControlConfig config,
                              MetricSampleAggregator metricSampleAggregator,
                              MetadataClient metadataClient,
                              Time time,
                              MetricRegistry dropwizardMetricRegistry) {
    this(config, metricSampleAggregator, metadataClient, time, dropwizardMetricRegistry, true);
  }

  /**
   * Constructor for unit test.
   *
   * @param config      The load monitor configurations.
   * @param metricSampleAggregator The queue that holds the metric samples.
   * @param metadataClient    The metadata of the cluster.
   * @param time        The time object.
   * @param dropwizardMetricRegistry The Metric Registry object.
   * @param fetchers    A list of metric fetchers.
   */
  public MetricFetcherManager(KafkaCruiseControlConfig config,
                              MetricSampleAggregator metricSampleAggregator,
                              MetadataClient metadataClient,
                              Time time,
                              MetricRegistry dropwizardMetricRegistry,
                              List<MetricSampler> fetchers) {
    this(config, metricSampleAggregator, metadataClient, time, dropwizardMetricRegistry, false);
    _metricSamplers.addAll(fetchers);
  }

  /**
   * Private constructor to avoid duplicate code.
   *
   * @param config        The load monitor configurations.
   * @param metricSampleAggregator   The queue that holds the metric samples.
   * @param metadataClient      The metadata of the cluster.
   * @param time          The time object.
   * @param dropwizardMetricRegistry The Metric Registry object.
   * @param createSampler Whether to create the metric fetchers or not. ( For unit test purpose)
   */
  private MetricFetcherManager(KafkaCruiseControlConfig config,
                               MetricSampleAggregator metricSampleAggregator,
                               MetadataClient metadataClient,
                               Time time,
                               MetricRegistry dropwizardMetricRegistry,
                               boolean createSampler) {
    _time = time;
    _metricSampleAggregator = metricSampleAggregator;
    _metadataClient = metadataClient;
    _numMetricFetchers = config.getInt(KafkaCruiseControlConfig.NUM_METRIC_FETCHERS_CONFIG);
    _samplingExecutor = Executors.newFixedThreadPool(_numMetricFetchers,
                                                     new KafkaCruiseControlThreadFactory("MetricFetcher", true, LOG));
    _metricSamplers = new ArrayList<>(_numMetricFetchers);
    if (createSampler) {
      for (int i = 0; i < _numMetricFetchers; i++) {
        MetricSampler metricSampler =
            config.getConfiguredInstance(KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG, MetricSampler.class);
        metricSampler.configure(config.originals());
        _metricSamplers.add(metricSampler);
      }
    }
    _partitionAssignor = config.getConfiguredInstance(KafkaCruiseControlConfig.METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS_CONFIG,
                                                      MetricSamplerPartitionAssignor.class);
    _partitionAssignor.configure(config.originals());
    _useLinearRegressionModel = config.getBoolean(KafkaCruiseControlConfig.USE_LINEAR_REGRESSION_MODEL_CONFIG);
    _dropwizardMetricRegistry = dropwizardMetricRegistry;
    _partitionSamplesFetcherTimer = _dropwizardMetricRegistry.timer(MetricRegistry.name("MetricFetcherManager",
                                                                                       "partition-samples-fetcher-timer"));
    _partitionSamplesFetcherFailureRate = _dropwizardMetricRegistry.meter(MetricRegistry.name("MetricFetcherManager",
                                                                                             "partition-samples-fetcher-failure-rate"));
    _trainingSamplesFetcherTimer = _dropwizardMetricRegistry.timer(MetricRegistry.name("MetricFetcherManager",
                                                                                      "training-samples-fetcher-timer"));
    _trainingSamplesFetcherFailureRate = _dropwizardMetricRegistry.meter(MetricRegistry.name("MetricFetcherManager",
                                                                                            "training-samples-fetcher-failure-rate"));

  }

  /**
   * Shutdown the metric fetcher manager.
   */
  public void shutdown() {
      for (MetricSampler metricSampler : _metricSamplers) {
        try {
          metricSampler.close();
        } catch (Exception e) {
          LOG.warn("Received exception when closing metric samplers.", e);
        }
      }
    LOG.info("Shutting down metric fetcher manager.");
    _samplingExecutor.shutdown();
    LOG.info("Metric fetcher manager shutdown completed.");
  }

  public boolean fetchPartitionMetricSamples(long startMs,
                                             long endMs,
                                             long timeoutMs,
                                             SampleStore sampleStore) throws TimeoutException {
    LOG.info("Kicking off sampling for time range [{}, {}], duration {} ms using {} fetchers with timeout {} ms.",
        startMs, endMs, endMs - startMs, _numMetricFetchers, timeoutMs);
    List<Set<TopicPartition>> partitionAssignment =
        _partitionAssignor.assignPartitions(_metadataClient.cluster(), _numMetricFetchers);
    List<MetricFetcher> samplingFetchers = new ArrayList<>();
    for (int i = 0; i < _numMetricFetchers; i++) {
      samplingFetchers.add(new SamplingFetcher(_metricSamplers.get(i), _metadataClient.cluster(), _metricSampleAggregator,
                                               sampleStore, partitionAssignment.get(i), startMs, endMs, true,
                                               _useLinearRegressionModel, _partitionSamplesFetcherTimer,
          _partitionSamplesFetcherFailureRate));
    }
    return fetchSamples(samplingFetchers, timeoutMs);
  }

  public boolean fetchBrokerMetricSamples(long startMs,
                                          long endMs,
                                          long timeoutMs,
                                          SampleStore sampleStore) throws TimeoutException {
    List<Set<TopicPartition>> partitionAssignment =
        _partitionAssignor.assignPartitions(_metadataClient.cluster(), _numMetricFetchers);
    List<MetricFetcher> trainingFetchers = new ArrayList<>();
    for (int i = 0; i < _numMetricFetchers; i++) {
      trainingFetchers.add(new TrainingFetcher(_metricSamplers.get(i), _metadataClient.cluster(), sampleStore,
                                               partitionAssignment.get(i), startMs, endMs, _trainingSamplesFetcherTimer,
          _trainingSamplesFetcherFailureRate));
    }
    return fetchSamples(trainingFetchers, timeoutMs);
  }

  // Package private functions
  private boolean fetchSamples(Collection<MetricFetcher> metricFetchers, long timeoutMs) throws TimeoutException {

    // Initialize the state before kicking off sampling.
    boolean hasSamplingError = false;
    // The start time is also the current sampling period end time.
    long samplingActionStartMs = _time.milliseconds();
    long deadlineMs = samplingActionStartMs + timeoutMs;

    // Kick off the sampling.
    List<Future<Boolean>> errorFutures = new ArrayList<>();
    for (MetricFetcher metricFetcher : metricFetchers) {
      errorFutures.add(_samplingExecutor.submit(metricFetcher));
    }

    for (Future<Boolean> future : errorFutures) {
      try {
        hasSamplingError = hasSamplingError || future.get(deadlineMs - _time.milliseconds(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Sampling scheduler thread is interrupted when waiting for sampling to finish.", e);
      } catch (ExecutionException e) {
        LOG.error("Sampling scheduler received Execution exception when waiting for sampling to finish", e);
      } catch (Exception e) {
        LOG.error("Sampling scheduler received Unknown exception when waiting for sampling to finish", e);
      }
    }

    long samplingTime = _time.milliseconds() - samplingActionStartMs;
    LOG.info("Finished sampling in {} ms.", samplingTime);

    return hasSamplingError;
  }
}
