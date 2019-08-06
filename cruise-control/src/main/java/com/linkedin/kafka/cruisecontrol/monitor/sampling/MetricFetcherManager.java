/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaBrokerMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;
import java.util.Collections;
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
  static final String BROKER_CAPACITY_CONFIG_RESOLVER_OBJECT_CONFIG = "broker.capacity.config.resolver.object";
  private static final Logger LOG = LoggerFactory.getLogger(MetricFetcherManager.class);
  static final int SUPPORTED_NUM_METRIC_FETCHER = 1;

  private final Time _time;
  private final KafkaPartitionMetricSampleAggregator _partitionMetricSampleAggregator;
  private final KafkaBrokerMetricSampleAggregator _brokerMetricSampleAggregator;
  private final MetadataClient _metadataClient;
  private final MetricSampler _metricSampler;
  private final MetricSamplerPartitionAssignor _partitionAssignor;
  private final ExecutorService _samplingExecutor;
  // The following two configuration is actually for MetricSampleAggregator, the MetricFetcherManager uses it to
  // check if a bootstrap is done or not.
  private final boolean _useLinearRegressionModel;
  private final MetricDef _metricDef;
  // The below two members keep track last time the sampling threads were executed
  private final Timer _samplingFetcherTimer;
  private final Meter _samplingFetcherFailureRate;
  private final Timer _trainingSamplesFetcherTimer;
  private final Meter _trainingSamplesFetcherFailureRate;

  /**
   * Create a metric fetcher manager.
   * See {@link #MetricFetcherManager(KafkaCruiseControlConfig, KafkaPartitionMetricSampleAggregator, KafkaBrokerMetricSampleAggregator,
   * MetadataClient, MetricDef, Time, MetricRegistry, BrokerCapacityConfigResolver, MetricSampler)}
   */
  public MetricFetcherManager(KafkaCruiseControlConfig config,
                              KafkaPartitionMetricSampleAggregator partitionMetricSampleAggregator,
                              KafkaBrokerMetricSampleAggregator brokerMetricSampleAggregator,
                              MetadataClient metadataClient,
                              MetricDef metricDef,
                              Time time,
                              MetricRegistry dropwizardMetricRegistry,
                              BrokerCapacityConfigResolver brokerCapacityConfigResolver) {
    this(config, partitionMetricSampleAggregator, brokerMetricSampleAggregator, metadataClient, metricDef, time, dropwizardMetricRegistry,
         brokerCapacityConfigResolver, null);
  }

  /**
   * Create a metric fetcher manager.
   *
   * @param config      The load monitor configurations.
   * @param partitionMetricSampleAggregator The {@link KafkaPartitionMetricSampleAggregator} to aggregate partition metrics.
   * @param brokerMetricSampleAggregator The {@link KafkaBrokerMetricSampleAggregator} to aggregate the broker metrics.
   * @param metadataClient    The metadata of the cluster.
   * @param metricDef the metric definitions.
   * @param time        The time object.
   * @param dropwizardMetricRegistry The Metric Registry object.
   * @param brokerCapacityConfigResolver The resolver for retrieving broker capacities.
   * @param sampler Metric fetcher or {@code null} to create one using {@link KafkaCruiseControlConfig#METRIC_SAMPLER_CLASS_CONFIG}.
   */
  public MetricFetcherManager(KafkaCruiseControlConfig config,
                              KafkaPartitionMetricSampleAggregator partitionMetricSampleAggregator,
                              KafkaBrokerMetricSampleAggregator brokerMetricSampleAggregator,
                              MetadataClient metadataClient,
                              MetricDef metricDef,
                              Time time,
                              MetricRegistry dropwizardMetricRegistry,
                              BrokerCapacityConfigResolver brokerCapacityConfigResolver,
                              MetricSampler sampler) {
    _time = time;
    _partitionMetricSampleAggregator = partitionMetricSampleAggregator;
    _brokerMetricSampleAggregator = brokerMetricSampleAggregator;
    _metadataClient = metadataClient;
    _metricDef = metricDef;
    _samplingExecutor = Executors.newFixedThreadPool(SUPPORTED_NUM_METRIC_FETCHER,
                                                     new KafkaCruiseControlThreadFactory("MetricFetcher", true, LOG));
    _partitionAssignor = config.getConfiguredInstance(KafkaCruiseControlConfig.METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS_CONFIG,
                                                      MetricSamplerPartitionAssignor.class);
    _partitionAssignor.configure(config.mergedConfigValues());
    _useLinearRegressionModel = config.getBoolean(KafkaCruiseControlConfig.USE_LINEAR_REGRESSION_MODEL_CONFIG);
    _samplingFetcherTimer = dropwizardMetricRegistry.timer(MetricRegistry.name("MetricFetcherManager",
                                                                                "partition-samples-fetcher-timer"));
    _samplingFetcherFailureRate = dropwizardMetricRegistry.meter(MetricRegistry.name("MetricFetcherManager",
                                                                                      "partition-samples-fetcher-failure-rate"));
    _trainingSamplesFetcherTimer = dropwizardMetricRegistry.timer(MetricRegistry.name("MetricFetcherManager",
                                                                                       "training-samples-fetcher-timer"));
    _trainingSamplesFetcherFailureRate = dropwizardMetricRegistry.meter(MetricRegistry.name("MetricFetcherManager",
                                                                                             "training-samples-fetcher-failure-rate"));

    _metricSampler = sampler == null
                     ? config.getConfiguredInstance(KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG, MetricSampler.class,
                                                    Collections.singletonMap(BROKER_CAPACITY_CONFIG_RESOLVER_OBJECT_CONFIG,
                                                                             brokerCapacityConfigResolver))
                     : sampler;
  }

  /**
   * Shutdown the metric fetcher manager.
   */
  public void shutdown() {
    try {
      _metricSampler.close();
    } catch (Exception e) {
      LOG.warn("Received exception when closing metric samplers.", e);
    }
    LOG.info("Shutting down metric fetcher manager.");
    _samplingExecutor.shutdown();
    LOG.info("Metric fetcher manager shutdown completed.");
  }

  /**
   * Fetch the partition metric samples for a given period.
   * @param startMs the starting time of the fetching period.
   * @param endMs the end time of the fetching period.
   * @param timeoutMs the timeout.
   * @param sampleStore the sample store to save the partition metric samples.
   * @return true if there was no fetching error, false otherwise.
   */
  public boolean fetchPartitionMetricSamples(long startMs,
                                             long endMs,
                                             long timeoutMs,
                                             SampleStore sampleStore) {
    LOG.info("Kicking off partition metric sampling for time range [{}, {}], duration {} ms with timeout {} ms.",
             startMs, endMs, endMs - startMs, timeoutMs);
    Set<TopicPartition> partitionAssignment = _partitionAssignor.assignPartitions(_metadataClient.cluster());
    MetricFetcher samplingFetcher = new SamplingFetcher(_metricSampler,
                                                        _metadataClient.cluster(),
                                                        _partitionMetricSampleAggregator,
                                                        _brokerMetricSampleAggregator,
                                                        sampleStore,
                                                        partitionAssignment,
                                                        startMs,
                                                        endMs,
                                                        true,
                                                        _useLinearRegressionModel,
                                                        _metricDef,
                                                        _samplingFetcherTimer,
                                                        _samplingFetcherFailureRate);
    return fetchSamples(samplingFetcher, timeoutMs);
  }

  /**
   * Fetch the broker metric samples for a given period.
   * @param startMs the starting time of the fetching period.
   * @param endMs the end time of the fetching period.
   * @param timeoutMs the timeout.
   * @param sampleStore the sample store to save the broker metric samples.
   * @return true if there was no fetching error, false otherwise.
   */
  public boolean fetchBrokerMetricSamples(long startMs,
                                          long endMs,
                                          long timeoutMs,
                                          SampleStore sampleStore) {
    LOG.info("Kicking off broker metric sampling for time range [{}, {}], duration {} ms with timeout {} ms.",
             startMs, endMs, endMs - startMs, timeoutMs);
    Set<TopicPartition> partitionAssignment = _partitionAssignor.assignPartitions(_metadataClient.cluster());
    MetricFetcher trainingFetcher = new TrainingFetcher(_metricSampler,
                                                        _metadataClient.cluster(),
                                                        sampleStore,
                                                        partitionAssignment,
                                                        startMs,
                                                        endMs,
                                                        _metricDef,
                                                        _trainingSamplesFetcherTimer,
                                                        _trainingSamplesFetcherFailureRate);
    return fetchSamples(trainingFetcher, timeoutMs);
  }

  // Package private functions
  private boolean fetchSamples(MetricFetcher metricFetcher, long timeoutMs) {

    // Initialize the state before kicking off sampling.
    boolean hasSamplingError = false;
    // The start time is also the current sampling period end time.
    long samplingActionStartMs = _time.milliseconds();
    long deadlineMs = samplingActionStartMs + timeoutMs;

    // Kick off the sampling.
    Future<Boolean> errorFuture = _samplingExecutor.submit(metricFetcher);

    try {
      hasSamplingError = errorFuture.get(deadlineMs - _time.milliseconds(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Sampling scheduler thread is interrupted when waiting for sampling to finish.", e);
    } catch (ExecutionException e) {
      LOG.error("Sampling scheduler received Execution exception when waiting for sampling to finish.", e);
    } catch (TimeoutException e) {
      LOG.error("Sampling scheduler received Timeout exception when waiting for sampling to finish.", e);
    } catch (Exception e) {
      LOG.error("Sampling scheduler received Unknown exception when waiting for sampling to finish.", e);
    }

    long samplingTime = _time.milliseconds() - samplingActionStartMs;
    LOG.info("Finished sampling in {} ms.", samplingTime);

    return hasSamplingError;
  }
}
