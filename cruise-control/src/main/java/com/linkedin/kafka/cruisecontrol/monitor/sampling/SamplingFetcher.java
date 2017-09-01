/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.MetricSamplingException;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A metric fetcher that is responsible for fetching the metric samples to monitor the cluster load.
 */
class SamplingFetcher extends MetricFetcher {
  private final Logger LOG = LoggerFactory.getLogger(MetricFetcher.class);
  // The metadata of the cluster this metric fetcher is fetching from.
  private final MetricSampler _metricSampler;
  private final Cluster _cluster;
  private final MetricSampleAggregator _metricSampleAggregator;
  private final SampleStore _sampleStore;
  private final Set<TopicPartition> _assignedPartitions;
  private final long _startTimeMs;
  private final long _endTimeMs;
  private final boolean _leaderValidation;
  private final boolean _useLinearRegressionModel;
  private final Timer _fetchTimer;
  private final Meter _fetchFailureRate;

  SamplingFetcher(MetricSampler metricSampler,
                  Cluster cluster,
                  MetricSampleAggregator metricSampleAggregator,
                  SampleStore sampleStore,
                  Set<TopicPartition> assignedPartitions,
                  long startTimeMs,
                  long endTimeMs,
                  boolean leaderValidation,
                  boolean useLinearRegressionModel,
                  Timer fetchTimer,
                  Meter fetchFailureRate) {
    _metricSampler = metricSampler;
    _cluster = cluster;
    _metricSampleAggregator = metricSampleAggregator;
    _sampleStore = sampleStore;
    _assignedPartitions = assignedPartitions;
    _startTimeMs = startTimeMs;
    _endTimeMs = endTimeMs;
    _leaderValidation = leaderValidation;
    _useLinearRegressionModel = useLinearRegressionModel;
    _fetchTimer = fetchTimer;
    _fetchFailureRate = fetchFailureRate;
  }

  /**
   * Execute one iteration of metric sampling for all the assigned partitions.
   */
  @Override
  protected void fetchMetricsForAssignedPartitions() throws MetricSamplingException {
    final Timer.Context ctx = _fetchTimer.time();

    try {
      MetricSampler.Samples samples = fetchSamples();
      _sampleStore.storeSamples(samples);
    } catch (Exception e) {
      _fetchFailureRate.mark();
      throw e;
    } finally {
      ctx.stop();
    }
  }

  /**
   * Fetch the partition metric samples.
   * @return the accepted partition metric samples.
   * @throws MetricSamplingException
   */
  private MetricSampler.Samples fetchSamples() throws MetricSamplingException {
    MetricSampler.Samples samples =
        _metricSampler.getSamples(_cluster, _assignedPartitions, _startTimeMs, _endTimeMs,
                                  MetricSampler.SamplingMode.ALL);
    if (samples == null) {
      samples = MetricSampler.EMPTY_SAMPLES;
    }
    Long earliestSnapshotWindowBefore = _metricSampleAggregator.earliestSnapshotWindow();
    // Give an initial capacity to avoid resizing.
    Set<TopicPartition> returnedPartitions = new HashSet<>(_assignedPartitions.size());
    // Ignore the null value if the metric sampler did not return a sample
    if (samples.partitionMetricSamples() != null) {
      Iterator<PartitionMetricSample> iter = samples.partitionMetricSamples().iterator();
      while (iter.hasNext()) {
        PartitionMetricSample partitionMetricSample = iter.next();
        TopicPartition tp = partitionMetricSample.topicPartition();
        if (_assignedPartitions.contains(tp)) {
          // we fill in the cpu utilization based on the model in case user did not fill it in.
          if (_useLinearRegressionModel && ModelParameters.trainingCompleted()) {
            partitionMetricSample.record(Resource.CPU, estimateCpuUtil(partitionMetricSample), true);
          }
          // we close the metric sample in case the implementation forgot to do so.
          partitionMetricSample.close(_endTimeMs);
          // We remove the sample from the returning set if it is not accepted.
          if (!_metricSampleAggregator.addSample(partitionMetricSample, _leaderValidation, true)) {
            iter.remove();
          }
          returnedPartitions.add(tp);
          LOG.trace("Enqueued metric sample {}", partitionMetricSample);
        } else {
          LOG.warn("Collected metric sample for partition {} which is not an assigned partition. "
                       + "The metric sample will be ignored.", tp);
        }
      }
      LOG.debug("Collected {} metric samples for {} partitions. Total partition assigned: {}.",
                samples.partitionMetricSamples().size(), returnedPartitions.size(), _assignedPartitions.size());
    } else {
      LOG.warn("Failed to collect metric samples for {} assigned partitions", _assignedPartitions.size());
    }

    // Add the broker metric samples to the observation.
    ModelParameters.addMetricObservation(samples.brokerMetricSamples());
    for (BrokerMetricSample brokerMetricSample : samples.brokerMetricSamples()) {
      _metricSampleAggregator.updateBrokerMetricSample(brokerMetricSample);
    }

    Long earliestSnapshotWindow = _metricSampleAggregator.earliestSnapshotWindow();
    if (earliestSnapshotWindowBefore != null
        && earliestSnapshotWindow != null
        && earliestSnapshotWindow > earliestSnapshotWindowBefore) {
      _sampleStore.evictSamplesBefore(earliestSnapshotWindow);
    }

    return samples;
  }

  private double estimateCpuUtil(PartitionMetricSample partitionMetricSample) {
    return ModelUtils.estimateLeaderCpuUtilUsingLinearRegressionModel(partitionMetricSample.metricFor(Resource.NW_IN),
                                                                      partitionMetricSample.metricFor(Resource.NW_OUT));
  }
}
