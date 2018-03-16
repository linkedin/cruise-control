/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.MetricSamplingException;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaCruiseControlMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaCruiseControlMetricDef.CPU_USAGE;


/**
 * A metric fetcher that is responsible for fetching the metric samples to monitor the cluster load.
 */
class SamplingFetcher extends MetricFetcher {
  private final Logger LOG = LoggerFactory.getLogger(MetricFetcher.class);
  // The metadata of the cluster this metric fetcher is fetching from.
  private final MetricSampler _metricSampler;
  private final Cluster _cluster;
  private final KafkaPartitionMetricSampleAggregator _metricSampleAggregator;
  private final SampleStore _sampleStore;
  private final Set<TopicPartition> _assignedPartitions;
  private final long _startTimeMs;
  private final long _endTimeMs;
  private final boolean _leaderValidation;
  private final boolean _useLinearRegressionModel;
  private final Timer _fetchTimer;
  private final Meter _fetchFailureRate;
  private final MetricDef _metricDef;

  SamplingFetcher(MetricSampler metricSampler,
                  Cluster cluster,
                  KafkaPartitionMetricSampleAggregator metricSampleAggregator,
                  SampleStore sampleStore,
                  Set<TopicPartition> assignedPartitions,
                  long startTimeMs,
                  long endTimeMs,
                  boolean leaderValidation,
                  boolean useLinearRegressionModel,
                  MetricDef metricDef,
                  Timer fetchTimer,
                  Meter fetchFailureRate) {
    _metricSampler = metricSampler;
    _cluster = cluster;
    _metricSampleAggregator = metricSampleAggregator;
    _sampleStore = sampleStore;
    _assignedPartitions = assignedPartitions;
    _metricDef = metricDef;
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
                                  MetricSampler.SamplingMode.ALL, _metricDef);
    if (samples == null) {
      samples = MetricSampler.EMPTY_SAMPLES;
    }
    Long earliestWindowBefore = _metricSampleAggregator.earliestWindow();
    // Give an initial capacity to avoid resizing.
    Set<TopicPartition> returnedPartitions = new HashSet<>(_assignedPartitions.size());
    // Ignore the null value if the metric sampler did not return a sample
    if (samples.partitionMetricSamples() != null) {
      Iterator<PartitionMetricSample> iter = samples.partitionMetricSamples().iterator();
      while (iter.hasNext()) {
        PartitionMetricSample partitionMetricSample = iter.next();
        TopicPartition tp = partitionMetricSample.entity().tp();
        if (_assignedPartitions.contains(tp)) {
          // we fill in the cpu utilization based on the model in case user did not fill it in.
          if (_useLinearRegressionModel && ModelParameters.trainingCompleted()) {
            partitionMetricSample.record(KafkaCruiseControlMetricDef.commonMetricDef().metricInfo(CPU_USAGE.name()),
                                         estimateCpuUtil(partitionMetricSample));
          }
          // we close the metric sample in case the implementation forgot to do so.
          partitionMetricSample.close(_endTimeMs);
          // We remove the sample from the returning set if it is not accepted.
          if (!_metricSampleAggregator.addSample(partitionMetricSample, _leaderValidation)) {
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

    Long earliestWindow = _metricSampleAggregator.earliestWindow();
    if (earliestWindowBefore != null
        && earliestWindow != null
        && earliestWindow > earliestWindowBefore) {
      _sampleStore.evictSamplesBefore(earliestWindow);
    }

    return samples;
  }

  private double estimateCpuUtil(PartitionMetricSample partitionMetricSample) {
    int cpuId = KafkaCruiseControlMetricDef.resourceToMetricId(Resource.CPU);
    int networkOutId = KafkaCruiseControlMetricDef.resourceToMetricId(Resource.NW_OUT);
    return ModelUtils.estimateLeaderCpuUtilUsingLinearRegressionModel(partitionMetricSample.metricValue(cpuId),
                                                                      partitionMetricSample.metricValue(networkOutId));
  }
}
