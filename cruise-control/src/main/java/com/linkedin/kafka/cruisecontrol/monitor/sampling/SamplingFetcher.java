/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.exception.MetricSamplingException;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaBrokerMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.CPU_USAGE;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils.estimateLeaderCpuUtilUsingLinearRegressionModel;


/**
 * A metric fetcher that is responsible for fetching the metric samples to monitor the cluster load.
 */
class SamplingFetcher extends MetricFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(SamplingFetcher.class);
  // The metadata of the cluster this metric fetcher is fetching from.
  private final MetricSampler _metricSampler;
  private final Cluster _cluster;
  private final KafkaPartitionMetricSampleAggregator _partitionMetricSampleAggregator;
  private final KafkaBrokerMetricSampleAggregator _brokerMetricSampleAggregator;
  private final SampleStore _sampleStore;
  private final Set<TopicPartition> _assignedPartitions;
  private final long _startTimeMs;
  private final long _endTimeMs;
  private final boolean _leaderValidation;
  private final boolean _useLinearRegressionModel;
  private final Timer _fetchTimer;
  private final Meter _fetchFailureRate;
  private final MetricDef _metricDef;
  private final long _timeout;

  SamplingFetcher(MetricSampler metricSampler,
                  Cluster cluster,
                  KafkaPartitionMetricSampleAggregator partitionMetricSampleAggregator,
                  KafkaBrokerMetricSampleAggregator brokerMetricSampleAggregator,
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
    _partitionMetricSampleAggregator = partitionMetricSampleAggregator;
    _brokerMetricSampleAggregator = brokerMetricSampleAggregator;
    _sampleStore = sampleStore;
    _assignedPartitions = assignedPartitions;
    _metricDef = metricDef;
    _startTimeMs = startTimeMs;
    _endTimeMs = endTimeMs;
    _leaderValidation = leaderValidation;
    _useLinearRegressionModel = useLinearRegressionModel;
    _fetchTimer = fetchTimer;
    _fetchFailureRate = fetchFailureRate;
    _timeout = System.currentTimeMillis() + (endTimeMs - startTimeMs) / 2;
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
      // TODO: evolve sample store interface to allow independent eviction time for different type of metric samples.
      // We are not calling sampleStore.evictSamplesBefore() because the broker metric samples and partition metric
      // samples may have different number of windows so they can not be evicted using the same timestamp.
    } catch (Exception e) {
      _fetchFailureRate.mark();
      throw e;
    } finally {
      ctx.stop();
    }
  }

  /**
   * Fetch the partition and broker metric samples.
   * @return The accepted partition and broker metric samples.
   * @throws MetricSamplingException
   */
  private MetricSampler.Samples fetchSamples() throws MetricSamplingException {
    MetricSampler.Samples samples =
        _metricSampler.getSamples(_cluster, _assignedPartitions, _startTimeMs, _endTimeMs,
                                  MetricSampler.SamplingMode.ALL, _metricDef, _timeout);
    if (samples == null) {
      samples = MetricSampler.EMPTY_SAMPLES;
    }
    addPartitionSamples(samples.partitionMetricSamples());
    addBrokerMetricSamples(samples.brokerMetricSamples());
    // Add the broker metric samples to the observation.
    ModelParameters.addMetricObservation(samples.brokerMetricSamples());

    return samples;
  }

  private void addPartitionSamples(Set<PartitionMetricSample> partitionMetricSamples) {
    // Give an initial capacity to avoid resizing.
    Set<TopicPartition> returnedPartitions = new HashSet<>(_assignedPartitions.size());
    // Ignore the null value if the metric sampler did not return a sample
    if (partitionMetricSamples != null) {
      int discarded = 0;
      Iterator<PartitionMetricSample> iter = partitionMetricSamples.iterator();
      while (iter.hasNext()) {
        PartitionMetricSample partitionMetricSample = iter.next();
        TopicPartition tp = partitionMetricSample.entity().tp();
        if (_assignedPartitions.contains(tp)) {
          // we fill in the cpu utilization based on the model in case user did not fill it in.
          if (_useLinearRegressionModel && ModelParameters.trainingCompleted()) {
            partitionMetricSample.record(KafkaMetricDef.commonMetricDef().metricInfo(CPU_USAGE.name()),
                                         estimateLeaderCpuUtilUsingLinearRegressionModel(partitionMetricSample));
          }
          // we close the metric sample in case the implementation forgot to do so.
          partitionMetricSample.close(_endTimeMs);
          // We remove the sample from the returning set if it is not accepted.
          if (_partitionMetricSampleAggregator.addSample(partitionMetricSample, _leaderValidation)) {
            LOG.trace("Enqueued partition metric sample {}", partitionMetricSample);
          } else {
            iter.remove();
            discarded++;
            LOG.trace("Failed to add partition metric sample {}", partitionMetricSample);
          }
          returnedPartitions.add(tp);
        } else {
          LOG.warn("Collected partition metric sample for partition {} which is not an assigned partition. "
                       + "The metric sample will be ignored.", tp);
        }
      }
      LOG.info("Collected {}{} partition metric samples for {} partitions. Total partition assigned: {}.",
                partitionMetricSamples.size(), discarded > 0 ? String.format("(%d discarded)", discarded) : "",
                returnedPartitions.size(), _assignedPartitions.size());
    } else {
      LOG.warn("Failed to collect partition metric samples for {} assigned partitions", _assignedPartitions.size());
    }
  }

  private void addBrokerMetricSamples(Set<BrokerMetricSample> brokerMetricSamples) {
    Set<Integer> returnedBrokerIds = new HashSet<>();
    if (brokerMetricSamples != null) {
      int discarded = 0;
      Iterator<BrokerMetricSample> iter = brokerMetricSamples.iterator();
      while (iter.hasNext()) {
        BrokerMetricSample brokerMetricSample = iter.next();
        // Close the broker metric sample in case user forgot to close it.
        brokerMetricSample.close(_endTimeMs);
        if (_brokerMetricSampleAggregator.addSample(brokerMetricSample)) {
          LOG.trace("Enqueued broker metric sample {}", brokerMetricSample);
        } else {
          iter.remove();
          discarded++;
          LOG.trace("Failed to add broker metric sample {}", brokerMetricSample);
        }
        returnedBrokerIds.add(brokerMetricSample.brokerId());
      }
      LOG.info("Collected {}{} broker metric samples for {} brokers.",
                brokerMetricSamples.size(), discarded > 0 ? String.format("(%d discarded)", discarded) : "",
                returnedBrokerIds.size());
    } else {
      LOG.warn("Failed to collect broker metrics samples.");
    }
  }
}
