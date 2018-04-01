/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.exception.MetricSamplingException;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;


/**
 * A metric fetcher that is responsible for fetching the partition metric samples for model training.
 */
class TrainingFetcher extends MetricFetcher {
  private final MetricSampler _metricSampler;
  private final Cluster _cluster;
  private final SampleStore _sampleStore;
  private final Set<TopicPartition> _assignedPartitions;
  private final long _startTimeMs;
  private final long _endTimeMs;
  private final Timer _fetcherTimer;
  private final Meter _fetcherFailureRate;
  private final MetricDef _metricDef;

  TrainingFetcher(MetricSampler metricSampler,
                  Cluster cluster,
                  SampleStore sampleStore,
                  Set<TopicPartition> assignedPartitions,
                  long startTimeMs,
                  long endTimeMs,
                  MetricDef metricDef,
                  Timer fetcherTimer,
                  Meter fetcherFailureRate) {
    _cluster = cluster;
    _sampleStore = sampleStore;
    _metricSampler = metricSampler;
    _startTimeMs = startTimeMs;
    _endTimeMs = endTimeMs;
    _assignedPartitions = assignedPartitions;
    _metricDef = metricDef;
    _fetcherTimer = fetcherTimer;
    _fetcherFailureRate = fetcherFailureRate;
  }

  @Override
  protected void fetchMetricsForAssignedPartitions() throws MetricSamplingException {
    final Timer.Context ctx = _fetcherTimer.time();

    try {
      MetricSampler.Samples samples =
          _metricSampler.getSamples(_cluster, _assignedPartitions, _startTimeMs, _endTimeMs,
                                    MetricSampler.SamplingMode.BROKER_METRICS_ONLY, _metricDef);
      ModelParameters.addMetricObservation(samples.brokerMetricSamples());

      _sampleStore.storeSamples(samples);
    } catch (Exception e) {
      _fetcherFailureRate.mark();
      throw e;
    } finally {
      ctx.stop();
    }
  }
}
