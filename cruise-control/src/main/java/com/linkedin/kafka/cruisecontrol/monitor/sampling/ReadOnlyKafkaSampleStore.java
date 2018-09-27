/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Executors;


/**
 * This samples store only reads the partition metric samples and broker metric samples from the Kafka topic.
 * It does not change any configurations or produce to Kafka.
 */
public class ReadOnlyKafkaSampleStore extends KafkaSampleStore {
  /**
   * We have to override the configure method so that no producer is created and no ZK topic configuration change
   * is made either.
   */
  @Override
  public void configure(Map<String, ?> config) {
    _partitionMetricSampleStoreTopic = KafkaCruiseControlUtils.getRequiredConfig(config, PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG);
    _brokerMetricSampleStoreTopic = KafkaCruiseControlUtils.getRequiredConfig(config, BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG);
    String metricSampleStoreTopicReplicationFactorString = (String) config.get(SAMPLE_STORE_TOPIC_REPLICATION_FACTOR_CONFIG);
    _sampleStoreTopicReplicationFactor = metricSampleStoreTopicReplicationFactorString == null || metricSampleStoreTopicReplicationFactorString.isEmpty()
        ? null : Integer.parseInt(metricSampleStoreTopicReplicationFactorString);
    String numProcessingThreadsString = (String) config.get(NUM_SAMPLE_LOADING_THREADS_CONFIG);
    int numProcessingThreads = numProcessingThreadsString == null || numProcessingThreadsString.isEmpty()
        ? DEFAULT_NUM_SAMPLE_LOADING_THREADS : Integer.parseInt(numProcessingThreadsString);
    _metricProcessorExecutor = Executors.newFixedThreadPool(numProcessingThreads);
    _consumers = new ArrayList<>(numProcessingThreads);
    for (int i = 0; i < numProcessingThreads; i++) {
      _consumers.add(createConsumers(config));
    }
    _loadingProgress = -1.0;
  }

  @Override
  public void close() {
    // Do nothing.
  }

  @Override
  public void storeSamples(MetricSampler.Samples samples) {
    // Do nothing.
  }

  @Override
  public void evictSamplesBefore(long timestamp) {
    // Do nothing.
  }
}
