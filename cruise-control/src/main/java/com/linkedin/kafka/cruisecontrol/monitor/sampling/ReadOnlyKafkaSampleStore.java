/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;


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
    _partitionMetricSampleStoreTopic = (String) config.get(PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG);
    _brokerMetricSampleStoreTopic = (String) config.get(BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG);
    if (_partitionMetricSampleStoreTopic == null
        || _brokerMetricSampleStoreTopic == null
        || _partitionMetricSampleStoreTopic.isEmpty()
        || _brokerMetricSampleStoreTopic.isEmpty()) {
      throw new IllegalArgumentException("The sample store topic names must be configured.");
    }
    String numProcessingThreadsString = (String) config.get(NUM_PROCESSING_THREADS_CONFIG);
    int numProcessingThreads = numProcessingThreadsString == null || numProcessingThreadsString.isEmpty() ?
        5 : Integer.parseInt(numProcessingThreadsString);
    _metricProcessorExecutor = Executors.newFixedThreadPool(numProcessingThreads);
    _metricRecordsQueues = new ArrayList<>(numProcessingThreads);
    for (int i = 0; i < numProcessingThreads; i++) {
      _metricRecordsQueues.add(new LinkedBlockingQueue<>(5));
    }
    Properties consumerProps = new Properties();
    long randomToken = RANDOM.nextLong();
    consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                              (String) config.get(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG));
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "LiKafkaCruiseControlSampleStore" + randomToken);
    consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_CLIENT_ID + randomToken);
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    KafkaCruiseControlUtils.setSslConfigs(consumerProps, config);
    _consumer = new KafkaConsumer<>(consumerProps);
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
