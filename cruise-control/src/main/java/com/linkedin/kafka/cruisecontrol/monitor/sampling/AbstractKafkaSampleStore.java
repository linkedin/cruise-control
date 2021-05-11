/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.CLIENT_REQUEST_TIMEOUT_MS;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.createTopic;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.maybeUpdateTopicConfig;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.maybeIncreasePartitionCount;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils.bootstrapServers;


public abstract class AbstractKafkaSampleStore implements SampleStore {
  protected static final Duration PRODUCER_CLOSE_TIMEOUT = Duration.ofMinutes(3);
  protected static final short DEFAULT_SAMPLE_STORE_TOPIC_REPLICATION_FACTOR = 2;
  protected static final int DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT = 32;

  protected volatile boolean _shutdown = false;
  protected Short _sampleStoreTopicReplicationFactor;
  protected Producer<byte[], byte[]> _producer;

  protected void createProducer(Map<String, ?> config, String producerClientId) {
    Properties producerProps = new Properties();
    producerProps.putAll(config);
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(config));
    producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, producerClientId);
    // Set batch.size and linger.ms to a big number to have better batching.
    producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "30000");
    producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "500000");
    producerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864");
    producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
    producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.setProperty(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, config.get(MonitorConfig.RECONNECT_BACKOFF_MS_CONFIG).toString());
    _producer = new KafkaProducer<>(producerProps);
  }

  /**
   * Retrieve the desired replication factor of sample store topics.
   *
   * @param config The configurations for Cruise Control.
   * @param adminClient The adminClient to send describeCluster request.
   * @return Desired replication factor of sample store topics, or {@code null} if failed to resolve replication factor.
   */
  protected short sampleStoreTopicReplicationFactor(Map<String, ?> config, AdminClient adminClient) {
    if (_sampleStoreTopicReplicationFactor == null) {
      short numberOfBrokersInCluster;
      try {
        numberOfBrokersInCluster = (short) adminClient.describeCluster().nodes().get(CLIENT_REQUEST_TIMEOUT_MS,
                                                                                     TimeUnit.MILLISECONDS).size();
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw new IllegalStateException("Auto creation of sample store topics failed due to failure to describe cluster.", e);
      }
      if (numberOfBrokersInCluster <= 1) {
        throw new IllegalStateException(String.format("Kafka cluster has less than 2 brokers (brokers in cluster=%d, zookeeper.connect=%s)",
                                                      numberOfBrokersInCluster, config.get(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG)));
      }

      return (short) Math.min(DEFAULT_SAMPLE_STORE_TOPIC_REPLICATION_FACTOR, numberOfBrokersInCluster);
    }

    return _sampleStoreTopicReplicationFactor;
  }

  protected void ensureTopicCreated(AdminClient adminClient, NewTopic sampleStoreTopic) {
    if (!createTopic(adminClient, sampleStoreTopic)) {
      // Update topic config and partition count to ensure desired properties.
      maybeUpdateTopicConfig(adminClient, sampleStoreTopic);
      maybeIncreasePartitionCount(adminClient, sampleStoreTopic);
    }
  }

  static AtomicInteger storePartitionMetricSamples(MetricSampler.Samples samples, Producer<byte[], byte[]> producer,
                                                   String partitionMetricSampleStoreTopic, Logger log) {
    final AtomicInteger metricSampleCount = new AtomicInteger(0);
    for (PartitionMetricSample sample : samples.partitionMetricSamples()) {
      producer.send(new ProducerRecord<>(partitionMetricSampleStoreTopic, null, sample.sampleTime(), null, sample.toBytes()),
                    (recordMetadata, e) -> {
                      if (e == null) {
                        metricSampleCount.incrementAndGet();
                      } else {
                        log.error("Failed to produce partition metric sample for {} of timestamp {} due to exception",
                                  sample.entity().tp(), sample.sampleTime(), e);
                      }
                    });
    }
    return metricSampleCount;
  }

  @Override
  public void evictSamplesBefore(long timestamp) {
    //TODO: use the deleteMessageBefore method to delete old samples.
  }

  @Override
  public void close() {
    _shutdown = true;
    _producer.close(PRODUCER_CLOSE_TIMEOUT);
  }
}
