/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
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
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils.bootstrapServers;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.createTopic;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.CLIENT_REQUEST_TIMEOUT_MS;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.maybeUpdateTopicConfig;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.maybeIncreasePartitionCount;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.wrapTopic;

/**
 * The sample store that implements the {@link SampleStore}. It stores the partition metric samples back to Kafka during ongoing execution.
 *
 * Required configurations for this class.
 * <ul>
 *   <li>{@link #PARTITION_METRIC_SAMPLE_STORE_DURING_EXECUTION_TOPIC_CONFIG}: The config for Kafka topic name to store partition samples.</li>
 *   <li>{@link #SAMPLE_STORE_DURING_EXECUTION_TOPIC_REPLICATION_FACTOR_CONFIG}: The config for the replication factor of Kafka sample store topics,
 *   default value is set to {@link #DEFAULT_SAMPLE_STORE_TOPIC_REPLICATION_FACTOR}.</li>
 *   <li>{@link #PARTITION_SAMPLE_STORE_DURING_EXECUTION_TOPIC_PARTITION_COUNT_CONFIG}: The config for the number of partition for Kafka
 *   partition sample store topic, default value is set to {@link #DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT}.</li>
 *   <li>{@link #MIN_PARTITION_SAMPLE_STORE_DURING_EXECUTION_TOPIC_RETENTION_TIME_MS_CONFIG}: The config for the minimal retention time for
 *   Kafka partition sample store topic, default value is set to {@link #DEFAULT_MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS}.</li>
 * </ul>
 */
public class KafkaPartitionMetricSampleDuringExecutionStore implements SampleStore {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaPartitionMetricSampleDuringExecutionStore.class);
  protected static final Duration PRODUCER_CLOSE_TIMEOUT = Duration.ofMinutes(3);
  // Keep additional windows in case some of the windows do not have enough samples.
  protected static final int ADDITIONAL_WINDOW_TO_RETAIN_FACTOR = 2;

  private static final short DEFAULT_SAMPLE_STORE_TOPIC_REPLICATION_FACTOR = 2;
  private static final int DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT = 32;
  private static final long DEFAULT_MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS = TimeUnit.HOURS.toMillis(1);
  private static final String PRODUCER_CLIENT_ID = "KafkaCruiseControlPartitionMetricSampleDuringExecutionStoreProducer";

  protected String _partitionMetricSampleStoreTopic;
  protected Short _sampleStoreTopicReplicationFactor;
  protected int _partitionSampleStoreTopicPartitionCount;
  protected long _minPartitionSampleStoreTopicRetentionTimeMs;
  protected volatile double _loadingProgress;
  protected Producer<byte[], byte[]> _producer;
  protected volatile boolean _shutdown = false;

  public static final String PARTITION_METRIC_SAMPLE_STORE_DURING_EXECUTION_TOPIC_CONFIG = "partition.metric.sample.store.during.execution.topic";
  public static final String SAMPLE_STORE_DURING_EXECUTION_TOPIC_REPLICATION_FACTOR_CONFIG = "sample.store.during.execution.topic.replication.factor";
  public static final String PARTITION_SAMPLE_STORE_DURING_EXECUTION_TOPIC_PARTITION_COUNT_CONFIG =
      "partition.sample.store.during.execution.topic.partition.count";
  public static final String MIN_PARTITION_SAMPLE_STORE_DURING_EXECUTION_TOPIC_RETENTION_TIME_MS_CONFIG =
      "min.partition.sample.store.during.execution.topic.retention.time.ms";

  @Override
  public void configure(Map<String, ?> config) {
    _partitionMetricSampleStoreTopic = (String) config.get(PARTITION_METRIC_SAMPLE_STORE_DURING_EXECUTION_TOPIC_CONFIG);
    String metricSampleStoreTopicReplicationFactorString = (String) config.get(SAMPLE_STORE_DURING_EXECUTION_TOPIC_REPLICATION_FACTOR_CONFIG);
    _sampleStoreTopicReplicationFactor = metricSampleStoreTopicReplicationFactorString == null
                                         || metricSampleStoreTopicReplicationFactorString.isEmpty()
                                         ? null : Short.parseShort(metricSampleStoreTopicReplicationFactorString);
    String partitionSampleStoreTopicPartitionCountString = (String) config.get(PARTITION_SAMPLE_STORE_DURING_EXECUTION_TOPIC_PARTITION_COUNT_CONFIG);
    _partitionSampleStoreTopicPartitionCount = partitionSampleStoreTopicPartitionCountString == null
                                               || partitionSampleStoreTopicPartitionCountString.isEmpty()
                                               ? DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT
                                               : Integer.parseInt(partitionSampleStoreTopicPartitionCountString);
    String minPartitionSampleStoreTopicRetentionTimeMsString =
        (String) config.get(MIN_PARTITION_SAMPLE_STORE_DURING_EXECUTION_TOPIC_RETENTION_TIME_MS_CONFIG);
    _minPartitionSampleStoreTopicRetentionTimeMs = minPartitionSampleStoreTopicRetentionTimeMsString == null
                                                   || minPartitionSampleStoreTopicRetentionTimeMsString.isEmpty()
                                                   ? DEFAULT_MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS
                                                   : Long.parseLong(minPartitionSampleStoreTopicRetentionTimeMsString);

    _producer = createProducer(config);
    _loadingProgress = -1.0;

    ensureTopicsCreated(config);
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

  protected KafkaProducer<byte[], byte[]> createProducer(Map<String, ?> config) {
    Properties producerProps = new Properties();
    producerProps.putAll(config);
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(config));
    producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_CLIENT_ID);
    // Set batch.size and linger.ms to a big number to have better batching.
    producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "30000");
    producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "500000");
    producerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864");
    producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
    producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.setProperty(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, config.get(MonitorConfig.RECONNECT_BACKOFF_MS_CONFIG).toString());
    return new KafkaProducer<>(producerProps);
  }

  @SuppressWarnings("unchecked")
  protected void ensureTopicsCreated(Map<String, ?> config) {
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient((Map<String, Object>) config);
    try {
      short replicationFactor = sampleStoreTopicReplicationFactor(config, adminClient);

      // Retention
      long partitionSampleWindowMs = (Long) config.get(MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG);
      int numPartitionSampleWindows = (Integer) config.get(MonitorConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG);
      long partitionSampleRetentionMs = (numPartitionSampleWindows * ADDITIONAL_WINDOW_TO_RETAIN_FACTOR) * partitionSampleWindowMs;
      partitionSampleRetentionMs = Math.max(_minPartitionSampleStoreTopicRetentionTimeMs, partitionSampleRetentionMs);

      // New topics
      if (_partitionMetricSampleStoreTopic != null) {
        NewTopic partitionSampleStoreNewTopic =
            wrapTopic(_partitionMetricSampleStoreTopic, _partitionSampleStoreTopicPartitionCount, replicationFactor,
                partitionSampleRetentionMs);
        ensureTopicCreated(adminClient, partitionSampleStoreNewTopic);
      }

    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }
  }

  protected void ensureTopicCreated(AdminClient adminClient, NewTopic sampleStoreTopic) {
    if (!createTopic(adminClient, sampleStoreTopic)) {
      // Update topic config and partition count to ensure desired properties.
      maybeUpdateTopicConfig(adminClient, sampleStoreTopic);
      maybeIncreasePartitionCount(adminClient, sampleStoreTopic);
    }
  }

  @Override
  public void storeSamples(MetricSampler.Samples samples) {
    final AtomicInteger metricSampleCount = new AtomicInteger(0);
    if (_partitionMetricSampleStoreTopic != null) {
      for (PartitionMetricSample sample : samples.partitionMetricSamples()) {
        _producer.send(new ProducerRecord<>(_partitionMetricSampleStoreTopic, null, sample.sampleTime(), null, sample.toBytes()),
                       (recordMetadata, e) -> {
                         if (e == null) {
                           metricSampleCount.incrementAndGet();
                         } else {
                           LOG.error("Failed to produce partition metric sample for {} of timestamp {} due to exception",
                               sample.entity().tp(), sample.sampleTime(), e);
                         }
                       });
      }
    }
    _producer.flush();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stored {} partition metric samples to Kafka", metricSampleCount.get());
    }
  }

  @Override
  public void loadSamples(SampleLoader sampleLoader) {
  }

  @Override
  public double sampleLoadingProgress() {
    return _loadingProgress;
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
