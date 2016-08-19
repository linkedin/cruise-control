/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.log.LogConfig;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The sample store that implements the {@link SampleStore}. It stores the partition metric samples and broker metric
 * samples back to Kafka and load from Kafka at startup.
 */
public class KafkaSampleStore implements SampleStore {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSampleStore.class);
  public static final String PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG = "partition.metric.sample.store.topic";
  public static final String BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG = "broker.metric.sample.store.topic";
  public static final String NUM_PROCESSING_THREADS_CONFIG = "num.processing.threads";
  protected static final String PRODUCER_CLIENT_ID = "LiKafkaCruiseControlSampleStoreProducer";
  protected static final String CONSUMER_CLIENT_ID = "LiKafkaCruiseControlSampleStoreConsumer";
  // Keep additional snapshot windows in case some of the windows do not have enough samples.
  private static final int ADDITIONAL_SNAPSHOT_WINDOW_TO_RETAIN_FACTOR = 2;
  private static final ConsumerRecords<byte[], byte[]> SHUTDOWN_RECORDS = new ConsumerRecords<>(Collections.emptyMap());
  protected static final Random RANDOM = new Random();
  protected List<LinkedBlockingQueue<ConsumerRecords<byte[], byte[]>>> _metricRecordsQueues;
  protected ExecutorService _metricProcessorExecutor;
  protected String _partitionMetricSampleStoreTopic;
  protected String _brokerMetricSampleStoreTopic;
  protected volatile double _loadingProgress;
  protected Producer<byte[], byte[]> _producer;
  protected Consumer<byte[], byte[]> _consumer;
  protected volatile boolean _shutdown = false;

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
    Properties producerProps = new Properties();
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                              (String) config.get(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG));
    producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_CLIENT_ID);
    // Set batch.size and linger.ms to a big number to have better batching.
    producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "30000");
    producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "800000");
    producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
    producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    KafkaCruiseControlUtils.setSslConfigs(producerProps, config);
    _producer = new KafkaProducer<>(producerProps);

    Properties consumerProps = new Properties();
    long randomToken = RANDOM.nextLong();
    consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                              (String) config.get(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG));
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "LiKafkaCruiseControlSampleStore" + randomToken);
    consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_CLIENT_ID + randomToken);
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(Integer.MAX_VALUE));
    consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    KafkaCruiseControlUtils.setSslConfigs(consumerProps, config);
    _consumer = new KafkaConsumer<>(consumerProps);
    _loadingProgress = -1.0;

    ensureTopicCreated(config);
  }

  private void ensureTopicCreated(Map<String, ?> config) {
    ZkUtils zkUtils = createZkUtils(config);
    Map<String, List<PartitionInfo>> topics = _consumer.listTopics();
    long snapshotWindowMs = Long.parseLong((String) config.get(KafkaCruiseControlConfig.LOAD_SNAPSHOT_WINDOW_MS_CONFIG));
    int numSnapshotWindows = Integer.parseInt((String) config.get(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG));
    long retentionMs = (numSnapshotWindows * ADDITIONAL_SNAPSHOT_WINDOW_TO_RETAIN_FACTOR) * snapshotWindowMs;
    Properties props = new Properties();
    props.setProperty(LogConfig.RetentionMsProp(), Long.toString(retentionMs));
    int replicationFactor = Math.min(2, zkUtils.getAllBrokersInCluster().size());
    if (!topics.containsKey(_partitionMetricSampleStoreTopic)) {
      AdminUtils.createTopic(zkUtils, _partitionMetricSampleStoreTopic, 8, replicationFactor, props, RackAwareMode.Safe$.MODULE$);
    } else {
      AdminUtils.changeTopicConfig(zkUtils, _partitionMetricSampleStoreTopic, props);
    }

    if (!topics.containsKey(_brokerMetricSampleStoreTopic)) {
      AdminUtils.createTopic(zkUtils, _brokerMetricSampleStoreTopic, 8, replicationFactor, props, RackAwareMode.Safe$.MODULE$);
    } else {
      AdminUtils.changeTopicConfig(zkUtils, _brokerMetricSampleStoreTopic, props);
    }

    KafkaCruiseControlUtils.closeZkUtilsWithTimeout(zkUtils, 10000);
  }

  private ZkUtils createZkUtils(Map<String, ?> config) {
    String zkConnect = (String) config.get(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG);
    return ZkUtils.apply(zkConnect, 30000, 30000, false);
  }

  @Override
  public void storeSamples(MetricSampler.Samples samples) {
    final AtomicInteger metricSampleCount = new AtomicInteger(0);
    for (PartitionMetricSample sample : samples.partitionMetricSamples()) {
      _producer.send(new ProducerRecord<>(_partitionMetricSampleStoreTopic, null, sample.sampleTime(), null, sample.toBytes()),
                     new Callback() {
                       @Override
                       public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                         if (e == null) {
                           metricSampleCount.incrementAndGet();
                         } else {
                           LOG.error("Failed to produce partition metric sample for {} of timestamp {} due to exception",
                                     sample.topicPartition(), sample.sampleTime(), e);
                         }
                       }
                     });
    }
    final AtomicInteger brokerMetricSampleCount = new AtomicInteger(0);
    for (BrokerMetricSample sample : samples.brokerMetricSamples()) {
      _producer.send(new ProducerRecord<>(_brokerMetricSampleStoreTopic, sample.toBytes()),
                     new Callback() {
                       @Override
                       public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                         if (e == null) {
                           brokerMetricSampleCount.incrementAndGet();
                         } else {
                           LOG.error("Failed to produce model training sample due to exception", e);
                         }
                       }
                     });
    }
    _producer.flush();
    LOG.debug("Stored {} partition metric samples and {} broker metric samples to Kafka",
              metricSampleCount.get(), brokerMetricSampleCount.get());
  }

  @Override
  public void loadSamples(SampleLoader sampleLoader) {
    LOG.info("Starting loading samples.");
    long startMs = System.currentTimeMillis();
    AtomicLong numPartitionMetricSamples = new AtomicLong(0L);
    AtomicLong numBrokerMetricSamples = new AtomicLong(0L);
    long totalSamples = -1L;
    AtomicLong numLoadedSamples = new AtomicLong(0L);
    try {
      prepareConsumer();
      Map<TopicPartition, Long> beginningOffsets = _consumer.beginningOffsets(_consumer.assignment());
      Map<TopicPartition, Long> endOffsets = _consumer.endOffsets(_consumer.assignment());
      LOG.debug("Loading beginning offsets: {}, loading end offsets: {}", beginningOffsets, endOffsets);
      for (Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
        totalSamples += endOffsets.get(entry.getKey()) - entry.getValue();
      }
      int numQueues = _metricRecordsQueues.size();
      for (LinkedBlockingQueue<ConsumerRecords<byte[], byte[]>> metricRecordsQueue : _metricRecordsQueues) {
        _metricProcessorExecutor.submit(
            new MetricProcessor(sampleLoader, numLoadedSamples, numPartitionMetricSamples, numBrokerMetricSamples,
                                totalSamples, metricRecordsQueue));
      }
      int i = 0;
      do {
        ConsumerRecords<byte[], byte[]> consumerRecords = _consumer.poll(100);
        while (!_metricRecordsQueues.get(i % numQueues).offer(consumerRecords, 10, TimeUnit.MILLISECONDS)) {
          i++;
        }
      } while (!sampleLoadingFinished(endOffsets) && !_shutdown);
    } catch (Exception e) {
      LOG.error("Received exception when loading samples", e);
    } finally {
      _metricRecordsQueues.forEach(q -> q.offer(SHUTDOWN_RECORDS));
      _consumer.close();
      _metricProcessorExecutor.shutdown();
      try {
        _metricProcessorExecutor.awaitTermination(30000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted during waiting for metrics processor to shutdown.");
      }
    }
    LOG.info("Sample loading finished. Loaded {} partition metrics samples and {} broker metric samples in {} ms",
             numPartitionMetricSamples, numBrokerMetricSamples, System.currentTimeMillis() - startMs);
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
    _producer.close(300L, TimeUnit.SECONDS);
  }

  private void prepareConsumer() {
    List<TopicPartition> assignments = new ArrayList<>();
    for (String topic : Arrays.asList(_partitionMetricSampleStoreTopic, _brokerMetricSampleStoreTopic)) {
      for (PartitionInfo partInfo : _consumer.partitionsFor(topic)) {
        assignments.add(new TopicPartition(partInfo.topic(), partInfo.partition()));
      }
    }
    _consumer.assign(assignments);
    _consumer.seekToBeginning(assignments);
  }

  private boolean sampleLoadingFinished(Map<TopicPartition, Long> endOffsets) {
    for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
      long position = _consumer.position(entry.getKey());
      if (position < entry.getValue()) {
        LOG.trace("Partition {} is still lagging. Current position: {}, LEO: {}", entry.getKey(),
                  position, entry.getValue());
        return false;
      }
    }
    return true;
  }

  private class MetricProcessor implements Runnable {
    private final SampleLoader _sampleLoader;
    private final AtomicLong _numLoadedSamples;
    private final AtomicLong _numPartitionMetricSamples;
    private final AtomicLong _numBrokerMetricSamples;
    private final long _totalSamples;
    private final LinkedBlockingQueue<ConsumerRecords<byte[], byte[]>> _queue;

    MetricProcessor(SampleLoader sampleLoader,
                    AtomicLong numLoadedSamples,
                    AtomicLong numPartitionMetricSamples,
                    AtomicLong numBrokerMetricSamples,
                    long totalSamples,
                    LinkedBlockingQueue<ConsumerRecords<byte[], byte[]>> queue) {
      _sampleLoader = sampleLoader;
      _numLoadedSamples = numLoadedSamples;
      _numPartitionMetricSamples = numPartitionMetricSamples;
      _numBrokerMetricSamples = numBrokerMetricSamples;
      _totalSamples = totalSamples;
      _queue = queue;
    }

    @Override
    public void run() {
      while (true) {
        try {
          ConsumerRecords<byte[], byte[]> consumerRecords = _queue.take();
          if (consumerRecords == SHUTDOWN_RECORDS) {
            return;
          }
          Set<PartitionMetricSample> partitionMetricSamples = new HashSet<>();
          Set<BrokerMetricSample> brokerMetricSamples = new HashSet<>();
          for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
            try {
              if (record.topic().equals(_partitionMetricSampleStoreTopic)) {
                PartitionMetricSample sample = PartitionMetricSample.fromBytes(record.value());
                partitionMetricSamples.add(sample);
                _numPartitionMetricSamples.incrementAndGet();
                LOG.trace("Loaded partition metric sample {}", sample);
              } else if (record.topic().equals(_brokerMetricSampleStoreTopic)) {
                BrokerMetricSample sample = BrokerMetricSample.fromBytes(record.value());
                brokerMetricSamples.add(sample);
                _numBrokerMetricSamples.incrementAndGet();
                LOG.trace("Loaded broker metric sample {}", sample);
              }
            } catch (UnknownVersionException e) {
              LOG.warn("Ignoring sample due to", e);
            }
          }
          _sampleLoader.loadSamples(new MetricSampler.Samples(partitionMetricSamples, brokerMetricSamples));
          _loadingProgress = (double) _numLoadedSamples.addAndGet(consumerRecords.count()) / _totalSamples;
        } catch (InterruptedException ie) {
          if (_shutdown && _queue.isEmpty()) {
            return;
          }
        }
      }
    }
  }
}
