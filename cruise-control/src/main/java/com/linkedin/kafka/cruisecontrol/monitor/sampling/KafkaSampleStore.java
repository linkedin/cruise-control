/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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
  public static final String NUM_SAMPLE_LOADING_THREADS = "num.sample.loading.threads";
  protected static final String PRODUCER_CLIENT_ID = "KafkaCruiseControlSampleStoreProducer";
  protected static final String CONSUMER_CLIENT_ID = "KafkaCruiseControlSampleStoreConsumer";
  // Keep additional snapshot windows in case some of the windows do not have enough samples.
  private static final int ADDITIONAL_SNAPSHOT_WINDOW_TO_RETAIN_FACTOR = 2;
  private static final ConsumerRecords<byte[], byte[]> SHUTDOWN_RECORDS = new ConsumerRecords<>(Collections.emptyMap());
  protected static final Random RANDOM = new Random();
  protected List<KafkaConsumer<byte[], byte[]>> _consumers;
  protected ExecutorService _metricProcessorExecutor;
  protected String _partitionMetricSampleStoreTopic;
  protected String _brokerMetricSampleStoreTopic;
  protected volatile double _loadingProgress;
  protected Producer<byte[], byte[]> _producer;
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
    String numProcessingThreadsString = (String) config.get(NUM_SAMPLE_LOADING_THREADS);
    int numProcessingThreads = numProcessingThreadsString == null || numProcessingThreadsString.isEmpty() ?
        8 : Integer.parseInt(numProcessingThreadsString);
    _metricProcessorExecutor = Executors.newFixedThreadPool(numProcessingThreads);
    _consumers = new ArrayList<>(numProcessingThreads);
    for (int i = 0; i < numProcessingThreads; i++) {
      _consumers.add(createConsumers(config));
    }
    Properties producerProps = new Properties();
    producerProps.putAll(config);
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
    _producer = new KafkaProducer<>(producerProps);

    _loadingProgress = -1.0;

    ensureTopicCreated(config);
  }

  protected KafkaConsumer<byte[], byte[]> createConsumers(Map<String, ?> config) {
      Properties consumerProps = new Properties();
      consumerProps.putAll(config);
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
      return new KafkaConsumer<>(consumerProps);
  }

  private void ensureTopicCreated(Map<String, ?> config) {
    ZkUtils zkUtils = createZkUtils(config);
    Map<String, List<PartitionInfo>> topics = _consumers.get(0).listTopics();
    long snapshotWindowMs = Long.parseLong((String) config.get(KafkaCruiseControlConfig.LOAD_SNAPSHOT_WINDOW_MS_CONFIG));
    int numSnapshotWindows = Integer.parseInt((String) config.get(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG));
    long retentionMs = (numSnapshotWindows * ADDITIONAL_SNAPSHOT_WINDOW_TO_RETAIN_FACTOR) * snapshotWindowMs;
    Properties props = new Properties();
    props.setProperty(LogConfig.RetentionMsProp(), Long.toString(retentionMs));
    int replicationFactor = Math.min(2, zkUtils.getAllBrokersInCluster().size());
    if (!topics.containsKey(_partitionMetricSampleStoreTopic)) {
      AdminUtils.createTopic(zkUtils, _partitionMetricSampleStoreTopic, 32, replicationFactor, props, RackAwareMode.Safe$.MODULE$);
    } else {
      AdminUtils.changeTopicConfig(zkUtils, _partitionMetricSampleStoreTopic, props);
    }

    if (!topics.containsKey(_brokerMetricSampleStoreTopic)) {
      AdminUtils.createTopic(zkUtils, _brokerMetricSampleStoreTopic, 32, replicationFactor, props, RackAwareMode.Safe$.MODULE$);
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
    AtomicLong totalSamples = new AtomicLong(0L);
    AtomicLong numLoadedSamples = new AtomicLong(0L);
    try {
      prepareConsumers();

      for (KafkaConsumer<byte[], byte[]> consumer : _consumers) {
        _metricProcessorExecutor.submit(
            new MetricLoader(consumer, sampleLoader, numLoadedSamples, numPartitionMetricSamples, numBrokerMetricSamples,
                             totalSamples));
      }
      // Blocking waiting for the metric loading to finish.
      _metricProcessorExecutor.shutdown();
      _metricProcessorExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOG.error("Received exception when loading samples", e);
    } finally {
      _consumers.forEach(Consumer::close);
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

  private void prepareConsumers() {
    int numConsumers = _consumers.size();
    List<List<TopicPartition>> assignments = new ArrayList<>();
    for (int i = 0; i < numConsumers; i++) {
      assignments.add(new ArrayList<>());
    }
    int j = 0;
    for (String topic : Arrays.asList(_partitionMetricSampleStoreTopic, _brokerMetricSampleStoreTopic)) {
      for (PartitionInfo partInfo : _consumers.get(0).partitionsFor(topic)) {
        assignments.get(j++ % numConsumers).add(new TopicPartition(partInfo.topic(), partInfo.partition()));
      }
    }
    for (int i = 0; i < numConsumers; i++) {
      _consumers.get(i).assign(assignments.get(i));
      _consumers.get(i).seekToBeginning(assignments.get(i));
    }
  }

  private class MetricLoader implements Runnable {
    private final SampleLoader _sampleLoader;
    private final AtomicLong _numLoadedSamples;
    private final AtomicLong _numPartitionMetricSamples;
    private final AtomicLong _numBrokerMetricSamples;
    private final AtomicLong _totalSamples;
    private final KafkaConsumer<byte[], byte[]> _consumer;

    MetricLoader(KafkaConsumer<byte[], byte[]> consumer,
                 SampleLoader sampleLoader,
                 AtomicLong numLoadedSamples,
                 AtomicLong numPartitionMetricSamples,
                 AtomicLong numBrokerMetricSamples,
                 AtomicLong totalSamples) {
      _consumer = consumer;
      _sampleLoader = sampleLoader;
      _numLoadedSamples = numLoadedSamples;
      _numPartitionMetricSamples = numPartitionMetricSamples;
      _numBrokerMetricSamples = numBrokerMetricSamples;
      _totalSamples = totalSamples;
    }

    @Override
    public void run() {
      try {
        Map<TopicPartition, Long> beginningOffsets = _consumer.beginningOffsets(_consumer.assignment());
        Map<TopicPartition, Long> endOffsets = _consumer.endOffsets(_consumer.assignment());
        LOG.debug("Loading beginning offsets: {}, loading end offsets: {}", beginningOffsets, endOffsets);
        for (Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
          _totalSamples.addAndGet(endOffsets.get(entry.getKey()) - entry.getValue());
          _loadingProgress = (double) _numLoadedSamples.get() / _totalSamples.get();
        }
        while (!sampleLoadingFinished(endOffsets)) {
          try {
            ConsumerRecords<byte[], byte[]> consumerRecords = _consumer.poll(10);
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
            _loadingProgress = (double) _numLoadedSamples.addAndGet(consumerRecords.count()) / _totalSamples.get();
          } catch (Exception e) {
            if (_shutdown) {
              return;
            }
          }
        }
      } catch (Throwable t) {
        LOG.warn("Encountered error when loading sample from Kafka.", t);
      }
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
  }
}
