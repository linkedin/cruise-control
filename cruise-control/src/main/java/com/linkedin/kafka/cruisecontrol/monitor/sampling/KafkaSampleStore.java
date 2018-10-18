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
import java.util.HashMap;
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
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;


/**
 * The sample store that implements the {@link SampleStore}. It stores the partition metric samples and broker metric
 * samples back to Kafka and load from Kafka at startup.
 *
 * Required configurations for this class.
 * <ul>
 *   <li>{@link #PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG}: The config for the topic name of Kafka topic to store partition samples.</li>
 *   <li>{@link #BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG}: The config for the topic name of Kafka topic to store broker samples.</li>
 *   <li>{@link #NUM_SAMPLE_LOADING_THREADS_CONFIG}: The config for the number of Kafka sample store consumer threads, default value is
 *   set to {@link #DEFAULT_NUM_SAMPLE_LOADING_THREADS}.</li>
 *   <li>{@link #SAMPLE_STORE_TOPIC_REPLICATION_FACTOR_CONFIG}: The config for the replication factor of Kafka sample store topics,
 *   default value is set to {@link #DEFAULT_SAMPLE_STORE_TOPIC_REPLICATION_FACTOR}.</li>
 *   <li>{@link #PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG}: The config for the number of partition for Kafka partition sample store
 *    topic, default value is set to {@link #DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT}.</li>
 *   <li>{@link #BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG}: The config for the number of partition for Kafka broker sample store topic,
 *   default value is set to {@link #DEFAULT_BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT}.</li>
 *   <li>{@link #MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG}: The config for the minimal retention time for Kafka partition sample
 *   store topic, default value is set to {@link #DEFAULT_MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS}.</li>
 *   <li>{@link #MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG}: The config for the minimal retention time for Kafka broker sample store
 *   topic, default value is set to {@link #DEFAULT_MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS}.</li>
 * </ul>
 */
public class KafkaSampleStore implements SampleStore {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSampleStore.class);
  private static final String DEFAULT_CLEANUP_POLICY = "delete";
  // Keep additional windows in case some of the windows do not have enough samples.
  private static final int ADDITIONAL_WINDOW_TO_RETAIN_FACTOR = 2;
  private static final ConsumerRecords<byte[], byte[]> SHUTDOWN_RECORDS = new ConsumerRecords<>(Collections.emptyMap());
  private static final long SAMPLE_POLL_TIMEOUT = 1000L;

  protected static final int DEFAULT_NUM_SAMPLE_LOADING_THREADS = 8;
  protected static final int DEFAULT_SAMPLE_STORE_TOPIC_REPLICATION_FACTOR = 2;
  protected static final int DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT = 32;
  protected static final int DEFAULT_BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT = 32;
  protected static final long DEFAULT_MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS = 3600000L;
  protected static final long DEFAULT_MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS = 3600000L;
  protected static final String PRODUCER_CLIENT_ID = "KafkaCruiseControlSampleStoreProducer";
  protected static final String CONSUMER_CLIENT_ID = "KafkaCruiseControlSampleStoreConsumer";
  protected static final Random RANDOM = new Random();
  protected List<KafkaConsumer<byte[], byte[]>> _consumers;
  protected ExecutorService _metricProcessorExecutor;
  protected String _partitionMetricSampleStoreTopic;
  protected String _brokerMetricSampleStoreTopic;
  protected Integer _sampleStoreTopicReplicationFactor;
  protected Integer _partitionSampleStoreTopicPartitionCount;
  protected Integer _brokerSampleStoreTopicPartitionCount;
  protected Long _minPartitionSampleStoreTopicRetentionTimeMs;
  protected Long _minBrokerSampleStoreTopicRetentionTimeMs;
  protected volatile double _loadingProgress;
  protected Producer<byte[], byte[]> _producer;
  protected volatile boolean _shutdown = false;

  public static final String PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG = "partition.metric.sample.store.topic";
  public static final String BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG = "broker.metric.sample.store.topic";
  public static final String NUM_SAMPLE_LOADING_THREADS_CONFIG = "num.sample.loading.threads";
  public static final String SAMPLE_STORE_TOPIC_REPLICATION_FACTOR_CONFIG = "sample.store.topic.replication.factor";
  public static final String PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG = "partition.sample.store.topic.partition.count";
  public static final String BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG = "broker.sample.store.topic.partition.count";
  public static final String MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG = "min.partition.sample.store.topic.retention.time.ms";
  public static final String MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG = "min.broker.sample.store.topic.retention.time.ms";
  @Override
  public void configure(Map<String, ?> config) {
    _partitionMetricSampleStoreTopic = KafkaCruiseControlUtils.getRequiredConfig(config, PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG);
    _brokerMetricSampleStoreTopic = KafkaCruiseControlUtils.getRequiredConfig(config, BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG);
    String metricSampleStoreTopicReplicationFactorString = (String) config.get(SAMPLE_STORE_TOPIC_REPLICATION_FACTOR_CONFIG);
    _sampleStoreTopicReplicationFactor = metricSampleStoreTopicReplicationFactorString == null
                                         || metricSampleStoreTopicReplicationFactorString.isEmpty()
                                         ? null : Integer.parseInt(metricSampleStoreTopicReplicationFactorString);
    String partitionSampleStoreTopicPartitionCountString = (String) config.get(PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG);
    _partitionSampleStoreTopicPartitionCount = partitionSampleStoreTopicPartitionCountString == null
                                               || partitionSampleStoreTopicPartitionCountString.isEmpty()
                                               ? DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT
                                               : Integer.parseInt(partitionSampleStoreTopicPartitionCountString);
    String brokerSampleStoreTopicPartitionCountString = (String) config.get(BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG);
    _brokerSampleStoreTopicPartitionCount = brokerSampleStoreTopicPartitionCountString == null
                                            || brokerSampleStoreTopicPartitionCountString.isEmpty()
                                            ? DEFAULT_BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT
                                            : Integer.parseInt(brokerSampleStoreTopicPartitionCountString);
    String minPartitionSampleStoreTopicRetentionTimeMsString = (String) config.get(MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG);
    _minPartitionSampleStoreTopicRetentionTimeMs = minPartitionSampleStoreTopicRetentionTimeMsString == null
                                                  || minPartitionSampleStoreTopicRetentionTimeMsString.isEmpty()
                                                  ? DEFAULT_MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS :
                                                  Integer.parseInt(minPartitionSampleStoreTopicRetentionTimeMsString);
    String minBrokerSampleStoreTopicRetentionTimeMsString = (String) config.get(MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG);
    _minBrokerSampleStoreTopicRetentionTimeMs = minBrokerSampleStoreTopicRetentionTimeMsString == null
                                               || minBrokerSampleStoreTopicRetentionTimeMsString.isEmpty()
                                               ? DEFAULT_MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS :
                                               Integer.parseInt(minBrokerSampleStoreTopicRetentionTimeMsString);
    String numProcessingThreadsString = (String) config.get(NUM_SAMPLE_LOADING_THREADS_CONFIG);
    int numProcessingThreads = numProcessingThreadsString == null || numProcessingThreadsString.isEmpty()
                               ? DEFAULT_NUM_SAMPLE_LOADING_THREADS : Integer.parseInt(numProcessingThreadsString);
    _metricProcessorExecutor = Executors.newFixedThreadPool(numProcessingThreads);
    _consumers = new ArrayList<>(numProcessingThreads);
    for (int i = 0; i < numProcessingThreads; i++) {
      _consumers.add(createConsumers(config));
    }

    _producer = createProducer(config);
    _loadingProgress = -1.0;

    ensureTopicsCreated(config);
  }

  protected KafkaProducer<byte[], byte[]> createProducer(Map<String, ?> config) {
    Properties producerProps = new Properties();
    producerProps.putAll(config);
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                              (String) config.get(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG));
    producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_CLIENT_ID);
    // Set batch.size and linger.ms to a big number to have better batching.
    producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "30000");
    producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "500000");
    producerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864");
    producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
    producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return new KafkaProducer<>(producerProps);
  }

  protected KafkaConsumer<byte[], byte[]> createConsumers(Map<String, ?> config) {
      Properties consumerProps = new Properties();
      consumerProps.putAll(config);
      long randomToken = RANDOM.nextLong();
      consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                (String) config.get(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG));
      consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "KafkaCruiseControlSampleStore" + randomToken);
      consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_CLIENT_ID + randomToken);
      consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(Integer.MAX_VALUE));
      consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
      consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
      return new KafkaConsumer<>(consumerProps);
  }

  private void ensureTopicsCreated(Map<String, ?> config) {
    ZkUtils zkUtils = createZkUtils(config);
    try {
      Map<String, List<PartitionInfo>> topics = _consumers.get(0).listTopics();
      long partitionSampleWindowMs = Long.parseLong((String) config.get(KafkaCruiseControlConfig.PARTITION_METRICS_WINDOW_MS_CONFIG));
      long brokerSampleWindowMs = Long.parseLong((String) config.get(KafkaCruiseControlConfig.BROKER_METRICS_WINDOW_MS_CONFIG));

      int numPartitionSampleWindows =
          Integer.parseInt((String) config.get(KafkaCruiseControlConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG));
      long partitionSampleRetentionMs = (numPartitionSampleWindows * ADDITIONAL_WINDOW_TO_RETAIN_FACTOR) * partitionSampleWindowMs;
      partitionSampleRetentionMs = Math.max(_minPartitionSampleStoreTopicRetentionTimeMs, partitionSampleRetentionMs);

      int numBrokerSampleWindows =
          Integer.parseInt((String) config.get(KafkaCruiseControlConfig.NUM_BROKER_METRICS_WINDOWS_CONFIG));
      long brokerSampleRetentionMs = (numBrokerSampleWindows * ADDITIONAL_WINDOW_TO_RETAIN_FACTOR) * brokerSampleWindowMs;
      brokerSampleRetentionMs = Math.max(_minBrokerSampleStoreTopicRetentionTimeMs, brokerSampleRetentionMs);

      int numberOfBrokersInCluster = zkUtils.getAllBrokersInCluster().size();
      if (numberOfBrokersInCluster == 0) {
        throw new IllegalStateException(String.format("Kafka cluster has no alive brokers. (zookeeper.connect = %s",
                                                      config.get(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG)));
      }
      int replicationFactor = _sampleStoreTopicReplicationFactor == null ? Math.min(DEFAULT_SAMPLE_STORE_TOPIC_REPLICATION_FACTOR,
                              numberOfBrokersInCluster) : _sampleStoreTopicReplicationFactor;

      ensureTopicCreated(zkUtils, topics.keySet(), _partitionMetricSampleStoreTopic, partitionSampleRetentionMs,
                         replicationFactor, _partitionSampleStoreTopicPartitionCount);
      ensureTopicCreated(zkUtils, topics.keySet(), _brokerMetricSampleStoreTopic, brokerSampleRetentionMs,
                         replicationFactor, _brokerSampleStoreTopicPartitionCount);
    } finally {
      KafkaCruiseControlUtils.closeZkUtilsWithTimeout(zkUtils, 10000);
    }
  }

  private void increaseTopicReplicaFactor(ZkUtils zkUtils, MetadataResponse.TopicMetadata topicMetadata, int replicationFactor,
      String topic, Properties props) {
    List<Integer> brokers = new ArrayList<>();
    JavaConversions.seqAsJavaList(AdminUtils.getBrokerMetadatas(zkUtils, RackAwareMode.Safe$.MODULE$, Option.empty()))
    .stream().forEach(bm -> brokers.add(bm.id()));
    if (replicationFactor > brokers.size()) {
      throw new RuntimeException("Unable to increase topic " + topic + " replica factor to " + replicationFactor +
                                 " since there are only " + brokers.size() + " brokers in the cluster.");
    }
    scala.collection.Map<Object, Seq<Object>> newReplicaAssignment = new scala.collection.mutable.HashMap<>();
    int cursor = 0;
    for (MetadataResponse.PartitionMetadata pm : topicMetadata.partitionMetadata()) {
      List<Object> newAssignedReplica = new ArrayList<>();
      // Make sure the current replicas are in new replica list.
      pm.replicas().forEach(node -> newAssignedReplica.add(node.id()));
      // Add new replica to partition in round-robin way.
      while (newAssignedReplica.size() < replicationFactor) {
        if (!newAssignedReplica.contains(brokers.get(cursor))) {
          newAssignedReplica.add(brokers.get(cursor));
        }
        cursor = (cursor + 1) % brokers.size();
      }
      ((scala.collection.mutable.HashMap<Object, Seq<Object>>) newReplicaAssignment).put(pm.partition(),
          JavaConverters.asScalaIteratorConverter(newAssignedReplica.iterator()).asScala().toSeq());
    }
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, newReplicaAssignment, props, true);
  }

  private void ensureTopicCreated(ZkUtils zkUtils, Set<String> allTopics, String topic, long retentionMs, int replicationFactor, int partitionCount) {
    Properties props = new Properties();
    props.setProperty(LogConfig.RetentionMsProp(), Long.toString(retentionMs));
    props.setProperty(LogConfig.CleanupPolicyProp(), DEFAULT_CLEANUP_POLICY);
    if (!allTopics.contains(topic)) {
      AdminUtils.createTopic(zkUtils, topic, partitionCount, replicationFactor, props, RackAwareMode.Safe$.MODULE$);
    } else {
      AdminUtils.changeTopicConfig(zkUtils, topic, props);
      MetadataResponse.TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils);
      if (replicationFactor > topicMetadata.partitionMetadata().get(0).replicas().size()) {
        increaseTopicReplicaFactor(zkUtils, topicMetadata, replicationFactor, topic, props);
      }
      if (partitionCount > topicMetadata.partitionMetadata().size()) {
        AdminUtils.addPartitions(zkUtils, topic, partitionCount, "", true, RackAwareMode.Safe$.MODULE$);
      }
    }
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
                                     sample.entity().tp(), sample.sampleTime(), e);
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
    long endMs = System.currentTimeMillis();
    long addedPartitionSampleCount = sampleLoader.partitionSampleCount();
    long addedBrokerSampleCount = sampleLoader.brokerSampleCount();
    long discardedPartitionMetricSamples = numPartitionMetricSamples.get() - addedPartitionSampleCount;
    long discardedBrokerMetricSamples = numBrokerMetricSamples.get() - addedBrokerSampleCount;
    LOG.info("Sample loading finished. Loaded {}{} partition metrics samples and {}{} broker metric samples in {} ms.",
             addedPartitionSampleCount,
             discardedPartitionMetricSamples > 0 ? String.format("(%d discarded)", discardedPartitionMetricSamples) : "",
             sampleLoader.brokerSampleCount(),
             discardedBrokerMetricSamples > 0 ? String.format("(%d discarded)", discardedBrokerMetricSamples) : "",
             endMs - startMs);
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
        prepareConsumerOffset();
        Map<TopicPartition, Long> beginningOffsets = _consumer.beginningOffsets(_consumer.assignment());
        Map<TopicPartition, Long> endOffsets = _consumer.endOffsets(_consumer.assignment());
        LOG.debug("Loading beginning offsets: {}, loading end offsets: {}", beginningOffsets, endOffsets);
        for (Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
          _totalSamples.addAndGet(endOffsets.get(entry.getKey()) - entry.getValue());
          _loadingProgress = (double) _numLoadedSamples.get() / _totalSamples.get();
        }
        while (!sampleLoadingFinished(endOffsets)) {
          try {
            ConsumerRecords<byte[], byte[]> consumerRecords = _consumer.poll(SAMPLE_POLL_TIMEOUT);
            if (consumerRecords == SHUTDOWN_RECORDS) {
              LOG.trace("Metric loader received empty records");
              return;
            }
            Set<PartitionMetricSample> partitionMetricSamples = new HashSet<>();
            Set<BrokerMetricSample> brokerMetricSamples = new HashSet<>();
            for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
              try {
                if (record.topic().equals(_partitionMetricSampleStoreTopic)) {
                  PartitionMetricSample sample = PartitionMetricSample.fromBytes(record.value());
                  partitionMetricSamples.add(sample);
                  LOG.trace("Loaded partition metric sample {}", sample);
                } else if (record.topic().equals(_brokerMetricSampleStoreTopic)) {
                  BrokerMetricSample sample = BrokerMetricSample.fromBytes(record.value());
                  // For some legacy BrokerMetricSample, there is no timestamp in the broker samples. In this case
                  // we use the record timestamp as the broker metric timestamp.
                  sample.close(record.timestamp());
                  brokerMetricSamples.add(sample);
                  LOG.trace("Loaded broker metric sample {}", sample);
                }
              } catch (UnknownVersionException e) {
                LOG.warn("Ignoring sample due to", e);
              }
            }
            if (!partitionMetricSamples.isEmpty() || !brokerMetricSamples.isEmpty()) {
              _sampleLoader.loadSamples(new MetricSampler.Samples(partitionMetricSamples, brokerMetricSamples));
              _numPartitionMetricSamples.getAndAdd(partitionMetricSamples.size());
              _numBrokerMetricSamples.getAndAdd(brokerMetricSamples.size());
              _loadingProgress = (double) _numLoadedSamples.addAndGet(consumerRecords.count()) / _totalSamples.get();
            }
          } catch (KafkaException ke) {
            if (ke.getMessage().toLowerCase().contains("record is corrupt")) {
              for (TopicPartition tp : _consumer.assignment()) {
                long position = _consumer.position(tp);
                if (position < endOffsets.get(tp)) {
                  _consumer.seek(tp, position + 1);
                }
              }
            } else {
              LOG.error("Metric loader received exception:", ke);
            }
          } catch (Exception e) {
            if (_shutdown) {
              return;
            } else {
              LOG.error("Metric loader received exception:", e);
            }
          }
        }
        LOG.info("Metric loader finished loading samples.");
      } catch (Throwable t) {
        LOG.warn("Encountered error when loading sample from Kafka.", t);
      }
    }

    private boolean sampleLoadingFinished(Map<TopicPartition, Long> endOffsets) {
      for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
        long position = _consumer.position(entry.getKey());
        if (position < entry.getValue()) {
          LOG.debug("Partition {} is still lagging. Current position: {}, LEO: {}", entry.getKey(),
                    position, entry.getValue());
          return false;
        }
      }
      return true;
    }

    /**
     * Config the sample loading consumers to consume from proper starting offsets. The sample store Kafka topic may contain data
     * which are too old for {@link com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator} to keep in memory,
     * to prevent loading these stale data, manually seek the consumers' staring offset to the offset at proper timestamp.
     */
    private void prepareConsumerOffset() {
      Map<TopicPartition, Long> beginningTimestamp = new HashMap<>(_consumer.assignment().size());
      long currentTimeMs = System.currentTimeMillis();
      for (TopicPartition tp : _consumer.assignment()) {
        if (tp.topic().equals(_brokerMetricSampleStoreTopic)) {
          beginningTimestamp.put(tp, currentTimeMs - _sampleLoader.brokerMonitoringPeriodMs());
        } else {
          beginningTimestamp.put(tp, currentTimeMs - _sampleLoader.partitionMonitoringPeriodMs());
        }
      }

      Set<TopicPartition> partitionWithNoRecentMessage = new HashSet<>();
      Map<TopicPartition, OffsetAndTimestamp> beginningOffsetAndTimestamp = _consumer.offsetsForTimes(beginningTimestamp);
      for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry: beginningOffsetAndTimestamp.entrySet()) {
        if (entry.getValue() == null) {
          // If this sample store topic partition does not have data available after beginning timestamp, then seek to the
          // beginning of this topic partition.
          partitionWithNoRecentMessage.add(entry.getKey());
        } else {
          _consumer.seek(entry.getKey(), entry.getValue().offset());
        }
      }
      if (partitionWithNoRecentMessage.size() > 0) {
        _consumer.seekToBeginning(partitionWithNoRecentMessage);
      }
    }
  }
}
