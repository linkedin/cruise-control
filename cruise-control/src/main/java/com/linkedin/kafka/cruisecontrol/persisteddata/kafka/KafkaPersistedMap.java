/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata.kafka;


import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.PersistedDataConfig;
import com.linkedin.kafka.cruisecontrol.persisteddata.PersistedMap;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores persistent map data in a compacted Kafka topic. The topic is treated like an event store
 * so any other writes to the topic are dynamically picked up and made available for reading.
 */
public class KafkaPersistedMap extends PersistedMap implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPersistedMap.class);

    // Keys and values are stored as strings within the kafka topic.
    private static final Class<StringSerializer> SERIALIZER_CLASS = StringSerializer.class;
    private static final Class<StringDeserializer> DESERIALIZER_CLASS = StringDeserializer.class;

    // The hard-coded producer config. This is overridable.
    private static final Map<String, String> DEFAULT_PRODUCER_CONFIG = ImmutableMap.<String, String>builder()
            .put(ProducerConfig.ACKS_CONFIG, "all")
            // 2MB
            .put(ProducerConfig.BUFFER_MEMORY_CONFIG, "2000000")
            .put(ProducerConfig.CLIENT_ID_CONFIG,
                    "kafka-cruise-control.kafka-persisted-map.producer")
            .put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip")
            // 1 hour
            .put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "3600000")
            .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SERIALIZER_CLASS.getName())
            .put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "2")
            // 1MB
            .put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1000000")
            .put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, "1000")
            .put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000")
            .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SERIALIZER_CLASS.getName())
            .build();

    // The hard-coded consumer config. This is overridable.
    private static final Map<String, String> DEFAULT_CONSUMER_CONFIG = ImmutableMap.<String, String>builder()
            .put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false")
            .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .put(ConsumerConfig.CLIENT_ID_CONFIG,
                    "kafka-cruise-control.kafka-persisted-map.consumer")
            .put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            .put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
            .put(ConsumerConfig.GROUP_ID_CONFIG,
                    "kafka-cruise-control.kafka-persisted-map.consumer-group")
            .put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
            .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DESERIALIZER_CLASS.getName())
            .put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(Integer.MAX_VALUE))
            .put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "1000")
            .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DESERIALIZER_CLASS.getName())
            .build();

    // The configuration which must be set on the data storage kafka topic.
    private static final Map<String, String> REQUIRED_TOPIC_CONFIG = Map.of(
            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);

    // The configured data storage kafka topic name.
    private final String _topic;

    // The number of partitions that the data storage kafka topic should have. This can be increased
    // if additional performance is necessary but can't be reduced without data loss.
    private final int _topicPartitions;

    // The configured number of brokers should host each partition of the data storage kafka topic.
    private final short _topicReplicationFactor;

    // Additional topic configuration configured by an administrator.
    private final Map<String, String> _topicAdditionalConfigs;

    // The producer instance to send data updates to kafka.
    private final Supplier<Producer<String, String>> _producer;

    // The consumer instance to read updates from the kafka topic.
    private final Supplier<Consumer<String, String>> _consumerFactory;

    // True while the background consumer thread is active. When it is false, the consumer thread
    // will terminate.
    private volatile boolean _active = true;

    // Holds a reference to the background thread.
    private final Thread _cacheUpdater;

    /**
     * Reads and writes data in a kafka topic and keeps an eventually consistent view of the
     * persisted data.
     */
    public KafkaPersistedMap(KafkaCruiseControlConfig config, AdminClient adminClient) {
        this(config.getString(PersistedDataConfig.KAFKA_TOPIC_NAME_CONFIG),
                config.getInt(PersistedDataConfig.KAFKA_TOPIC_PARTITION_COUNT_CONFIG),
                config.getShort(PersistedDataConfig.KAFKA_TOPIC_REPLICATION_FACTOR_CONFIG),
                config.getMap(PersistedDataConfig.KAFKA_TOPIC_ADDITIONAL_CONFIGS_MAP_CONFIG),
                config.getMap(PersistedDataConfig.KAFKA_PRODUCER_ADDITIONAL_CONFIGS_MAP_CONFIG),
                config.getMap(PersistedDataConfig.KAFKA_CONSUMER_ADDITIONAL_CONFIGS_MAP_CONFIG),
                KafkaCruiseControlUtils.maybeAddSecurityConfig(config, new HashMap<>()),
                String.join(",", config.getList(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG)),
                adminClient);
    }

    /**
     * Reads and writes data in the kafka topic and keeps an eventually consistent view of the
     * persisted data.
     *
     * @param topic The topic name to use and to ensure exists.
     * @param topicPartitions The number of partitions to ensure the topic has.
     * @param topicReplicationFactor Number of partition replicas to use for the topic.
     * @param topicAdditionalConfigs The additional topic configuration to apply to the topic.
     * @param producerAdditionalConfigs The additional producer configuration to use.
     * @param consumerAdditionalConfigs The additional consumer configuration to use.
     * @param commonSecurityConfigs The security configuration to use for the producer and consumer.
     * e.g. client SSL options.
     * @param bootstrapServers bootstrap.servers configuration to use for the producer and
     * consumer.
     * @param adminClient Kafka AdminClient used to create the backing Kafka topic.
     */
    public KafkaPersistedMap(String topic,
            int topicPartitions,
            short topicReplicationFactor,
            Map<String, String> topicAdditionalConfigs,
            Map<String, String> producerAdditionalConfigs,
            Map<String, String> consumerAdditionalConfigs,
            Map<String, Object> commonSecurityConfigs,
            String bootstrapServers,
            AdminClient adminClient) {
        this(new ConcurrentHashMap<>(), topic, topicPartitions, topicReplicationFactor,
                topicAdditionalConfigs,
                newTopic -> KafkaCruiseControlUtils.maybeCreateOrUpdateTopic(adminClient, newTopic),
                () -> createKafkaProducer(bootstrapServers, producerAdditionalConfigs,
                        commonSecurityConfigs),
                () -> createKafkaConsumer(bootstrapServers, consumerAdditionalConfigs,
                        commonSecurityConfigs));
    }

    /**
     * Package private for testing.
     */
    KafkaPersistedMap(Map<String, String> child,
            String topic,
            int topicPartitions,
            short topicReplicationFactor,
            Map<String, String> topicAdditionalConfigs,
            java.util.function.Consumer<NewTopic> topicCreator,
            Supplier<Producer<String, String>> producer,
            Supplier<Consumer<String, String>> consumerFactory) {
        super(child);
        this._topic = topic;
        this._topicPartitions = topicPartitions;
        this._topicReplicationFactor = topicReplicationFactor;
        this._topicAdditionalConfigs = topicAdditionalConfigs;
        ensureTopicIsPresentAndConfigured(topicCreator);

        // This odd setup is to lazy-load the producer and also adapt the java Supplier to a
        // guava Supplier.
        this._producer = Suppliers.memoize(producer::get);
        this._consumerFactory = consumerFactory;

        this._cacheUpdater = Executors.defaultThreadFactory()
                .newThread(this::consumeAndUpdateCache);
        this._cacheUpdater.start();
    }

    /**
     * Returns the background thread. Package private for testing.
     *
     * @return the background thread that updates the cache.
     */
    Thread getCacheUpdater() {
        return _cacheUpdater;
    }

    /**
     * Configures and creates the requested Kafka client instance. Package private for testing.
     *
     * @param bootstrapServers The configured {@code bootstrap.servers} config to use.
     * @param defaultConfig The default config values to configure the client with.
     * @param securityConfig The security options the client should use when connecting to the Kafka
     * cluster.
     * @param additionalConfig Any additional config to override the default and security config
     * with.
     * @param clientFactory Function that takes a config map and returns the Kafka client instance.
     * @param <C> The type of Kafka client to return.
     * @return A fully configured Kafka client instance.
     */
    static <C> C createKafkaClient(String bootstrapServers, Map<String, ?> defaultConfig,
            Map<String, ?> securityConfig, Map<String, ?> additionalConfig,
            Function<Map<String, Object>, C> clientFactory) {
        // Configure the client by combining the default, security and additional config.
        Map<String, Object> config = mergeConfig(defaultConfig, securityConfig);
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.putAll(additionalConfig);
        LOG.debug("KafkaPersistedMap.createKafkaClient(bootstrapServers={}, defaultConfig={}, "
                        + "securityConfig={}, additionalConfig={}, clientFactory={})", bootstrapServers,
                defaultConfig, securityConfig, additionalConfig, clientFactory);
        return clientFactory.apply(config);
    }

    /**
     * Configure and create the KafkaConsumer instance.
     *
     * @param bootstrapServers The configured {@code bootstrap.servers} config to use.
     * @param consumerAdditionalConfigs Any additional {@link KafkaConsumer} configs to use to
     * override the default consumer config.
     * @param commonSecurityConfigs The kafka client security configuration options.
     * @return The {@link Consumer} instance.
     */
    private static Consumer<String, String> createKafkaConsumer(String bootstrapServers,
            Map<String, String> consumerAdditionalConfigs,
            Map<String, Object> commonSecurityConfigs) {
        return createKafkaClient(bootstrapServers, DEFAULT_CONSUMER_CONFIG, commonSecurityConfigs,
                consumerAdditionalConfigs, KafkaConsumer::new);
    }

    /**
     * Configure and create the KafkaProducer instance.
     *
     * @param bootstrapServers The configured {@code bootstrap.servers} config to use.
     * @param producerAdditionalConfigs Any additional {@link KafkaProducer} configs to use to
     * override the default producer config.
     * @param commonSecurityConfigs The kafka client security configuration options.
     * @return The {@link Producer} instance.
     */
    private static Producer<String, String> createKafkaProducer(String bootstrapServers,
            Map<String, String> producerAdditionalConfigs,
            Map<String, Object> commonSecurityConfigs) {
        return createKafkaClient(bootstrapServers, DEFAULT_PRODUCER_CONFIG, commonSecurityConfigs,
                producerAdditionalConfigs, KafkaProducer::new);
    }

    /**
     * Merge the two maps into a new one overwriting any default values with those from overrides.
     *
     * @param defaults The base key/values to overwrite if necessary.
     * @param overrides Any key/values to merge over the values in defaults.
     * @param <DEFAULTSVALUES> The defaults map value type.
     * @param <OVERRIDESVALUES> The overrides map value type.
     * @return The combined map of key/values.
     */
    private static <DEFAULTSVALUES, OVERRIDESVALUES> Map<String, Object> mergeConfig(
            Map<String, DEFAULTSVALUES> defaults,
            Map<String, OVERRIDESVALUES> overrides) {
        final Map<String, Object> result = new HashMap<>(defaults);
        result.putAll(overrides);
        return result;
    }

    /**
     * Create the topic if it isn't present, or update the topic config if it is present.
     *
     * @param topicCreator Used to create the topic. This uses a functional interface to make
     * testing easier to mock.
     */
    private void ensureTopicIsPresentAndConfigured(
            java.util.function.Consumer<NewTopic> topicCreator) {
        Map<String, String> config = new HashMap<>(this._topicAdditionalConfigs);
        config.putAll(REQUIRED_TOPIC_CONFIG);
        NewTopic topic = new NewTopic(this._topic, this._topicPartitions,
                this._topicReplicationFactor);
        topic.configs(config);
        topicCreator.accept(topic);
    }

    /**
     * Keep updating the cached aggregate data in a background thread.
     */
    private void consumeAndUpdateCache() {
        try (Consumer<String, String> consumer = this._consumerFactory.get()) {
            if (this._active) {
                // Start the consumer's subscription to the topic.
                consumer.subscribe(Collections.singleton(this._topic));

                // Keep polling for updates. Ignore committing offsets for compacted topic.
                while (this._active) {
                    try {
                        ConsumerRecords<String, String> records = consumer.poll(
                                Duration.ofSeconds(10));
                        records.forEach(record -> {
                            this._child.put(record.key(), record.value());
                            LOG.debug("Retrieved record: key={}, value={}",
                                    record.key(), record.value());
                        });
                    } catch (Exception e) {
                        LOG.warn(String.format("Error while consuming records from Kafka topic=%s",
                                this._topic), e);
                    }
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        // Signal that the consumer poll loop should stop.
        this._active = false;
    }

    @Override
    public String put(String key, String value) {
        String oldValue = this.get(key);
        ProducerRecord<String, String> record = new ProducerRecord<>(this._topic, key, value);
        final Future<RecordMetadata> future = this._producer.get().send(record);
        try {
            future.get();
            LOG.debug("Saved record: key={}, value={}", key, value);
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaPersistedMapException(
                    String.format("Failed to save data to Kafka: key=%s, value=%s", key, value), e);
        }
        return oldValue;
    }
}
