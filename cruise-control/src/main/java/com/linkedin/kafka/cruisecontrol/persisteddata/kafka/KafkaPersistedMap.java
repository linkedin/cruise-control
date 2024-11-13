/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata.kafka;


import com.google.common.base.Suppliers;
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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores persistent map data in a compacted Kafka topic. The topic is treated like an event store
 * so any other writes to the topic are dynamically picked up and made available for reading.
 */
public class KafkaPersistedMap extends PersistedMap implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPersistedMap.class);

    // The configured data storage kafka topic name.
    private final String _topic;

    // The number of partitions that the data storage kafka topic should have. This can be increased
    // if additional performance is necessary but can't be reduced without data loss.
    private final int _topicPartitions;

    // The configured number of brokers should host each partition of the data storage kafka topic.
    private final short _topicReplicationFactor;

    // Topic configuration configured by an administrator.
    private final Map<String, Object> _topicConfig;

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
                config.originalsWithPrefix(PersistedDataConfig.KAFKA_TOPIC_CONFIG_PREFIX),
                config.originalsWithPrefix(PersistedDataConfig.KAFKA_PRODUCER_CONFIG_PREFIX),
                config.originalsWithPrefix(PersistedDataConfig.KAFKA_CONSUMER_CONFIG_PREFIX),
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
     * @param topicConfigs The configuration to apply to the kafka topic.
     * @param producerConfigs The producer configuration to use.
     * @param consumerConfigs The consumer configuration to use.
     * @param commonSecurityConfigs The security configuration to use for the producer and consumer.
     * e.g. client SSL options.
     * @param bootstrapServers bootstrap.servers configuration to use for the producer and
     * consumer.
     * @param adminClient Kafka AdminClient used to create the backing Kafka topic.
     */
    public KafkaPersistedMap(String topic,
            int topicPartitions,
            short topicReplicationFactor,
            Map<String, Object> topicConfigs,
            Map<String, Object> producerConfigs,
            Map<String, Object> consumerConfigs,
            Map<String, Object> commonSecurityConfigs,
            String bootstrapServers,
            AdminClient adminClient) {
        this(new ConcurrentHashMap<>(), topic, topicPartitions, topicReplicationFactor,
                topicConfigs,
                newTopic -> KafkaCruiseControlUtils.maybeCreateOrUpdateTopic(adminClient, newTopic),
                () -> createKafkaProducer(bootstrapServers, producerConfigs, commonSecurityConfigs),
                () -> createKafkaConsumer(bootstrapServers, consumerConfigs,
                        commonSecurityConfigs));
    }

    /**
     * Package private for testing.
     */
    KafkaPersistedMap(Map<String, String> child,
            String topic,
            int topicPartitions,
            short topicReplicationFactor,
            Map<String, Object> topicConfigs,
            java.util.function.Consumer<NewTopic> topicCreator,
            Supplier<Producer<String, String>> producer,
            Supplier<Consumer<String, String>> consumerFactory) {
        super(child);
        this._topic = topic;
        this._topicPartitions = topicPartitions;
        this._topicReplicationFactor = topicReplicationFactor;
        this._topicConfig = topicConfigs;
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
     * @param <C> The type of Kafka client to return.
     * @param bootstrapServers The configured {@code bootstrap.servers} config to use.
     * @param securityConfig The security options the client should use when connecting to the Kafka
     * cluster.
     * @param clientConfig Any config to use. This will override any keys provided in
     * securityConfig.
     * @param clientFactory Function that takes a config map and returns the Kafka client instance.
     * @return A fully configured Kafka client instance.
     */
    static <C> C createKafkaClient(String bootstrapServers,
            Map<String, ?> securityConfig, Map<String, ?> clientConfig,
            Function<Map<String, Object>, C> clientFactory) {
        // Configure the client by combining the security and client config.
        Map<String, Object> config = new HashMap<>(securityConfig);
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.putAll(clientConfig);
        LOG.debug("KafkaPersistedMap.createKafkaClient(bootstrapServers={}, securityConfig={}, "
                        + "clientConfig={}, clientFactory={})", bootstrapServers,
                securityConfig, clientConfig, clientFactory);
        return clientFactory.apply(config);
    }

    /**
     * Configure and create the KafkaConsumer instance.
     *
     * @param bootstrapServers The configured {@code bootstrap.servers} config to use.
     * @param consumerConfig Any {@link KafkaConsumer} configs to use to configure the consumer.
     * @param securityConfig The kafka client security configuration options.
     * @return The {@link Consumer} instance.
     */
    private static Consumer<String, String> createKafkaConsumer(String bootstrapServers,
            Map<String, Object> consumerConfig, Map<String, Object> securityConfig) {
        return createKafkaClient(bootstrapServers, securityConfig, consumerConfig,
                KafkaConsumer::new);
    }

    /**
     * Configure and create the KafkaProducer instance.
     *
     * @param bootstrapServers The configured {@code bootstrap.servers} config to use.
     * @param producerConfig Any {@link KafkaProducer} configs to use to configure the producer.
     * @param securityConfig The kafka client security configuration options.
     * @return The {@link Producer} instance.
     */
    private static Producer<String, String> createKafkaProducer(String bootstrapServers,
            Map<String, Object> producerConfig, Map<String, Object> securityConfig) {
        return createKafkaClient(bootstrapServers, securityConfig, producerConfig,
                KafkaProducer::new);
    }

    /**
     * Create the topic if it isn't present, or update the topic config if it is present.
     *
     * @param topicCreator Used to create the topic. This uses a functional interface to make
     * testing easier to mock.
     */
    private void ensureTopicIsPresentAndConfigured(
            java.util.function.Consumer<NewTopic> topicCreator) {
        Map<String, String> config = new HashMap<>(this._topicConfig.size());
        this._topicConfig.forEach((key, value) -> config.put(key, value.toString()));
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
