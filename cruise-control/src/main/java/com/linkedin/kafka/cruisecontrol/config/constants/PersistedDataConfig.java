/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.config.constants;

import com.linkedin.kafka.cruisecontrol.persisteddata.PersistMethod;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;


/**
 * A class to keep Cruise Control persisted data configs and defaults.
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public final class PersistedDataConfig {

    public static final String CONFIG_PREFIX = "persisted.data.";
    public static final String KAFKA_PREFIX = CONFIG_PREFIX + "kafka.";
    public static final String KAFKA_TOPIC_PREFIX = KAFKA_PREFIX + "topic.";

    /**
     * <code>persisted.data.persist.method</code>
     */
    public static final String PERSIST_METHOD_CONFIG = CONFIG_PREFIX + "persist.method";
    public static final String DEFAULT_PERSIST_METHOD = "memory";
    public static final String PERSIST_METHOD_DOC =
            "The method to use to store persisted data. This is the first "
                    + "\"persisted.data\" config to set which will determine which other configs "
                    + "of the series should be configured. The available options are: "
                    + PersistMethod.stringValues().toString()
                    + ". The default is \"" + DEFAULT_PERSIST_METHOD + "\", which doesn't durably "
                    + "persist any runtime data.";

    /**
     * <code>persisted.data.kafka.topic.name</code>
     */
    public static final String KAFKA_TOPIC_NAME_CONFIG = KAFKA_TOPIC_PREFIX + "name";
    public static final String DEFAULT_KAFKA_TOPIC_NAME = "__CruiseControlPersistentData";
    public static final String KAFKA_TOPIC_NAME_DOC =
            "The name of the kafka topic to use to persist data when " + "\""
                    + PERSIST_METHOD_CONFIG + "\" is set to \"kafka\". If the topic is not "
                    + "present, then it will be created.";

    /**
     * <code>persisted.data.kafka.topic.partition.count</code>
     */
    public static final String KAFKA_TOPIC_PARTITION_COUNT_CONFIG =
            KAFKA_TOPIC_PREFIX + "partition.count";
    public static final int DEFAULT_KAFKA_TOPIC_PARTITION_COUNT = 1;
    public static final String KAFKA_TOPIC_PARTITION_COUNT_DOC =
            "The number of partitions to ensure are present "
                    + "for the kafka topic. Only applies when \"" + PERSIST_METHOD_CONFIG
                    + "\" is set to \"kafka\". If the topic has fewer than this number of "
                    + "partitions, then partitions will be added.";

    /**
     * <code>persisted.data.kafka.topic.replication.factor</code>
     */
    public static final String KAFKA_TOPIC_REPLICATION_FACTOR_CONFIG =
            KAFKA_TOPIC_PREFIX + "replication.factor";
    public static final short DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR = 3;
    public static final String KAFKA_TOPIC_REPLICATION_FACTOR_DOC =
            "The replication factor to use for the kafka " + "topic. Only applies when \""
                    + PERSIST_METHOD_CONFIG + "\" is set to \"kafka\". Multiple partition "
                    + "replicas are desirable to ensure the topic is reasonably available.";

    /**
     * <code>persisted.data.kafka.topic.config.*</code>
     * <br/>
     * The configs to apply to the kafka topic used to persist Cruise Control data. Only applies if
     * "persisted.data.persist.method=kafka". All valid topic config keys and values can be used to
     * apply to the "persisted.data.kafka.topic.name" topic. Use this prefix string with the topic
     * config keys. e.g. "persisted.data.kafka.topic.config.min.insync.replicas=2" See: <a
     * href="https://kafka.apache.org/documentation/#topicconfigs">Kafka topic config
     * documentation</a>.
     */
    public static final String KAFKA_TOPIC_CONFIG_PREFIX = KAFKA_TOPIC_PREFIX + "config.";

    /**
     * <code>persisted.data.kafka.producer.config.</code>
     * <br/>
     * The configs to use when creating the kafka producer to persist Cruise Control data. Only
     * applies if "persisted.data.persist.method=kafka". The keys and values need to be valid Kafka
     * Producer configs. All valid <a
     * href="https://kafka.apache.org/documentation/#producerconfigs">Kafka producer config</a> keys
     * can be prefixed, and they will be applied to the producer. e.g.
     * "persisted.data.kafka.producer.config.compression.type=gzip"
     */
    public static final String KAFKA_PRODUCER_CONFIG_PREFIX = KAFKA_PREFIX + "producer.config.";

    /**
     * <code>persisted.data.kafka.consumer.config.prefix.</code>
     * <br/>
     * The configs to use when creating the kafka consumer to read persisted Cruise Control data.
     * Only applies if "persisted.data.persist.method=kafka". The keys and values need to be valid
     * Kafka Consumer configs. All valid <a
     * href="https://kafka.apache.org/documentation/#consumerconfigs">Kafka consumer config</a> keys
     * can be prefixed, and they will be applied to the consumer. e.g.
     * "persisted.data.kafka.consumer.config.max.poll.records=100"
     */
    public static final String KAFKA_CONSUMER_CONFIG_PREFIX = KAFKA_PREFIX + "consumer.config.";

    private PersistedDataConfig() {
    }

    /**
     * Define persisted data configs.
     *
     * @param configDef Config definition.
     * @return The given ConfigDef after defining the common configs.
     */
    public static ConfigDef define(ConfigDef configDef) {
        configDef.embed(KAFKA_TOPIC_CONFIG_PREFIX, "", -1, TopicConfigDef.configDef());
        configDef.embed(KAFKA_PRODUCER_CONFIG_PREFIX, "", -1, ProducerConfig.configDef());
        configDef.embed(KAFKA_CONSUMER_CONFIG_PREFIX, "", -1, ConsumerConfig.configDef());

        return configDef
                .define(PERSIST_METHOD_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_PERSIST_METHOD,
                        ConfigDef.Importance.MEDIUM,
                        PERSIST_METHOD_DOC)
                .define(KAFKA_TOPIC_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_KAFKA_TOPIC_NAME,
                        ConfigDef.Importance.LOW,
                        KAFKA_TOPIC_NAME_DOC)
                .define(KAFKA_TOPIC_PARTITION_COUNT_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_KAFKA_TOPIC_PARTITION_COUNT,
                        atLeast(1),
                        ConfigDef.Importance.LOW,
                        KAFKA_TOPIC_PARTITION_COUNT_DOC)
                .define(KAFKA_TOPIC_REPLICATION_FACTOR_CONFIG,
                        ConfigDef.Type.SHORT,
                        DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR,
                        atLeast(1),
                        ConfigDef.Importance.MEDIUM,
                        KAFKA_TOPIC_REPLICATION_FACTOR_DOC);
    }
}
