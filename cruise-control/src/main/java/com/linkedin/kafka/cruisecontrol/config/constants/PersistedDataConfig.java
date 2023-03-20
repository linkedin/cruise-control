/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.config.constants;

import com.linkedin.kafka.cruisecontrol.persisteddata.BackingMethod;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;


/**
 * A class to keep Cruise Control persisted data configs and defaults.
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public final class PersistedDataConfig {

    public static final String CONFIG_PREFIX = "persisted.data.";

    /**
     * <code>persisted.data.backing.method</code>
     */
    public static final String BACKING_METHOD_CONFIG = CONFIG_PREFIX + "backing.method";
    public static final String DEFAULT_BACKING_METHOD = "memory";
    public static final String BACKING_METHOD_DOC =
            "The method to use to store persisted data. This is the first "
                    + "\"persisted.data\" config to set which will determine which other configs "
                    + "of the series should be configured. The available options are: "
                    + BackingMethod.stringValues().toString()
                    + ". The default is \"" + DEFAULT_BACKING_METHOD + "\", which doesn't durably "
                    + "persist any runtime data.";

    /**
     * <code>persisted.data.kafka.topic.name</code>
     */
    public static final String KAFKA_TOPIC_NAME_CONFIG = CONFIG_PREFIX + "kafka.topic.name";
    public static final String DEFAULT_KAFKA_TOPIC_NAME = "__CruiseControlPersistentData";
    public static final String KAFKA_TOPIC_NAME_DOC =
            "The name of the kafka topic to use to persist data when " + "\""
                    + BACKING_METHOD_CONFIG + "\" is set to \"kafka\". If the topic is not "
                    + "present, then it will be created.";

    /**
     * <code>persisted.data.kafka.topic.partition.count</code>
     */
    public static final String KAFKA_TOPIC_PARTITION_COUNT_CONFIG =
            CONFIG_PREFIX + "kafka.topic.partition.count";
    public static final int DEFAULT_KAFKA_TOPIC_PARTITION_COUNT = 2;
    public static final String KAFKA_TOPIC_PARTITION_COUNT_DOC =
            "The number of partitions to ensure are present "
                    + "for the kafka topic. Only applies when \"" + BACKING_METHOD_CONFIG
                    + "\" is set to \"kafka\". If the topic has fewer than this number of "
                    + "partitions, then partitions will be added.";

    /**
     * <code>persisted.data.kafka.topic.replication.factor</code>
     */
    public static final String KAFKA_TOPIC_REPLICATION_FACTOR_CONFIG =
            CONFIG_PREFIX + "kafka.topic.replication.factor";
    public static final short DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR = 2;
    public static final String KAFKA_TOPIC_REPLICATION_FACTOR_DOC =
            "The replication factor to use for the kafka " + "topic. Only applies when \""
                    + BACKING_METHOD_CONFIG + "\" is set to \"kafka\". Multiple partition "
                    + "replicas are desirable to ensure the topic is reasonably available.";

    /**
     * <code>persisted.data.kafka.topic.additional.configs.map</code>
     */
    public static final String KAFKA_TOPIC_ADDITIONAL_CONFIGS_MAP_CONFIG =
            CONFIG_PREFIX + "kafka.topic.additional.configs.map";
    public static final String DEFAULT_KAFKA_TOPIC_ADDITIONAL_CONFIGS_MAP = "";
    public static final String KAFKA_TOPIC_ADDITIONAL_CONFIGS_MAP_DOC =
            "The configs to apply to the kafka topic used to "
                    + "persist Cruise Control data. Only applies if \"" + BACKING_METHOD_CONFIG
                    + "\" is set to \"kafka\". This \"list\" should be a semicolon separated "
                    + "string of 'key=value' pairs. The keys and values need to be valid Kafka "
                    + "Topic configs. See: "
                    + "https://kafka.apache.org/documentation/#topicconfigs";

    /**
     * <code>persisted.data.kafka.producer.additional.configs.map</code>
     */
    public static final String KAFKA_PRODUCER_ADDITIONAL_CONFIGS_MAP_CONFIG =
            CONFIG_PREFIX + "kafka.producer.additional.configs.map";
    public static final String DEFAULT_KAFKA_PRODUCER_ADDITIONAL_CONFIGS_MAP = "";
    public static final String KAFKA_PRODUCER_ADDITIONAL_CONFIGS_MAP_DOC =
            "The additional configs to use when creating the kafka "
                    + "producer to persist Cruise Control data. Only applies if \""
                    + BACKING_METHOD_CONFIG + "\" is set to \"kafka\". This \"list\" should be a "
                    + "semicolon separated string of 'key=value' pairs. The keys and values need "
                    + "to be valid Kafka Producer configs. See: "
                    + "https://kafka.apache.org/documentation/#producerconfigs";

    /**
     * <code>persisted.data.kafka.consumer.additional.configs.map</code>
     */
    public static final String KAFKA_CONSUMER_ADDITIONAL_CONFIGS_MAP_CONFIG =
            CONFIG_PREFIX + "kafka.consumer.additional.configs.map";
    public static final String DEFAULT_KAFKA_CONSUMER_ADDITIONAL_CONFIGS_MAP = "";
    public static final String KAFKA_CONSUMER_ADDITIONAL_CONFIGS_MAP_DOC =
            "The additional configs to use when creating the kafka "
                    + "consumer to read persisted Cruise Control data. Only applies if \""
                    + BACKING_METHOD_CONFIG + "\" is set to \"kafka\". This \"list\" should be a "
                    + "semicolon separated string of 'key=value' pairs. The keys and values need "
                    + "to be valid Kafka Consumer configs. See: "
                    + "https://kafka.apache.org/documentation/#consumerconfigs";

    private PersistedDataConfig() {
    }

    /**
     * Define persisted data configs.
     *
     * @param configDef Config definition.
     * @return The given ConfigDef after defining the common configs.
     */
    public static ConfigDef define(ConfigDef configDef) {
        return configDef
                .define(BACKING_METHOD_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_BACKING_METHOD,
                        ConfigDef.Importance.MEDIUM,
                        BACKING_METHOD_DOC)
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
                        KAFKA_TOPIC_REPLICATION_FACTOR_DOC)
                .define(KAFKA_TOPIC_ADDITIONAL_CONFIGS_MAP_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_KAFKA_TOPIC_ADDITIONAL_CONFIGS_MAP,
                        ConfigDef.Importance.LOW,
                        KAFKA_TOPIC_ADDITIONAL_CONFIGS_MAP_DOC)
                .define(KAFKA_PRODUCER_ADDITIONAL_CONFIGS_MAP_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_KAFKA_PRODUCER_ADDITIONAL_CONFIGS_MAP,
                        ConfigDef.Importance.LOW,
                        KAFKA_PRODUCER_ADDITIONAL_CONFIGS_MAP_DOC)
                .define(KAFKA_CONSUMER_ADDITIONAL_CONFIGS_MAP_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_KAFKA_CONSUMER_ADDITIONAL_CONFIGS_MAP,
                        ConfigDef.Importance.LOW,
                        KAFKA_CONSUMER_ADDITIONAL_CONFIGS_MAP_DOC);
    }
}
