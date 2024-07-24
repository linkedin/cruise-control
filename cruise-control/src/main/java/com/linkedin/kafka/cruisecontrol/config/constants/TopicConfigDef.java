/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import kafka.log.Defaults;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.TopicConfig;

/**
 * Generates the ConfigDef instance for the TopicConfig class provided by Kafka. It may only be
 * needed for Kafka versions < 3.5 since there is some refactoring wrt LogConfig after that. The
 * following code was generated using the following prompt for ChatGPT:
 *
 * <pre>
 *     The following class file exists, named ProducerConfig.java:
 *     ```java
 *      ... contents of org/apache/kafka/clients/producer/ProducerConfig.java ...
 *     ```
 *
 *     It defines `_CONFIG` and `_DOC` String values, then initializes a `ConfigDef`
 *     instance starting with the `CONFIG = new ConfigDef().define(...` code. I can
 *     then get a copy of the `CONFIG` variable using the `configDef()` static method.
 *
 *     I also have the `TopicConfig.java` file which contains the following:
 *     ```java
 *      ... contents of org/apache/kafka/common/config/TopicConfig.java ...
 *     ```
 *
 *     Default values can be referenced from the `LogConfig.scala` file which contains the
 *     following:
 *     ```scala
 *      ... contents of org/apache/kafka/log/LogConfig.scala...
 *     ```
 *
 *     I want to create a new class named `TopicConfigDef` which uses the `_CONFIG` and `_DOC`
 *     variables defined in `TopicConfig` to initialize a `ConfigDef` instance. The `ConfigDef`
 *     instance should be created and initialized in a private static method, so the deprecation
 *     warning suppression annotation can be applied to it, rather than in a `static`
 *     initialization block. Do not include a `static` initialization block, but instead assign to
 *     the `CONFIG` member variable. It must also have a `configDef()` static method to get a copy
 *     of the statically created `ConfigDef` instance. Each of the calls to the `define()` method
 *     should include an argument to the `defaultValue` parameter. Reference the correct value from
 *     the `kafka.log.Defaults` object as though it is a method call. e.g.
 *     `kafka.log.Defaults.SegmentSize()`
 *  </pre>
 *
 * See <a href="https://kafka.apache.org/documentation/#topicconfigs">Kafka topic configuration
 * documentation</a> for more details.
 */
public final class TopicConfigDef {

    private static final ConfigDef CONFIG = initConfig();

    private TopicConfigDef() {
    }

    @SuppressWarnings("deprecation")
    private static ConfigDef initConfig() {

        return new ConfigDef()
                .define(
                        TopicConfig.CLEANUP_POLICY_CONFIG,
                        Type.STRING,
                        Defaults.CleanupPolicy(),
                        Importance.HIGH,
                        TopicConfig.CLEANUP_POLICY_DOC)
                .define(
                        TopicConfig.COMPRESSION_TYPE_CONFIG,
                        Type.STRING,
                        Defaults.CompressionType(),
                        Importance.MEDIUM,
                        TopicConfig.COMPRESSION_TYPE_DOC)
                .define(
                        TopicConfig.DELETE_RETENTION_MS_CONFIG,
                        Type.LONG,
                        Defaults.DeleteRetentionMs(),
                        Importance.LOW,
                        TopicConfig.DELETE_RETENTION_MS_DOC)
                .define(
                        TopicConfig.FILE_DELETE_DELAY_MS_CONFIG,
                        Type.LONG,
                        Defaults.FileDeleteDelayMs(),
                        Importance.LOW,
                        TopicConfig.FILE_DELETE_DELAY_MS_DOC)
                .define(
                        TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG,
                        Type.LONG,
                        Defaults.FlushInterval(),
                        Importance.LOW,
                        TopicConfig.FLUSH_MESSAGES_INTERVAL_DOC)
                .define(
                        TopicConfig.FLUSH_MS_CONFIG,
                        Type.LONG,
                        Defaults.FlushMs(),
                        Importance.LOW,
                        TopicConfig.FLUSH_MS_DOC)
                .define(
                        TopicConfig.INDEX_INTERVAL_BYTES_CONFIG,
                        Type.INT,
                        Defaults.IndexInterval(),
                        Importance.MEDIUM,
                        TopicConfig.INDEX_INTERVAL_BYTES_DOCS)
                .define(
                        TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG,
                        Type.LONG,
                        Defaults.LocalRetentionBytes(),
                        Importance.MEDIUM,
                        TopicConfig.LOCAL_LOG_RETENTION_BYTES_DOC)
                .define(
                        TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG,
                        Type.LONG,
                        Defaults.LocalRetentionMs(),
                        Importance.MEDIUM,
                        TopicConfig.LOCAL_LOG_RETENTION_MS_DOC)
                .define(
                        TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG,
                        Type.LONG,
                        Defaults.MaxCompactionLagMs(),
                        Importance.LOW,
                        TopicConfig.MAX_COMPACTION_LAG_MS_DOC)
                .define(
                        TopicConfig.MAX_MESSAGE_BYTES_CONFIG,
                        Type.INT,
                        Defaults.MaxMessageSize(),
                        Importance.MEDIUM,
                        TopicConfig.MAX_MESSAGE_BYTES_DOC)
                .define(
                        TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG,
                        Type.BOOLEAN,
                        Defaults.MessageDownConversionEnable(),
                        Importance.MEDIUM,
                        TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_DOC)
                .define(
                        TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG,
                        Type.STRING,
                        Defaults.MessageFormatVersion(),
                        Importance.MEDIUM,
                        TopicConfig.MESSAGE_FORMAT_VERSION_DOC)
                .define(
                        TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG,
                        Type.LONG,
                        Defaults.MessageTimestampDifferenceMaxMs(),
                        Importance.MEDIUM,
                        TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC)
                .define(
                        TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG,
                        Type.STRING,
                        Defaults.MessageTimestampType(),
                        Importance.MEDIUM,
                        TopicConfig.MESSAGE_TIMESTAMP_TYPE_DOC)
                .define(
                        TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG,
                        Type.DOUBLE,
                        Defaults.MinCleanableDirtyRatio(),
                        Importance.LOW,
                        TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_DOC)
                .define(
                        TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG,
                        Type.LONG,
                        Defaults.MinCompactionLagMs(),
                        Importance.LOW,
                        TopicConfig.MIN_COMPACTION_LAG_MS_DOC)
                .define(
                        TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                        Type.INT,
                        Defaults.MinInSyncReplicas(),
                        Importance.HIGH,
                        TopicConfig.MIN_IN_SYNC_REPLICAS_DOC)
                .define(
                        TopicConfig.PREALLOCATE_CONFIG,
                        Type.BOOLEAN,
                        Defaults.PreAllocateEnable(),
                        Importance.LOW,
                        TopicConfig.PREALLOCATE_DOC)
                .define(
                        TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG,
                        Type.BOOLEAN,
                        Defaults.RemoteLogStorageEnable(),
                        Importance.MEDIUM,
                        TopicConfig.REMOTE_LOG_STORAGE_ENABLE_DOC)
                .define(
                        TopicConfig.RETENTION_BYTES_CONFIG,
                        Type.LONG,
                        Defaults.RetentionSize(),
                        Importance.HIGH,
                        TopicConfig.RETENTION_BYTES_DOC)
                .define(
                        TopicConfig.RETENTION_MS_CONFIG,
                        Type.LONG,
                        Defaults.RetentionMs(),
                        Importance.HIGH,
                        TopicConfig.RETENTION_MS_DOC)
                .define(
                        TopicConfig.SEGMENT_BYTES_CONFIG,
                        Type.INT,
                        Defaults.SegmentSize(),
                        Importance.HIGH,
                        TopicConfig.SEGMENT_BYTES_DOC)
                .define(
                        TopicConfig.SEGMENT_INDEX_BYTES_CONFIG,
                        Type.INT,
                        Defaults.MaxIndexSize(),
                        Importance.MEDIUM,
                        TopicConfig.SEGMENT_INDEX_BYTES_DOC)
                .define(
                        TopicConfig.SEGMENT_JITTER_MS_CONFIG,
                        Type.LONG,
                        Defaults.SegmentJitterMs(),
                        Importance.MEDIUM,
                        TopicConfig.SEGMENT_JITTER_MS_DOC)
                .define(
                        TopicConfig.SEGMENT_MS_CONFIG,
                        Type.LONG,
                        Defaults.SegmentMs(),
                        Importance.HIGH,
                        TopicConfig.SEGMENT_MS_DOC)
                .define(
                        TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
                        Type.BOOLEAN,
                        Defaults.UncleanLeaderElectionEnable(),
                        Importance.MEDIUM,
                        TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_DOC);
    }

    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }
}
