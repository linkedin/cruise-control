/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.PersistedDataConfig;
import com.linkedin.kafka.cruisecontrol.persisteddata.kafka.KafkaPersistedMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

import static org.easymock.EasyMock.mock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.Is.isA;

public class PersistedMapFactoryTest {

    public static final String SERIALIZER = StringSerializer.class.getName();
    public static final String DESERIALIZER = StringDeserializer.class.getName();

    /**
     * Ensure setting the backing method config results in the factory producing an instance of the
     * right type.
     */
    @Test
    public void instanceReturnsKafkaPersistedMapWhenConfiguredForKafka() {
        final PersistedMapFactory factory = configureAndGetPersistedMapFactory("kafka");
        PersistedMap map = factory.instance();
        assertThat(map instanceof KafkaPersistedMap, is(true));
    }

    /**
     * Ensure setting the backing method config results in the factory producing an instance of the
     * right type.
     */
    @Test
    public void instanceReturnsPersistedMapWhenConfiguredForMemory() {
        final PersistedMapFactory factory = configureAndGetPersistedMapFactory(
                "memory");
        PersistedMap map = factory.instance();
        assertThat(map, isA(PersistedMap.class));
    }

    private static PersistedMapFactory configureAndGetPersistedMapFactory(String backingMethod) {
        KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(
                Map.of(PersistedDataConfig.PERSIST_METHOD_CONFIG, backingMethod,
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "fake",
                        ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG, "connect:1234",
                        PersistedDataConfig.KAFKA_PRODUCER_CONFIG_PREFIX
                                + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SERIALIZER,
                        PersistedDataConfig.KAFKA_PRODUCER_CONFIG_PREFIX
                                + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SERIALIZER,
                        PersistedDataConfig.KAFKA_CONSUMER_CONFIG_PREFIX
                                + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DESERIALIZER,
                        PersistedDataConfig.KAFKA_CONSUMER_CONFIG_PREFIX
                                + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DESERIALIZER
                ));
        return new PersistedMapFactory(config,
                () -> mock(KafkaPersistedMap.class),
                () -> new PersistedMap(new HashMap<>()));
    }
}
