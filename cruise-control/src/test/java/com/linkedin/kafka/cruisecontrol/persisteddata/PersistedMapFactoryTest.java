/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.PersistedDataConfig;
import com.linkedin.kafka.cruisecontrol.persisteddata.kafka.KafkaPersistedMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.Test;

import static org.easymock.EasyMock.mock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.Is.isA;


public class PersistedMapFactoryTest {

    /**
     * Ensure setting the backing method config results in the factory producing an instance of the
     * right type.
     */
    @Test
    public void instanceReturnsKafkaPersistedMapWhenConfiguredForKafka() {
        final PersistedMapFactory factory = configureAndGetPersistedMapFactory(
                "kafka");
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
                Map.of(PersistedDataConfig.BACKING_METHOD_CONFIG, backingMethod,
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "fake",
                        ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG, "connect:1234"));
        return new PersistedMapFactory(config,
                () -> mock(KafkaPersistedMap.class),
                () -> new PersistedMap(new HashMap<>()));
    }
}
