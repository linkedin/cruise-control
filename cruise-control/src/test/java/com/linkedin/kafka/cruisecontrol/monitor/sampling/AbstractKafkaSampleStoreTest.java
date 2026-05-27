/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import java.lang.reflect.Field;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.easymock.EasyMock;
import org.junit.Test;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Unit test for {@link AbstractKafkaSampleStore}
 */
public class AbstractKafkaSampleStoreTest {

    @Test
    public void testSampleStoreTopicReplicationFactorWhenValueAlreadyExists() throws ReflectiveOperationException {
        short expected = 1;
        Map<String, ?> config = Collections.emptyMap();
        AdminClient adminClient = EasyMock.mock(AdminClient.class);
        AbstractKafkaSampleStore kafkaSampleStore = EasyMock.partialMockBuilder(AbstractKafkaSampleStore.class).createMock();
        setField(kafkaSampleStore, "_sampleStoreTopicReplicationFactor", expected);
        EasyMock.replay(adminClient, kafkaSampleStore);

        short actual = kafkaSampleStore.sampleStoreTopicReplicationFactor(config, adminClient);

        assertEquals(expected, actual);
        EasyMock.verify(adminClient, kafkaSampleStore);
    }

    @Test
    public void testSampleStoreTopicReplicationFactorWhenValueNotExistsAndNodeCountIsOne() throws Exception {
        Map<String, Object> config = createFilledConfigMap();
        AdminClient adminClient = EasyMock.mock(AdminClient.class);
        prepareForNumberOfBrokersCall(adminClient, false);
        AbstractKafkaSampleStore kafkaSampleStore = EasyMock.partialMockBuilder(AbstractKafkaSampleStore.class).createMock();
        EasyMock.replay(adminClient, kafkaSampleStore);

        assertThrows(IllegalStateException.class,
                () -> kafkaSampleStore.sampleStoreTopicReplicationFactor(config, adminClient));
        EasyMock.verify(adminClient, kafkaSampleStore);
    }

    @Test
    public void testSampleStoreTopicReplicationFactorWhenValueNotExistsAndNodeCountIsTwo() throws Exception {
        short expected = 2;
        Map<String, Object> config = createFilledConfigMap();
        AdminClient adminClient = EasyMock.mock(AdminClient.class);
        prepareForNumberOfBrokersCall(adminClient, true);
        AbstractKafkaSampleStore kafkaSampleStore = EasyMock.partialMockBuilder(AbstractKafkaSampleStore.class).createMock();
        EasyMock.replay(adminClient, kafkaSampleStore);

        short actual = kafkaSampleStore.sampleStoreTopicReplicationFactor(config, adminClient);

        assertEquals(expected, actual);
        EasyMock.verify(adminClient, kafkaSampleStore);
    }

    @Test
    public void testSampleStoreTopicReplicationFactorWhenValueNotExistsAndDescribeOfClusterFails()
            throws ExecutionException, InterruptedException, TimeoutException {
        Map<String, Object> config = createFilledConfigMap();
        AdminClient adminClient = EasyMock.mock(AdminClient.class);
        KafkaFuture nodesFuture = getNodesKafkaFutureWithRequiredMocks(adminClient);

        EasyMock.expect(nodesFuture.get(TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS))
                .andThrow(new TimeoutException()).times(2);
        EasyMock.replay(nodesFuture);
        AbstractKafkaSampleStore kafkaSampleStore = EasyMock.partialMockBuilder(AbstractKafkaSampleStore.class).createMock();
        EasyMock.replay(adminClient, kafkaSampleStore);

        assertThrows(IllegalStateException.class,
                () -> kafkaSampleStore.sampleStoreTopicReplicationFactor(config, adminClient));
        EasyMock.verify(adminClient, kafkaSampleStore, nodesFuture);
    }

    private Map<String, Object> createFilledConfigMap() {
        Map<String, Object> config = new HashMap<>();
        config.put(MonitorConfig.FETCH_METRIC_SAMPLES_MAX_RETRY_COUNT_CONFIG, 2);
        return config;
    }

    private void prepareForNumberOfBrokersCall(AdminClient adminClient, boolean isNodeCountEnough) throws Exception {
        KafkaFuture nodesFuture = getNodesKafkaFutureWithRequiredMocks(adminClient);
        Node node = EasyMock.mock(Node.class);
        Collection<Node> nodes = isNodeCountEnough ? Arrays.asList(node, node) : Collections.singletonList(node);

        EasyMock.expect(nodesFuture.get(TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS)).andReturn(nodes).anyTimes();
        EasyMock.replay(nodesFuture);
    }

    private KafkaFuture getNodesKafkaFutureWithRequiredMocks(AdminClient adminClient) {
        DescribeClusterResult describeClusterResult = EasyMock.mock(DescribeClusterResult.class);
        KafkaFuture nodesFuture = EasyMock.mock(KafkaFuture.class);

        EasyMock.expect(adminClient.describeCluster()).andReturn(describeClusterResult).anyTimes();
        EasyMock.expect(describeClusterResult.nodes()).andReturn(nodesFuture).anyTimes();
        EasyMock.replay(describeClusterResult);

        return nodesFuture;
    }

    // Replacement for PowerMock's Whitebox.setInternalState. Plain reflection on
    // an in-package field works under JDK 17 without --add-opens because the
    // field lives in the unnamed module (our test classpath), not a JDK module.
    private static void setField(Object target, String name, Object value) throws ReflectiveOperationException {
        Class<?> cls = target.getClass();
        while (cls != null) {
            try {
                Field field = cls.getDeclaredField(name);
                field.setAccessible(true);
                field.set(target, value);
                return;
            } catch (NoSuchFieldException ignored) {
                cls = cls.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

}
