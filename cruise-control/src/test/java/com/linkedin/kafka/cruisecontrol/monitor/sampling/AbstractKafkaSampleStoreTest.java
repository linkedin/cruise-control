/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
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
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link AbstractKafkaSampleStore}
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest(AbstractKafkaSampleStore.class)
public class AbstractKafkaSampleStoreTest {

    @Test
    public void testSampleStoreTopicReplicationFactorWhenValueAlreadyExists() {
        short expected = 1;
        Map<String, ?> config = Collections.emptyMap();
        AdminClient adminClient = PowerMockito.mock(AdminClient.class);
        AbstractKafkaSampleStore kafkaSampleStore = getAbstractKafkaSampleStore(config, adminClient);
        Whitebox.setInternalState(kafkaSampleStore, "_sampleStoreTopicReplicationFactor", expected);

        short actual = kafkaSampleStore.sampleStoreTopicReplicationFactor(config, adminClient);

        assertEquals(expected, actual);
    }

    @Test
    public void testSampleStoreTopicReplicationFactorWhenValueNotExistsAndNodeCountIsOne() throws Exception {
        Map<String, Object> config = createFilledConfigMap();
        AdminClient adminClient = PowerMockito.mock(AdminClient.class);
        prepareForNumberOfBrokersCall(adminClient, false);
        AbstractKafkaSampleStore kafkaSampleStore = getAbstractKafkaSampleStore(config, adminClient);

        assertThrows(IllegalStateException.class,
                () -> kafkaSampleStore.sampleStoreTopicReplicationFactor(config, adminClient));
    }

    @Test
    public void testSampleStoreTopicReplicationFactorWhenValueNotExistsAndNodeCountIsTwo() throws Exception {
        short expected = 2;
        Map<String, Object> config = createFilledConfigMap();
        AdminClient adminClient = PowerMockito.mock(AdminClient.class);
        prepareForNumberOfBrokersCall(adminClient, true);
        AbstractKafkaSampleStore kafkaSampleStore = getAbstractKafkaSampleStore(config, adminClient);

        short actual = kafkaSampleStore.sampleStoreTopicReplicationFactor(config, adminClient);

        assertEquals(expected, actual);
    }

    @Test
    public void testSampleStoreTopicReplicationFactorWhenValueNotExistsAndDescribeOfClusterFails()
            throws ExecutionException, InterruptedException, TimeoutException {
        Map<String, Object> config = createFilledConfigMap();
        AdminClient adminClient = PowerMockito.mock(AdminClient.class);
        KafkaFuture nodesFuture = getNodesKafkaFutureWithRequiredMocks(adminClient);

        when(nodesFuture.get(TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS)).thenThrow(TimeoutException.class);
        AbstractKafkaSampleStore kafkaSampleStore = getAbstractKafkaSampleStore(config, adminClient);

        assertThrows(IllegalStateException.class,
                () -> kafkaSampleStore.sampleStoreTopicReplicationFactor(config, adminClient));
    }

    private Map<String, Object> createFilledConfigMap() {
        Map<String, Object> config = new HashMap<>();
        config.put(MonitorConfig.FETCH_METRIC_SAMPLES_RETRY_COUNT_MAX_CONFIG, 2);
        return config;
    }

    private void prepareForNumberOfBrokersCall(AdminClient adminClient, boolean isNodeCountEnough) throws Exception {
        KafkaFuture nodesFuture = getNodesKafkaFutureWithRequiredMocks(adminClient);
        Node node = PowerMockito.mock(Node.class);
        Collection<Node> nodes = isNodeCountEnough ? Arrays.asList(node, node) : Collections.singletonList(node);

        try {
            when(nodesFuture.get(TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS)).thenReturn(nodes);
        } catch (Exception e) {
            throw new Exception("Cannot prepare test mocks: " + e);
        }
    }

    private KafkaFuture getNodesKafkaFutureWithRequiredMocks(AdminClient adminClient) {
        DescribeClusterResult describeClusterResult = PowerMockito.mock(DescribeClusterResult.class);
        KafkaFuture nodesFuture = PowerMockito.mock(KafkaFuture.class);

        when(adminClient.describeCluster()).thenReturn(describeClusterResult);
        when(describeClusterResult.nodes()).thenReturn(nodesFuture);

        return nodesFuture;
    }

    private AbstractKafkaSampleStore getAbstractKafkaSampleStore(Map<String, ?> config, AdminClient adminClient) {
        AbstractKafkaSampleStore kafkaSampleStore
                = PowerMockito.mock(AbstractKafkaSampleStore.class);
        PowerMockito.doCallRealMethod()
                .when(kafkaSampleStore)
                .sampleStoreTopicReplicationFactor(config, adminClient);
        return kafkaSampleStore;
    }

}
