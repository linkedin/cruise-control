/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.exception.SamplingException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSamplerOptions;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionEntity;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusMetric;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusQueryResult;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusValue;

import static com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.PrometheusMetricSampler.*;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

/**
 * Unit test for {@link PrometheusMetricSampler} class.
 */
public class PrometheusMetricSamplerTest {
    private static final double DOUBLE_DELTA = 0.00000001;
    private static final double BYTES_IN_KB = 1024.0;

    private static final int FIXED_VALUE = 94;
    private static final long START_EPOCH_SECONDS = 1603301400L;
    private static final long START_TIME_MS = TimeUnit.SECONDS.toMillis(START_EPOCH_SECONDS);
    private static final long END_TIME_MS = START_TIME_MS + TimeUnit.SECONDS.toMillis(59);

    private static final int TOTAL_BROKERS = 3;
    private static final int TOTAL_PARTITIONS = 3;

    private static final String TEST_TOPIC = "test-topic";

    private PrometheusMetricSampler _prometheusMetricSampler;
    private PrometheusAdapter _prometheusAdapter;
    private Map<RawMetricType, String> _prometheusQueryMap;

    /**
     * Set up mocks
     */
    @Before
    public void setUp() {
        _prometheusAdapter = mock(PrometheusAdapter.class);
        _prometheusMetricSampler = new PrometheusMetricSampler();
        _prometheusQueryMap = new DefaultPrometheusQuerySupplier().get();
    }

    @Test(expected = ConfigException.class)
    public void testConfigureWithPrometheusEndpointNoPortFails() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(PROMETHEUS_SERVER_ENDPOINT_CONFIG, "http://kafka-cluster-1.org");
        addCapacityConfig(config);
        _prometheusMetricSampler.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void testConfigureWithPrometheusEndpointNegativePortFails() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(PROMETHEUS_SERVER_ENDPOINT_CONFIG, "http://kafka-cluster-1.org:-20");
        addCapacityConfig(config);
        _prometheusMetricSampler.configure(config);
    }

    @Test
    public void testConfigureWithPrometheusEndpointNoSchemaDoesNotFail() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(PROMETHEUS_SERVER_ENDPOINT_CONFIG, "kafka-cluster-1.org:9090");
        addCapacityConfig(config);
        _prometheusMetricSampler.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void testConfigureWithNoPrometheusEndpointFails() throws Exception {
        Map<String, Object> config = new HashMap<>();
        addCapacityConfig(config);
        _prometheusMetricSampler.configure(config);
    }

    @Test(expected = SamplingException.class)
    public void testGetSamplesQueryThrowsException() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(PROMETHEUS_SERVER_ENDPOINT_CONFIG, "http://kafka-cluster-1.org:9090");
        addCapacityConfig(config);
        _prometheusMetricSampler.configure(config);

        MetricSamplerOptions metricSamplerOptions = buildMetricSamplerOptions();
        _prometheusMetricSampler._prometheusAdapter = _prometheusAdapter;
        expect(_prometheusAdapter.queryMetric(anyString(), anyLong(), anyLong()))
            .andThrow(new IOException("Exception in fetching metrics"));

        replay(_prometheusAdapter);
        try {
            _prometheusMetricSampler.getSamples(metricSamplerOptions);
        } finally {
            verify(_prometheusAdapter);
        }
    }

    @Test
    public void testGetSamplesCustomPrometheusQuerySupplier() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(PROMETHEUS_SERVER_ENDPOINT_CONFIG, "http://kafka-cluster-1.org:9090");
        addCapacityConfig(config);
        config.put(PROMETHEUS_QUERY_SUPPLIER_CONFIG, TestQuerySupplier.class.getName());
        _prometheusMetricSampler.configure(config);

        MetricSamplerOptions metricSamplerOptions = buildMetricSamplerOptions();
        _prometheusMetricSampler._prometheusAdapter = _prometheusAdapter;

        expect(_prometheusAdapter.queryMetric(eq(TestQuerySupplier.TEST_QUERY), anyLong(), anyLong()))
            .andReturn(buildBrokerResults());
        replay(_prometheusAdapter);

        _prometheusMetricSampler.getSamples(metricSamplerOptions);

        verify(_prometheusAdapter);
    }

    @Test(expected = ConfigException.class)
    public void testGetSamplesPrometheusQuerySupplierUnknownClass() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(PROMETHEUS_SERVER_ENDPOINT_CONFIG, "http://kafka-cluster-1.org:9090");
        addCapacityConfig(config);
        config.put(PROMETHEUS_QUERY_SUPPLIER_CONFIG, "com.test.NonExistentClass");
        _prometheusMetricSampler.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void testGetSamplesPrometheusQuerySupplierInvalidClass() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(PROMETHEUS_SERVER_ENDPOINT_CONFIG, "http://kafka-cluster-1.org:9090");
        addCapacityConfig(config);
        config.put(PROMETHEUS_QUERY_SUPPLIER_CONFIG, String.class.getName());
        _prometheusMetricSampler.configure(config);
    }

    private static MetricSamplerOptions buildMetricSamplerOptions() {
        return new MetricSamplerOptions(
            generateCluster(),
            generatePartitions(),
            START_TIME_MS,
            END_TIME_MS,
            MetricSampler.SamplingMode.ALL,
            KafkaMetricDef.commonMetricDef(),
            60000
        );
    }

    @Test
    public void testGetSamplesSuccess() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(PROMETHEUS_SERVER_ENDPOINT_CONFIG, "http://kafka-cluster-1.org:9090");
        addCapacityConfig(config);
        _prometheusMetricSampler.configure(config);

        MetricSamplerOptions metricSamplerOptions = buildMetricSamplerOptions();
        _prometheusMetricSampler._prometheusAdapter = _prometheusAdapter;

        for (RawMetricType rawMetricType : _prometheusQueryMap.keySet()) {
            setupPrometheusAdapterMock(rawMetricType, buildBrokerResults(),
                buildTopicResults(), buildPartitionResults());
        }

        replay(_prometheusAdapter);
        MetricSampler.Samples samples = _prometheusMetricSampler.getSamples(metricSamplerOptions);

        assertSamplesValid(samples);
        verify(_prometheusAdapter);
    }

    @Test
    public void testGetSamplesWithCustomSamplingInterval() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(PROMETHEUS_SERVER_ENDPOINT_CONFIG, "http://kafka-cluster-1.org:9090");
        config.put(PROMETHEUS_QUERY_RESOLUTION_STEP_MS_CONFIG, "5000");
        addCapacityConfig(config);
        _prometheusMetricSampler.configure(config);
        assertEquals(5000, _prometheusMetricSampler._prometheusAdapter._samplingIntervalMs);
    }

    @Test(expected = ConfigException.class)
    public void testGetSamplesWithCustomMalformedSamplingInterval() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(PROMETHEUS_SERVER_ENDPOINT_CONFIG, "http://kafka-cluster-1.org:9090");
        config.put(PROMETHEUS_QUERY_RESOLUTION_STEP_MS_CONFIG, "non-number");
        addCapacityConfig(config);
        _prometheusMetricSampler.configure(config);
    }

    @Test(expected = ConfigException.class)
    public void testGetSamplesWithCustomNegativeSamplingInterval() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(PROMETHEUS_SERVER_ENDPOINT_CONFIG, "http://kafka-cluster-1.org:9090");
        config.put(PROMETHEUS_QUERY_RESOLUTION_STEP_MS_CONFIG, "-2000");
        addCapacityConfig(config);
        _prometheusMetricSampler.configure(config);
    }

    @Test
    public void testPrometheusQueryReturnsBadHostname() throws Exception {
        testPrometheusQueryReturnsInvalidResults(buildBrokerResultsWithBadHostname(),
                                             buildTopicResults(), buildPartitionResults());
    }

    @Test
    public void testPrometheusQueryReturnsNullHostPort() throws Exception {
        testPrometheusQueryReturnsInvalidResults(buildBrokerResultsWithNullHostPort(),
            buildTopicResults(), buildPartitionResults());
    }

    @Test
    public void testPrometheusQueryReturnsNullTopic() throws Exception {
        testPrometheusQueryReturnsInvalidResults(buildBrokerResults(),
            buildTopicResultsWithNullTopic(), buildPartitionResults());
    }

    @Test
    public void testPrometheusQueryReturnsNullPartition() throws Exception {
        testPrometheusQueryReturnsInvalidResults(buildBrokerResults(),
            buildTopicResults(), buildPartitionResultsWithNullPartition());
    }

    @Test
    public void testPrometheusQueryReturnsMalformedPartition() throws Exception {
        testPrometheusQueryReturnsInvalidResults(buildBrokerResults(),
            buildTopicResults(), buildPartitionResultsWithMalformedPartition());
    }

    public void testPrometheusQueryReturnsInvalidResults(
        List<PrometheusQueryResult> brokerResults,
        List<PrometheusQueryResult> topicResults,
        List<PrometheusQueryResult> partitionResults) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(PROMETHEUS_SERVER_ENDPOINT_CONFIG, "http://kafka-cluster-1.org:9090");
        addCapacityConfig(config);
        _prometheusMetricSampler.configure(config);

        MetricSamplerOptions metricSamplerOptions = buildMetricSamplerOptions();
        _prometheusMetricSampler._prometheusAdapter = _prometheusAdapter;
        for (RawMetricType rawMetricType : _prometheusQueryMap.keySet()) {
            setupPrometheusAdapterMock(rawMetricType, brokerResults,
                topicResults, partitionResults);
        }

        replay(_prometheusAdapter);
        _prometheusMetricSampler.getSamples(metricSamplerOptions);
    }

    private void assertSamplesValid(MetricSampler.Samples samples) {
        assertEquals(TOTAL_BROKERS, samples.brokerMetricSamples().size());
        assertEquals(
            new HashSet<>(Arrays.asList(0, 1, 2)),
            samples.brokerMetricSamples().stream()
                .map(BrokerMetricSample::brokerId)
                .collect(Collectors.toSet()));
        samples.brokerMetricSamples().forEach(brokerMetricSample -> {
            assertEquals(FIXED_VALUE,
                         brokerMetricSample.metricValue(KafkaMetricDef.CPU_USAGE),
                DOUBLE_DELTA);
            assertEquals(FIXED_VALUE / BYTES_IN_KB,
                         brokerMetricSample.metricValue(KafkaMetricDef.LEADER_BYTES_OUT),
                DOUBLE_DELTA);
        });

        assertEquals(TOTAL_BROKERS, samples.partitionMetricSamples().size());
        assertEquals(
            new HashSet<>(Arrays.asList(0, 1, 2)),
            samples.partitionMetricSamples().stream()
                .map(PartitionMetricSample::entity)
                .map(PartitionEntity::tp)
                .map(TopicPartition::partition)
                .collect(Collectors.toSet()));

        samples.partitionMetricSamples().forEach(partitionMetricSample -> {
            assertEquals(TEST_TOPIC, partitionMetricSample.entity().tp().topic());
            assertEquals(FIXED_VALUE,
                         partitionMetricSample.metricValue(
                             KafkaMetricDef.commonMetricDefId(KafkaMetricDef.MESSAGE_IN_RATE)),
                DOUBLE_DELTA);
            assertEquals(FIXED_VALUE / BYTES_IN_KB,
                         partitionMetricSample.metricValue(
                         KafkaMetricDef.commonMetricDefId(KafkaMetricDef.LEADER_BYTES_IN)),
                DOUBLE_DELTA);
        });
    }

    private void setupPrometheusAdapterMock(RawMetricType metricType,
                                            List<PrometheusQueryResult> brokerResults,
                                            List<PrometheusQueryResult> topicResults,
                                            List<PrometheusQueryResult> partitionResults) throws IOException {
        switch (metricType.metricScope()) {
            case BROKER:
                expect(_prometheusAdapter.queryMetric(eq(_prometheusQueryMap.get(metricType)), anyLong(), anyLong()))
                    .andReturn(brokerResults);
                break;
            case TOPIC:
                expect(_prometheusAdapter.queryMetric(eq(_prometheusQueryMap.get(metricType)), anyLong(), anyLong()))
                    .andReturn(topicResults);
                break;
            case PARTITION:
                expect(_prometheusAdapter.queryMetric(eq(_prometheusQueryMap.get(metricType)), anyLong(), anyLong()))
                    .andReturn(partitionResults);
                break;
            default:
                break;
        }
    }

    private static List<PrometheusQueryResult> buildBrokerResults() {
        List<PrometheusQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < TOTAL_BROKERS; brokerId++) {
            resultList.add(new PrometheusQueryResult(new PrometheusMetric(
                "broker-" + brokerId + ".test-cluster.org:11001",
                null,
                null
            ), Collections.singletonList(new PrometheusValue(START_EPOCH_SECONDS, FIXED_VALUE))));
        }
        return resultList;
    }

    private static List<PrometheusQueryResult> buildBrokerResultsWithBadHostname() {
        List<PrometheusQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < TOTAL_BROKERS; brokerId++) {
            resultList.add(new PrometheusQueryResult(new PrometheusMetric(
                "broker-" + brokerId + ".non-existent-cluster.org:11001",
                null,
                null
            ), Collections.singletonList(new PrometheusValue(START_EPOCH_SECONDS, FIXED_VALUE))));
        }
        return resultList;
    }

    private static List<PrometheusQueryResult> buildBrokerResultsWithNullHostPort() {
        List<PrometheusQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < TOTAL_BROKERS; brokerId++) {
            resultList.add(new PrometheusQueryResult(new PrometheusMetric(
                null,
                null,
                null
            ), Collections.singletonList(new PrometheusValue(START_EPOCH_SECONDS, FIXED_VALUE))));
        }
        return resultList;
    }

    private static List<PrometheusQueryResult> buildTopicResults() {
        List<PrometheusQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < TOTAL_BROKERS; brokerId++) {
            resultList.add(new PrometheusQueryResult(new PrometheusMetric(
                "broker-" + brokerId + ".test-cluster.org:11001",
                TEST_TOPIC,
                null
            ), Collections.singletonList(new PrometheusValue(START_EPOCH_SECONDS, FIXED_VALUE))));
        }
        return resultList;
    }

    private static List<PrometheusQueryResult> buildTopicResultsWithNullTopic() {
        List<PrometheusQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < TOTAL_BROKERS; brokerId++) {
            resultList.add(new PrometheusQueryResult(new PrometheusMetric(
                "broker-" + brokerId + ".test-cluster.org:11001",
                null,
                null
            ), Collections.singletonList(new PrometheusValue(START_EPOCH_SECONDS, FIXED_VALUE))));
        }
        return resultList;
    }

    private static List<PrometheusQueryResult> buildPartitionResultsWithNullPartition() {
        List<PrometheusQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < TOTAL_BROKERS; brokerId++) {
            resultList.add(new PrometheusQueryResult(new PrometheusMetric(
                "broker-" + brokerId + ".test-cluster.org:11001",
                TEST_TOPIC,
                null
            ), Collections.singletonList(new PrometheusValue(START_EPOCH_SECONDS, FIXED_VALUE))));
        }
        return resultList;
    }

    private static List<PrometheusQueryResult> buildPartitionResultsWithMalformedPartition() {
        List<PrometheusQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < TOTAL_BROKERS; brokerId++) {
            resultList.add(new PrometheusQueryResult(new PrometheusMetric(
                "broker-" + brokerId + ".test-cluster.org:11001",
                TEST_TOPIC,
                "non-number"
            ), Collections.singletonList(new PrometheusValue(START_EPOCH_SECONDS, FIXED_VALUE))));
        }
        return resultList;
    }

    private static List<PrometheusQueryResult> buildPartitionResults() {
        List<PrometheusQueryResult> resultList = new ArrayList<>();
        for (int brokerId = 0; brokerId < TOTAL_BROKERS; brokerId++) {
            for (int partition = 0; partition < TOTAL_PARTITIONS; partition++) {
                resultList.add(new PrometheusQueryResult(new PrometheusMetric(
                    "broker-" + brokerId + ".test-cluster.org:11001",
                    TEST_TOPIC,
                    String.valueOf(partition)
                ), Collections.singletonList(new PrometheusValue(START_EPOCH_SECONDS, FIXED_VALUE))));
            }
        }
        return resultList;
    }

    private void addCapacityConfig(Map<String, Object> config) throws IOException {
        File capacityConfigFile = File.createTempFile("capacityConfig", "json");
        FileOutputStream fileOutputStream = new FileOutputStream(capacityConfigFile);
        try (OutputStreamWriter writer = new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8)) {
            writer.write("{\n"
                + "  \"brokerCapacities\":[\n"
                + "    {\n"
                + "      \"brokerId\": \"-1\",\n"
                + "      \"capacity\": {\n"
                + "        \"DISK\": \"100000\",\n"
                + "        \"CPU\": {\"num.cores\": \"4\"},\n"
                + "        \"NW_IN\": \"5000000\",\n"
                + "        \"NW_OUT\": \"5000000\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}\n");
        }
        config.put("capacity.config.file", capacityConfigFile.getAbsolutePath());
        BrokerCapacityConfigResolver brokerCapacityConfigResolver = new BrokerCapacityConfigFileResolver();
        config.put("broker.capacity.config.resolver.object", brokerCapacityConfigResolver);
        config.put("sampling.allow.cpu.capacity.estimation", true);
        brokerCapacityConfigResolver.configure(config);
    }

    private static Set<TopicPartition> generatePartitions() {
        Set<TopicPartition> set = new HashSet<>();
        for (int partition = 0; partition < TOTAL_PARTITIONS; partition++) {
            TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, partition);
            set.add(topicPartition);
        }
        return set;
    }

    private static Cluster generateCluster() {
        Node[] allNodes = new Node[TOTAL_BROKERS];
        Set<PartitionInfo> partitionInfo = new HashSet<>(TOTAL_BROKERS);
        for (int brokerId = 0; brokerId < TOTAL_BROKERS; brokerId++) {
            allNodes[brokerId] = new Node(brokerId, "broker-" + brokerId + ".test-cluster.org", 9092);
        }
        for (int partitionId = 0; partitionId < TOTAL_PARTITIONS; partitionId++) {
            partitionInfo.add(new PartitionInfo(TEST_TOPIC, partitionId, allNodes[partitionId], allNodes, allNodes));
        }
        return new Cluster("cluster_id", Arrays.asList(allNodes),
                           partitionInfo, Collections.emptySet(), Collections.emptySet());
    }

    public static class TestQuerySupplier implements PrometheusQuerySupplier {

        public static final String TEST_QUERY = "test_query";

        @Override public Map<RawMetricType, String> get() {
            Map<RawMetricType, String> queryMap = new HashMap<>();
            queryMap.put(RawMetricType.ALL_TOPIC_BYTES_IN, TEST_QUERY);
            return queryMap;
        }
    }
}
