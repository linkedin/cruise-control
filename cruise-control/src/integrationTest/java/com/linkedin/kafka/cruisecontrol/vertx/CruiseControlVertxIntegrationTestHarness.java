/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.vertx;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlApp;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlVertxApp;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCEmbeddedBroker;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import kafka.server.KafkaConfig;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class CruiseControlVertxIntegrationTestHarness extends CCKafkaIntegrationTestHarness {
    protected KafkaCruiseControlConfig _config;
    protected KafkaCruiseControlApp _vertxApp;
    protected int _vertxPort;
    protected static final String LOCALHOST = "localhost";
    protected Map<String, Object> withConfigs() {
        return Collections.emptyMap();
    }

    /**
     *
     * @param endpoint Endpoint that the client sends the request to
     * @return The response code of the HTTP response
     * @throws Exception
     */
    public Integer getResponseCode(String endpoint) throws Exception {
        URL url = new URL("http://localhost:" + _vertxPort + "/kafkacruisecontrol/" + endpoint);
        HttpURLConnection huc = (HttpURLConnection) url.openConnection();
        return huc.getResponseCode();
    }

    /**
     * Sets up the test environment with the given config
     */
    private void setupConfig() {
        Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
        properties.put(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        properties.put(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect());
        properties.put(KafkaSampleStore.PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG, "__partition_samples");
        properties.put(KafkaSampleStore.BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG, "__broker_samples");
        properties.putAll(withConfigs());
        _config = new KafkaCruiseControlConfig(properties);
    }

    /**
     * Starts the test environment
     * @throws Exception
     */
    public void start() throws Exception {
        super.setUp();
        _brokers.values().forEach(CCEmbeddedBroker::startup);
        setupConfig();
        _vertxPort = new ServerSocket(0).getLocalPort();
        _vertxApp = new KafkaCruiseControlVertxApp(_config, _vertxPort, LOCALHOST);
        _vertxApp.start();
    }

    /**
     * Stops the test environment
     */
    public void stop() {
        if (_vertxApp != null) {
            _vertxApp.stop();
        }
        _brokers.values().forEach(CCEmbeddedBroker::shutdown);
        _brokers.values().forEach(CCEmbeddedBroker::awaitShutdown);
        super.tearDown();
    }

    @Override
    protected Map<Object, Object> overridingProps() {
        return Collections.singletonMap(KafkaConfig.MetricReporterClassesProp(), CruiseControlMetricsReporter.class.getName());
    }
}
