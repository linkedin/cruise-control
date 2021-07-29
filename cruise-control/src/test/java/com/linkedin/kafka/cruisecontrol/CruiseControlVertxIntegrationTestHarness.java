/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCEmbeddedBroker;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import com.linkedin.kafka.cruisecontrol.servlet.response.ClusterBrokerState;
import com.linkedin.kafka.cruisecontrol.vertx.VertxRequestHandler;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class CruiseControlVertxIntegrationTestHarness extends CCKafkaIntegrationTestHarness {
    protected KafkaCruiseControlConfig _config;
    protected KafkaCruiseControlServletApp _servletApp;
    protected KafkaCruiseControlVertxApp _vertxApp;
    protected AdminClient _adminClient;
    protected int _vertxPort;
    protected int _servletPort;

    protected static final String LOCALHOST = "localhost";

    protected Map<String, Object> withConfigs() {
        return Collections.emptyMap();
    }
    public String getServletResult(String endpoint, int port) throws IOException {
        URL servletUrl = new URL("http://localhost:" + port + "/kafkacruisecontrol/" + endpoint);
        HttpURLConnection servletCon = (HttpURLConnection) servletUrl.openConnection();
        servletCon.setRequestMethod("GET");

        BufferedReader servletIn = new BufferedReader(
                new InputStreamReader(servletCon.getInputStream()));
        String servletInputLine;
        StringBuffer servletContent = new StringBuffer();
        while ((servletInputLine = servletIn.readLine()) != null) {
            servletContent.append(servletInputLine);
        }
        servletIn.close();
        return servletContent.toString();
    }

    public String getVertxResult(String endpoint, Integer port) throws Exception {
        URL vertxUrl = new URL("http://localhost:" + port + "/" + endpoint);
        HttpURLConnection vertxCon = (HttpURLConnection) vertxUrl.openConnection();
        vertxCon.setRequestMethod("GET");

        BufferedReader vertxIn = new BufferedReader(
                new InputStreamReader(vertxCon.getInputStream()));
        String vertxInputLine;
        StringBuffer vertxContent = new StringBuffer();
        Thread.sleep(10000);
        while ((vertxInputLine = vertxIn.readLine()) != null) {
            vertxContent.append(vertxInputLine);
        }
        vertxIn.close();
        vertxCon.disconnect();
        return vertxContent.toString();
    }

    private void setupConfig() {
        Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
        properties.put(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        properties.put(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect());
        properties.put(KafkaSampleStore.PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG, "__partition_samples");
        properties.put(KafkaSampleStore.BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG, "__broker_samples");
        properties.putAll(withConfigs());
        _config = new KafkaCruiseControlConfig(properties);
    }

    public void start() throws Exception {
        super.setUp();
        _brokers.values().forEach(CCEmbeddedBroker::startup);
        setupConfig();
        MetricRegistry metricRegistry = new MetricRegistry();
        AsyncKafkaCruiseControl kafkaCruiseControl = new AsyncKafkaCruiseControl(_config, metricRegistry);
        kafkaCruiseControl.startUp();
        _vertxApp = new KafkaCruiseControlVertxApp(_config, new ServerSocket(0).getLocalPort(), LOCALHOST,
                kafkaCruiseControl, metricRegistry);
        VertxRequestHandler vertxRequestHandler = null;
        ClusterBrokerState clusterBrokerState =
                new ClusterBrokerState(kafkaCruiseControl.kafkaCluster(),
                        kafkaCruiseControl.adminClient(), kafkaCruiseControl.config());
        while (clusterBrokerState.getLeaderCountByBrokerId().get(0).equals(0)
        && clusterBrokerState.getLeaderCountByBrokerId().get(1).equals(0)) {
            Thread.sleep(1000);
            clusterBrokerState =
                    new ClusterBrokerState(kafkaCruiseControl.kafkaCluster(),
                            kafkaCruiseControl.adminClient(), kafkaCruiseControl.config());
        }
        vertxRequestHandler = (VertxRequestHandler) _vertxApp.getVerticle().getEndPoints();
        _servletApp = new KafkaCruiseControlServletApp(_config, new ServerSocket(0).getLocalPort(), LOCALHOST,
                kafkaCruiseControl, metricRegistry, vertxRequestHandler.cruiseControlEndPoints()._userTaskManager);
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        _adminClient = AdminClient.create(properties);
        _servletPort = _servletApp.getPort();
        _vertxPort = _vertxApp.getPort();
    }

    public void stop() {
        _adminClient.close();
        if (_vertxApp != null) {
            _vertxApp.stop();
        }
        if (_servletApp != null) {
            _servletApp.stop();
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
