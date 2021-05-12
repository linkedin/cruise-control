/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCEmbeddedBroker;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public abstract class CruiseControlIntegrationTestHarness extends CCKafkaIntegrationTestHarness {

  protected KafkaCruiseControlConfig _config;
  protected KafkaCruiseControlApp _app;

  protected static final String LOCALHOST = "localhost";
  private static final int ANY_PORT = 0;

  protected Map<String, Object> withConfigs() {
    return Collections.emptyMap();
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

  /**
   * Starts up an embedded Cruise Control environment with Zookeeper, Kafka brokers and a Cruise Control instance.
   */
  public void start() throws Exception {
    super.setUp();
    _brokers.values().forEach(CCEmbeddedBroker::startup);
    setupConfig();
    _app = new KafkaCruiseControlApp(_config, ANY_PORT, LOCALHOST);
    _app.start();
  }

  /**
   * Shuts down the Cruise Control instance, Kafka brokers and the Zookeeper instance.
   */
  public void stop() {
    if (_app != null) {
      _app.stop();
    }
    _brokers.values().forEach(CCEmbeddedBroker::shutdown);
    _brokers.values().forEach(CCEmbeddedBroker::awaitShutdown);
    super.tearDown();
  }
}
