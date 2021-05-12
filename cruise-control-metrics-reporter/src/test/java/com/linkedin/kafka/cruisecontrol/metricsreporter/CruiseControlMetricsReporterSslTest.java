/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaTestUtils;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Assert;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG;


public class CruiseControlMetricsReporterSslTest extends CruiseControlMetricsReporterTest {

  private File _trustStoreFile;

  public CruiseControlMetricsReporterSslTest() {
    super();
    try {
      _trustStoreFile = File.createTempFile("truststore", ".jks");
    } catch (IOException e) {
      Assert.fail("Failed to create trust store");
    }
  }

  @Override
  public Properties overridingProps() {
    Properties props = new Properties();
    int port = CCKafkaTestUtils.findLocalPort();
    // We need to convert all the properties to the Cruise Control properties.
    setSecurityConfigs(props, "producer");
    for (String configName : ProducerConfig.configNames()) {
      Object value = props.get(configName);
      if (value != null) {
        props.remove(configName);
        props.put(appendPrefix(configName), value);
      }
    }
    props.setProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, CruiseControlMetricsReporter.class.getName());
    props.setProperty(KafkaConfig.ListenersProp(), "SSL://127.0.0.1:" + port);
    props.setProperty(CruiseControlMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), "127.0.0.1:" + port);
    props.setProperty(CruiseControlMetricsReporterConfig.config(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG), SecurityProtocol.SSL.name);
    props.setProperty(CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "100");
    props.setProperty(CRUISE_CONTROL_METRICS_TOPIC_CONFIG, TOPIC);
    props.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
    props.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
    props.setProperty(KafkaConfig.DefaultReplicationFactorProp(), "2");
    return props;
  }

  @Override
  public void testUpdatingMetricsTopicConfig() {
   // Skip this test since it is flaky due to undetermined time to propagate metadata.
  }

  @Override
  public File trustStoreFile() {
    return _trustStoreFile;
  }

  @Override
  public SecurityProtocol securityProtocol() {
    return SecurityProtocol.SSL;
  }

  private String appendPrefix(Object key) {
    return CruiseControlMetricsReporterConfig.config((String) key);
  }

}
