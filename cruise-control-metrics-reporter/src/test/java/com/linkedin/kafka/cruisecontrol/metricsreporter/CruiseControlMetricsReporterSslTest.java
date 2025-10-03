/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import javax.net.ssl.KeyManagerFactory;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCContainerizedKraftCluster;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Assert;

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
    // We need to convert all the properties to the Cruise Control properties.
    setSecurityConfigs(props, "producer");
    for (String configName : ProducerConfig.configNames()) {
      Object value = props.get(configName);
      if (value != null) {
        props.remove(configName);
        props.put(appendPrefix(configName), value);
      }
    }
    props.putAll(super.overridingProps());
    props.put("listener.security.protocol.map", String.join(",",
      CCContainerizedKraftCluster.CONTROLLER_LISTENER_NAME + ":PLAINTEXT",
      CCContainerizedKraftCluster.INTERNAL_LISTENER_NAME + ":SSL",
      CCContainerizedKraftCluster.EXTERNAL_LISTENER_NAME + ":SSL"));
    // The Kafka brokers should use the same key manager algorithm as the host that generates the certs
    props.setProperty(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, KeyManagerFactory.getDefaultAlgorithm());
    return props;
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
