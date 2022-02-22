/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;

public class CCEmbeddedBroker implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(CCEmbeddedBroker.class);
  private final Map<SecurityProtocol, Integer> _ports;
  private final Map<SecurityProtocol, String> _hosts;
  private final KafkaServer _kafkaServer;
  private int _id;
  private File _logDir;

  public CCEmbeddedBroker(Map<Object, Object> config) {
    _ports = new HashMap<>();
    _hosts = new HashMap<>();

    try {
      // Also validates the config
      KafkaConfig kafkaConfig = new KafkaConfig(config, true);
      parseConfigs(config);

      _kafkaServer = createKafkaServer(kafkaConfig);

      startup();
      _ports.replaceAll((securityProtocol, port) -> {
        try {
          return _kafkaServer.boundPort(ListenerName.forSecurityProtocol(securityProtocol));
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      });
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Creates the {@link KafkaServer} instance using the appropriate constructor for the version of Kafka on the classpath.
   * It will attempt to use the 2.8+ version first and then fall back to the 2.5+ version. If neither work, a
   * {@link NoSuchElementException} will be thrown.
   *
   * @param kafkaConfig The {@link KafkaConfig} instance to be used to create the returned {@link KafkaServer} instance.
   * @return A {@link KafkaServer} instance configured with the supplied {@link KafkaConfig}.
   * @throws ClassNotFoundException If a version of {@link KafkaServer} cannot be found on the classpath.
   */
  private static KafkaServer createKafkaServer(KafkaConfig kafkaConfig) throws ClassNotFoundException {
    // The KafkaServer constructor changed in 2.8, so we need to figure out which one we are using and invoke it with the correct parameters
    KafkaServer kafkaServer = null;
    Class<?> kafkaServerClass = Class.forName(KafkaServer.class.getName());

    try {
      Constructor<?> kafka28PlusCon = kafkaServerClass.getConstructor(KafkaConfig.class, Time.class, Option.class, boolean.class);
      kafkaServer = (KafkaServer) kafka28PlusCon.newInstance(kafkaConfig, Time.SYSTEM, Option.empty(), false);
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
      LOG.debug("Unable to find Kafka 2.8+ constructor for KafkaSever class", e);
    }

    if (kafkaServer == null) {
      try {
        Constructor<?> kafka25PlusCon = kafkaServerClass.getConstructor(KafkaConfig.class, Time.class, Option.class, Seq.class);
        kafkaServer = (KafkaServer) kafka25PlusCon.newInstance(kafkaConfig, Time.SYSTEM, Option.empty(), new ArrayBuffer<KafkaMetricsReporter>());
      } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
        LOG.debug("Unable to find Kafka 2.5+ constructor for KafkaSever class", e);
      }
    }

    if (kafkaServer != null) {
      return kafkaServer;
    } else {
      throw new NoSuchElementException("Unable to find viable constructor fo the KafkaServer class");
    }
  }

  private void parseConfigs(Map<Object, Object> config) {
    _id = Integer.parseInt((String) config.get(KafkaConfig.BrokerIdProp()));
    _logDir = new File((String) config.get(KafkaConfig.LogDirProp()));

    // Bind addresses
    String listenersString = (String) config.get(KafkaConfig.ListenersProp());
    for (String protocolAddr : listenersString.split("\\s*,\\s*")) {
      try {
        URI uri = new URI(protocolAddr.trim());
        SecurityProtocol protocol = SecurityProtocol.forName(uri.getScheme());
        _hosts.put(protocol, uri.getHost());
        // We get the value after boot
        _ports.put(protocol, null);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  public int id() {
    return _id;
  }

  /**
   * @param protocol Security protocol.
   * @return Address containing host and port.
   */
  public String addr(SecurityProtocol protocol) {
    if (!_hosts.containsKey(protocol)) {
      return null;
    }
    return _hosts.get(protocol) + ":" + _ports.get(protocol);
  }

  public String plaintextAddr() {
    return addr(SecurityProtocol.PLAINTEXT);
  }

  public String sslAddr() {
    return addr(SecurityProtocol.SSL);
  }

  public void startup() {
    _kafkaServer.startup();
  }

  public void shutdown() {
    _kafkaServer.shutdown();
  }

  public void awaitShutdown() {
    _kafkaServer.awaitShutdown();
  }

  @Override
  public void close() {
    CCKafkaTestUtils.quietly(this::shutdown);
    CCKafkaTestUtils.quietly(this::awaitShutdown);
    CCKafkaTestUtils.quietly(() -> FileUtils.forceDelete(_logDir));
  }

  public static CCEmbeddedBrokerBuilder newServer() {
    return new CCEmbeddedBrokerBuilder();
  }
}
