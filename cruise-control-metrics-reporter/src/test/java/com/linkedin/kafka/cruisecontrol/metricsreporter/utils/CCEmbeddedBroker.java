/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import scala.Option;
import scala.collection.mutable.ArrayBuffer;


public class CCEmbeddedBroker implements AutoCloseable {
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
      KafkaConfig kafkaConfig = KafkaConfig.apply(config);
      parseConfigs(config);
      _kafkaServer = new KafkaServer(kafkaConfig, Time.SYSTEM, Option.empty(), new ArrayBuffer<>());
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
