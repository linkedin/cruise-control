/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.test.TestSslUtils;


public class CCEmbeddedBrokerBuilder {
  private final static AtomicInteger BROKER_ID_COUNTER = new AtomicInteger();

  //mandatory fields
  private int nodeId = BROKER_ID_COUNTER.incrementAndGet();
  private String zkConnect;
  //storage config
  private File logDirectory;
  //networking config
  private int plaintextPort = -1;
  private int sslPort = -1;
  private File trustStore;
  private long socketTimeout = 1500;
  //feature control
  private boolean enableControlledShutdown;
  private long controlledShutdownRetryBackoff = 100;
  private boolean enableDeleteTopic;
  private boolean enableLogCleaner;
  //resource management
  private long logCleanerDedupBufferSize = 2097152; //2MB
  private String rack;

  public CCEmbeddedBrokerBuilder() {
  }

  public CCEmbeddedBrokerBuilder nodeId(int nodeId) {
    this.nodeId = nodeId;
    return this;
  }

  public CCEmbeddedBrokerBuilder zkConnect(CCEmbeddedZookeeper zk) {
    this.zkConnect = zk.getConnectionString();
    return this;
  }

  public CCEmbeddedBrokerBuilder logDirectory(File logDirectory) {
    this.logDirectory = logDirectory;
    return this;
  }

  public CCEmbeddedBrokerBuilder enable(SecurityProtocol protocol) {
    switch (protocol) {
      case PLAINTEXT:
        enablePlaintext();
        break;
      case SSL:
        enableSsl();
        break;
      default:
        throw new IllegalStateException("unhandled: " + protocol);
    }
    return this;
  }

  public CCEmbeddedBrokerBuilder enablePlaintext() {
    this.plaintextPort = 0;
    return this;
  }

  public CCEmbeddedBrokerBuilder sslPort(int sslPort) {
    this.sslPort = sslPort;
    return this;
  }

  public CCEmbeddedBrokerBuilder enableSsl() {
    return sslPort(0);
  }

  public CCEmbeddedBrokerBuilder trustStore(File trustStore) {
    this.trustStore = trustStore;
    return this;
  }

  public CCEmbeddedBrokerBuilder socketTimeout(long socketTimeout) {
    this.socketTimeout = socketTimeout;
    return this;
  }

  public CCEmbeddedBrokerBuilder enableControlledShutdown(boolean enableControlledShutdown) {
    this.enableControlledShutdown = enableControlledShutdown;
    return this;
  }

  public CCEmbeddedBrokerBuilder controlledShutdownRetryBackoff(long controlledShutdownRetryBackoff) {
    this.controlledShutdownRetryBackoff = controlledShutdownRetryBackoff;
    return this;
  }

  public CCEmbeddedBrokerBuilder enableDeleteTopic(boolean enableDeleteTopic) {
    this.enableDeleteTopic = enableDeleteTopic;
    return this;
  }

  public CCEmbeddedBrokerBuilder enableLogCleaner(boolean enableLogCleaner) {
    this.enableLogCleaner = enableLogCleaner;
    return this;
  }

  public CCEmbeddedBrokerBuilder logCleanerDedupBufferSize(long logCleanerDedupBufferSize) {
    this.logCleanerDedupBufferSize = logCleanerDedupBufferSize;
    return this;
  }

  public CCEmbeddedBrokerBuilder rack(String rack) {
    this.rack = rack;
    return this;
  }

  private void applyDefaults() {
    if (logDirectory == null) {
      logDirectory = CCKafkaTestUtils.newTempDir();
    }
  }

  private void validate() throws IllegalArgumentException {
    if (plaintextPort < 0 && sslPort < 0) {
      throw new IllegalArgumentException("at least one protocol must be used");
    }
    if (logDirectory == null) {
      throw new IllegalArgumentException("log directory must be specified");
    }
    if (zkConnect == null) {
      throw new IllegalArgumentException("zkConnect must be specified");
    }
  }

  public Map<Object, Object> buildConfig() {
    applyDefaults();
    validate();

    Map<Object, Object> props = new HashMap<>();

    StringJoiner csvJoiner = new StringJoiner(",");
    if (plaintextPort >= 0) {
      csvJoiner.add(SecurityProtocol.PLAINTEXT.name + "://localhost:" + plaintextPort);
    }
    if (sslPort >= 0) {
      csvJoiner.add(SecurityProtocol.SSL.name + "://localhost:" + sslPort);
    }
    props.put("broker.id", Integer.toString(nodeId));
    props.put("listeners", csvJoiner.toString());
    props.put("log.dir", logDirectory.getAbsolutePath());
    props.put("zookeeper.connect", zkConnect);
    props.put("replica.socket.timeout.ms", Long.toString(socketTimeout));
    props.put("controller.socket.timeout.ms", Long.toString(socketTimeout));
    props.put("controlled.shutdown.enable", Boolean.toString(enableControlledShutdown));
    props.put("delete.topic.enable", Boolean.toString(enableDeleteTopic));
    props.put("controlled.shutdown.retry.backoff.ms", Long.toString(controlledShutdownRetryBackoff));
    props.put("log.cleaner.dedupe.buffer.size", Long.toString(logCleanerDedupBufferSize));
    props.put("log.cleaner.enable", Boolean.toString(enableLogCleaner));
    props.put("offsets.topic.replication.factor", "1");
    if (rack != null) {
      props.put("broker.rack", rack);
    }
    if (trustStore != null || sslPort > 0) {
      try {
        props.putAll(TestSslUtils.createSslConfig(false, true, Mode.SERVER, trustStore, "server" + nodeId));
        //switch interbroker to ssl
        props.put("security.inter.broker.protocol", "SSL");
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    return props;
  }

  public CCEmbeddedBroker build() {
    return new CCEmbeddedBroker(buildConfig());
  }
}