/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.test.TestSslUtils;


public class CCEmbeddedBrokerBuilder {
  private static final AtomicInteger BROKER_ID_COUNTER = new AtomicInteger();

  //mandatory fields
  private int _nodeId = BROKER_ID_COUNTER.incrementAndGet();
  private String _zkConnect;
  //storage config
  private File _logDirectory;
  //networking config
  private int _plaintextPort = -1;
  private int _sslPort = -1;
  private File _trustStore;
  private long _socketTimeout = 1500;
  //feature control
  private boolean _enableControlledShutdown;
  private long _controlledShutdownRetryBackoff = 100;
  private boolean _enableDeleteTopic;
  private boolean _enableLogCleaner;
  //resource management
  private long _logCleanerDedupBufferSize = 2097152; //2MB
  private String _rack;

  public CCEmbeddedBrokerBuilder() {
  }

  public CCEmbeddedBrokerBuilder nodeId(int nodeId) {
    _nodeId = nodeId;
    return this;
  }

  public CCEmbeddedBrokerBuilder zkConnect(String zkConnect) {
    _zkConnect = zkConnect;
    return this;
  }

  public CCEmbeddedBrokerBuilder zkConnect(CCEmbeddedZookeeper zk) {
    return zkConnect(zk.connectionString());
  }

  public CCEmbeddedBrokerBuilder logDirectory(File logDirectory) {
    _logDirectory = logDirectory;
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

  public CCEmbeddedBrokerBuilder plaintextPort(int plaintextPort) {
    _plaintextPort = plaintextPort;
    return this;
  }

  public CCEmbeddedBrokerBuilder enablePlaintext() {
    return plaintextPort(0);
  }

  public CCEmbeddedBrokerBuilder sslPort(int sslPort) {
    _sslPort = sslPort;
    return this;
  }

  public CCEmbeddedBrokerBuilder enableSsl() {
    return sslPort(0);
  }

  public CCEmbeddedBrokerBuilder trustStore(File trustStore) {
    _trustStore = trustStore;
    return this;
  }

  public CCEmbeddedBrokerBuilder socketTimeout(long socketTimeout) {
    _socketTimeout = socketTimeout;
    return this;
  }

  public CCEmbeddedBrokerBuilder enableControlledShutdown(boolean enableControlledShutdown) {
    _enableControlledShutdown = enableControlledShutdown;
    return this;
  }

  public CCEmbeddedBrokerBuilder controlledShutdownRetryBackoff(long controlledShutdownRetryBackoff) {
    _controlledShutdownRetryBackoff = controlledShutdownRetryBackoff;
    return this;
  }

  public CCEmbeddedBrokerBuilder enableDeleteTopic(boolean enableDeleteTopic) {
    _enableDeleteTopic = enableDeleteTopic;
    return this;
  }

  public CCEmbeddedBrokerBuilder enableLogCleaner(boolean enableLogCleaner) {
    _enableLogCleaner = enableLogCleaner;
    return this;
  }

  public CCEmbeddedBrokerBuilder logCleanerDedupBufferSize(long logCleanerDedupBufferSize) {
    _logCleanerDedupBufferSize = logCleanerDedupBufferSize;
    return this;
  }

  public CCEmbeddedBrokerBuilder rack(String rack) {
    _rack = rack;
    return this;
  }

  private void applyDefaults() {
    if (_logDirectory == null) {
      _logDirectory = CCKafkaTestUtils.newTempDir();
    }
  }

  private void validate() throws IllegalArgumentException {
    if (_plaintextPort < 0 && _sslPort < 0) {
      throw new IllegalArgumentException("at least one protocol must be used");
    }
    if (_logDirectory == null) {
      throw new IllegalArgumentException("log directory must be specified");
    }
    if (_zkConnect == null) {
      throw new IllegalArgumentException("zkConnect must be specified");
    }
  }

  public Map<Object, Object> buildConfig() {
    applyDefaults();
    validate();

    Map<Object, Object> props = new HashMap<>();

    StringJoiner csvJoiner = new StringJoiner(",");
    if (_plaintextPort >= 0) {
      csvJoiner.add(SecurityProtocol.PLAINTEXT.name + "://localhost:" + _plaintextPort);
    }
    if (_sslPort >= 0) {
      csvJoiner.add(SecurityProtocol.SSL.name + "://localhost:" + _sslPort);
    }
    props.put(KafkaConfig.BrokerIdProp(), Integer.toString(_nodeId));
    props.put(KafkaConfig.ListenersProp(), csvJoiner.toString());
    props.put(KafkaConfig.LogDirProp(), _logDirectory.getAbsolutePath());
    props.put(KafkaConfig.ZkConnectProp(), _zkConnect);
    props.put(KafkaConfig.ReplicaSocketTimeoutMsProp(), Long.toString(_socketTimeout));
    props.put(KafkaConfig.ControllerSocketTimeoutMsProp(), Long.toString(_socketTimeout));
    props.put(KafkaConfig.ControlledShutdownEnableProp(), Boolean.toString(_enableControlledShutdown));
    props.put(KafkaConfig.DeleteTopicEnableProp(), Boolean.toString(_enableDeleteTopic));
    props.put(KafkaConfig.ControlledShutdownRetryBackoffMsProp(), Long.toString(_controlledShutdownRetryBackoff));
    props.put(KafkaConfig.LogCleanerDedupeBufferSizeProp(), Long.toString(_logCleanerDedupBufferSize));
    props.put(KafkaConfig.LogCleanerEnableProp(), Boolean.toString(_enableLogCleaner));
    props.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
    props.put(KafkaConfig.SslEndpointIdentificationAlgorithmProp(), "");
    if (_rack != null) {
      props.put(KafkaConfig.RackProp(), _rack);
    }
    if (_trustStore != null || _sslPort > 0) {
      try {
        props.putAll(TestSslUtils.createSslConfig(false, true, Mode.SERVER, _trustStore, "server" + _nodeId));
        // Switch interbroker to ssl
        props.put(KafkaConfig.InterBrokerSecurityProtocolProp(), SecurityProtocol.SSL.name);
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