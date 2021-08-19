/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;


public class CCEmbeddedZookeeper implements AutoCloseable {
  private final String _hostAddress;
  private final int _port;
  private final ZooKeeperServer _zk;
  private final ServerCnxnFactory _cnxnFactory;
  private final CountDownLatch _shutdownLatch = null;

  public CCEmbeddedZookeeper() {
    int tickTime = 500;
    try {
      File snapshotDir = CCKafkaTestUtils.newTempDir();
      File logDir = CCKafkaTestUtils.newTempDir();
      _zk = new ZooKeeperServer(snapshotDir, logDir, tickTime);
      _cnxnFactory = new NIOServerCnxnFactory();
      InetAddress localHost = InetAddress.getLocalHost();
      _hostAddress = localHost.getHostAddress();
      InetSocketAddress bindAddress = new InetSocketAddress(localHost, 0);
      _cnxnFactory.configure(bindAddress, 0);
      _cnxnFactory.startup(_zk);
      _port = _zk.getClientPort();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    //sanity check
    if (_zk.getClientPort() != _port) {
      throw new IllegalStateException();
    }
  }

  public String hostAddress() {
    return _hostAddress;
  }

  public int port() {
    return _port;
  }

  public String connectionString() {
    return _hostAddress + ":" + _port;
  }

  @Override
  public void close() {
    CCKafkaTestUtils.quietly(_zk::shutdown);
    CCKafkaTestUtils.quietly(_cnxnFactory::shutdown);
    if (_shutdownLatch != null) {
      CCKafkaTestUtils.quietly(() -> _shutdownLatch.await());
    }
  }
}
