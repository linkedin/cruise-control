/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;


public class CCEmbeddedZookeeper implements AutoCloseable {
  private File snapshotDir;
  private File logDir;
  private int tickTime;
  private String hostAddress;
  private int port;
  private ZooKeeperServer zk;
  private ServerCnxnFactory cnxnFactory;
  private CountDownLatch shutdownLatch = null;

  public CCEmbeddedZookeeper() {
    try {
      snapshotDir = CCKafkaTestUtils.newTempDir();
      logDir = CCKafkaTestUtils.newTempDir();
      tickTime = 500;
      zk = new ZooKeeperServer(snapshotDir, logDir, tickTime);
      registerShutdownHandler(zk);
      cnxnFactory = new NIOServerCnxnFactory();
      InetAddress localHost = InetAddress.getLocalHost();
      hostAddress = localHost.getHostAddress();
      InetSocketAddress bindAddress = new InetSocketAddress(localHost, port);
      cnxnFactory.configure(bindAddress, 0);
      cnxnFactory.startup(zk);
      port = zk.getClientPort();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    //sanity check
    if (zk.getClientPort() != port) {
      throw new IllegalStateException();
    }
  }

  public String getHostAddress() {
    return hostAddress;
  }

  public int getPort() {
    return port;
  }

  public String getConnectionString() {
    return hostAddress + ":" + port;
  }

  @Override
  public void close() {
    CCKafkaTestUtils.quietly(() -> zk.shutdown());
    CCKafkaTestUtils.quietly(() -> cnxnFactory.shutdown());
    if (shutdownLatch != null) {
      CCKafkaTestUtils.quietly(() -> shutdownLatch.await());
    }
  }

  /**
   * starting with ZK 3.4.9 there's a shutdown handler.
   * if one isnt registered ZK will spew errors at shutdown time, even though
   * both the handler interface and the method of registering one are not public API.
   * see https://issues.apache.org/jira/browse/ZOOKEEPER-2795
   * such craftsmanship. much wow.
   * @param zk a ZK server instance
   * @throws Exception if anything goes wrong
   */
  private void registerShutdownHandler(ZooKeeperServer zk) throws Exception {
    Class<?> handlerClass;
    try {
      handlerClass = Class.forName("org.apache.zookeeper.server.ZooKeeperServerShutdownHandler");
    } catch (ClassNotFoundException e) {
      //older ZK. forget about it
      return;
    }
    Method registerMethod = ZooKeeperServer.class.getDeclaredMethod("registerServerShutdownHandler", handlerClass);
    Constructor<?> ctr = handlerClass.getDeclaredConstructor(CountDownLatch.class);
    ctr.setAccessible(true);
    shutdownLatch = new CountDownLatch(1);
    Object handlerInstance = ctr.newInstance(shutdownLatch);
    registerMethod.setAccessible(true);
    registerMethod.invoke(zk, handlerInstance);
  }
}