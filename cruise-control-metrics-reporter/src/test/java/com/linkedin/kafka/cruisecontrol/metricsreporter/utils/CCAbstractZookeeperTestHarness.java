/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;


public abstract class CCAbstractZookeeperTestHarness {
  protected CCEmbeddedZookeeper _zookeeper = null;

  /**
   * Setup the unit test.
   */
  public void setUp() {
    if (_zookeeper == null) {
      _zookeeper = new CCEmbeddedZookeeper();
    }
  }

  /**
   * Teardown the unit test.
   */
  public void tearDown() {
    if (_zookeeper != null) {
      CCKafkaTestUtils.quietly(() -> _zookeeper.close());
      _zookeeper = null;
    }
  }

  protected CCEmbeddedZookeeper zookeeper() {
    return _zookeeper;
  }

  protected String zkConnect() {
    return zookeeper().connectionString();
  }
}
