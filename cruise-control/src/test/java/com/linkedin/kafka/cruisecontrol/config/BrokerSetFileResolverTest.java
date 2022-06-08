/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for {@link BrokerSetFileResolver}
 */
public class BrokerSetFileResolverTest {
  /**
   * Helper method to load config file
   *
   * @param configFileName brokerSet config file name
   * @param clazz          class object
   * @return Data store Object
   */
  private static BrokerSetResolver getBrokerSetResolver(String configFileName, Class<?> clazz) {
    BrokerSetResolver brokerSetResolver = new BrokerSetFileResolver();
    String fileName = Objects.requireNonNull(clazz.getClassLoader().getResource(configFileName)).getFile();
    Map<String, String> configs =
        Map.of(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "bootstrap.servers", ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG, "connect:1234",
               AnalyzerConfig.BROKER_SET_CONFIG_FILE_CONFIG, fileName);
    brokerSetResolver.configure(configs);

    return brokerSetResolver;
  }

  /**
   * Tests if the brokerSets.json is parsable and returns information correctly.
   */
  @Test
  public void testParseBrokerSetFile() throws BrokerSetResolutionException {
    BrokerSetResolver brokerSetResolver = getBrokerSetResolver("testBrokerSets.json", this.getClass());
    final Map<String, Set<Integer>> brokerSets = brokerSetResolver.brokerIdsByBrokerSetId(DeterministicCluster.brokerSetSatisfiable1());

    assertNotNull(brokerSets);

    assertTrue(brokerSets.containsKey("Blue"));
    assertTrue(brokerSets.containsKey("Green"));

    assertEquals(Set.of(0, 1, 2), brokerSets.get("Blue"));
    assertEquals(Set.of(3, 4, 5), brokerSets.get("Green"));
  }

  /**
   * Tests if a non-existent file throws exception in loading
   */
  @Test
  public void testNonExistentFile() {
    BrokerSetResolver brokerSetResolver = new BrokerSetFileResolver();
    String fileName = "testBrokerSetz.json";
    Map<String, String> configs =
        Map.of(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "bootstrap.servers", ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG, "connect:1234",
               AnalyzerConfig.BROKER_SET_CONFIG_FILE_CONFIG, fileName);
    brokerSetResolver.configure(configs);
    assertThrows(IllegalArgumentException.class,
                 () -> brokerSetResolver.brokerIdsByBrokerSetId(DeterministicCluster.brokerSetSatisfiable1()));
  }

  /**
   * Tests if the cluster model has extra brokers that are unresolved from broker set mapping then throws broker set resoltuion exception
   */
  @Test
  public void testBrokerSetResolutionExceptionWithNoOpDefaultAssignmentPolicy() {
    BrokerSetResolver brokerSetResolver = getBrokerSetResolver("testBrokerSets.json", this.getClass());
    assertThrows(BrokerSetResolutionException.class,
                 () -> brokerSetResolver.brokerIdsByBrokerSetId(DeterministicCluster.brokerSetUnSatisfiable3()));
  }
}
