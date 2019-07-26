/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;


/**
 * Unit test for {@link BrokerCapacityConfigFileResolver}
 */
public class BrokerCapacityConfigFileResolverTest {
  private static BrokerCapacityConfigResolver getBrokerCapacityConfigResolver(String configFileName, Class<?> klass) {
    BrokerCapacityConfigResolver configResolver = new BrokerCapacityConfigFileResolver();
    String fileName = Objects.requireNonNull(klass.getClassLoader().getResource(configFileName)).getFile();
    Map<String, String> configs = Collections.singletonMap(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE, fileName);
    configResolver.configure(configs);

    return configResolver;
  }

  @Test
  public void testParseConfigFile() {
    BrokerCapacityConfigResolver configResolver = getBrokerCapacityConfigResolver("testCapacityConfig.json", this.getClass());

    assertEquals(200000.0, configResolver.capacityForBroker("", "", 0)
                                         .capacity().get(Resource.NW_IN), 0.01);
    assertEquals(100000.0, configResolver.capacityForBroker("", "", 2)
                                         .capacity().get(Resource.NW_IN), 0.01);
    try {
      configResolver.capacityForBroker("", "", BrokerCapacityConfigFileResolver.DEFAULT_CAPACITY_BROKER_ID);
      fail("Should have thrown exception for negative broker id");
    } catch (IllegalArgumentException e) {
      // let it go
    }

    assertTrue(configResolver.capacityForBroker("", "", 2).isEstimated());
    assertTrue(configResolver.capacityForBroker("", "", 2).estimationInfo().length() > 0);
  }

  @Test
  public void testParseConfigJBODFile() {
    BrokerCapacityConfigResolver configResolver = getBrokerCapacityConfigResolver("testCapacityConfigJBOD.json", this.getClass());

    assertEquals(2000000.0, configResolver.capacityForBroker("", "", 0)
                                         .capacity().get(Resource.DISK), 0.01);
    assertEquals(2200000.0, configResolver.capacityForBroker("", "", 3)
                                         .capacity().get(Resource.DISK), 0.01);
    assertEquals(200000.0, configResolver.capacityForBroker("", "", 3)
                                         .diskCapacityByLogDir().get("/tmp/kafka-logs-4"), 0.01);

    assertFalse(configResolver.capacityForBroker("", "", 2).isEstimated());
    assertTrue(configResolver.capacityForBroker("", "", 3).isEstimated());
    assertTrue(configResolver.capacityForBroker("", "", 3).estimationInfo().length() > 0);
  }

  @Test
  public void testParseConfigCoresFile() {
    BrokerCapacityConfigResolver configResolver = getBrokerCapacityConfigResolver("testCapacityConfigCores.json", this.getClass());

    assertEquals(BrokerCapacityConfigFileResolver.DEFAULT_CPU_CAPACITY_WITH_CORES, configResolver
        .capacityForBroker("", "", 0).capacity().get(Resource.CPU), 0.01);
    assertEquals(BrokerCapacityConfigFileResolver.DEFAULT_CPU_CAPACITY_WITH_CORES, configResolver
        .capacityForBroker("", "", 3).capacity().get(Resource.CPU), 0.01);

    assertEquals(8, configResolver.capacityForBroker("", "", 0).numCpuCores());
    assertEquals(64, configResolver.capacityForBroker("", "", 1).numCpuCores());
    assertEquals(16, configResolver.capacityForBroker("", "", 3).numCpuCores());

    assertFalse(configResolver.capacityForBroker("", "", 1).isEstimated());
    assertTrue(configResolver.capacityForBroker("", "", 3).isEstimated());
    assertTrue(configResolver.capacityForBroker("", "", 3).estimationInfo().length() > 0);
  }
}
