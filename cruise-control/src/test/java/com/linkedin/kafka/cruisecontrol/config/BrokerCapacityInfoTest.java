/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.util.EnumMap;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * Backward-compatibility contract for {@link BrokerCapacityInfo}: existing constructors (which
 * predate the connection-count addition) must still validate the same set of {@link Resource}s
 * and report no connection-count limit. New construction sites that want to express a ceiling
 * use the explicit-connection-capacity constructor.
 */
public class BrokerCapacityInfoTest {
  private static Map<Resource, Double> fullCapacity() {
    Map<Resource, Double> m = new EnumMap<>(Resource.class);
    m.put(Resource.CPU, 100.0);
    m.put(Resource.DISK, 1024.0 * 1024.0);
    m.put(Resource.NW_IN, 1024.0);
    m.put(Resource.NW_OUT, 1024.0);
    return m;
  }

  @Test
  public void testLegacyConstructorsDefaultToUnlimitedConnectionCapacity() {
    BrokerCapacityInfo a = new BrokerCapacityInfo(fullCapacity());
    assertEquals(BrokerCapacityInfo.UNLIMITED_CONNECTION_CAPACITY, a.connectionCapacity(), 0.0);

    BrokerCapacityInfo b = new BrokerCapacityInfo(fullCapacity(), "estimation");
    assertEquals(BrokerCapacityInfo.UNLIMITED_CONNECTION_CAPACITY, b.connectionCapacity(), 0.0);

    BrokerCapacityInfo c = new BrokerCapacityInfo(fullCapacity(), 4.0 /* cores */);
    assertEquals(BrokerCapacityInfo.UNLIMITED_CONNECTION_CAPACITY, c.connectionCapacity(), 0.0);

    BrokerCapacityInfo d = new BrokerCapacityInfo(fullCapacity(), Map.of("/tmp/data", 1024.0));
    assertEquals(BrokerCapacityInfo.UNLIMITED_CONNECTION_CAPACITY, d.connectionCapacity(), 0.0);
  }

  @Test
  public void testExplicitConnectionCapacityIsRetained() {
    BrokerCapacityInfo info = new BrokerCapacityInfo(
        fullCapacity(),
        "estimation",
        Map.of("/tmp/data", 1024.0),
        4.0 /* cores */,
        250_000.0 /* connection ceiling */);
    assertEquals(250_000.0, info.connectionCapacity(), 0.0);
  }
}
