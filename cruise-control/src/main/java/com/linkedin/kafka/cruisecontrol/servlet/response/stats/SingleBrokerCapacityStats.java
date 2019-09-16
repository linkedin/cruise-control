/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import java.util.Map;


public class SingleBrokerCapacityStats extends SingleBrokerStats {
  private final BasicCapacityStats _basicCapacityStats;

  SingleBrokerCapacityStats(String host, int id, boolean isEstimated, double diskCap, double cpuCap, double nwInCap, double nwOutCap) {
    super(host, id, isEstimated);
    _basicCapacityStats = new BasicCapacityStats(diskCap, cpuCap, nwInCap, nwOutCap);
  }

  BasicCapacityStats basicCapacityStats() {
    return _basicCapacityStats;
  }
  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> entry = _basicCapacityStats.getJsonStructure();
    entry.put(hostKey(), host());
    entry.put(brokerKey(), id());
    return entry;
  }
}
