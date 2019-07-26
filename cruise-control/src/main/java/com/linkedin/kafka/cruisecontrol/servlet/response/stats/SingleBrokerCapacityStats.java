/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import java.util.Map;


public class SingleBrokerCapacityStats {
  private static final String HOST = "Host";
  private static final String BROKER = "Broker";
  private final String _host;
  private final int _id;
  private final BasicCapacityStats _basicCapacityStats;
  private final boolean _isEstimated;

  SingleBrokerCapacityStats(String host, int id, boolean isEstimated, double diskCap, double cpuCap, double nwInCap, double nwOutCap) {
    _host = host;
    _id = id;
    _basicCapacityStats = new BasicCapacityStats(diskCap, cpuCap, nwInCap, nwOutCap);
    _isEstimated = isEstimated;
  }

  public String host() {
    return _host;
  }

  public int id() {
    return _id;
  }

  BasicCapacityStats basicCapacityStats() {
    return _basicCapacityStats;
  }

  public boolean isEstimated() {
    return _isEstimated;
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJSONStructure() {
    Map<String, Object> entry = _basicCapacityStats.getJSONStructure();
    entry.put(HOST, _host);
    entry.put(BROKER, _id);
    return entry;
  }
}
