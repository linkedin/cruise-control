/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Get broker level stats related to capacity in human readable format.
 */
public class BrokerCapacityStats extends BrokerStats {
  private final List<SingleBrokerCapacityStats> _brokerCapacityStats;

  public BrokerCapacityStats(KafkaCruiseControlConfig config) {
    super(config);
    _brokerCapacityStats = new ArrayList<>();
  }

  public void addSingleBrokerCapacityStats(String host, int id, boolean isEstimated, double diskCap, double cpuCap, double nwInCap,
                                            double nwOutCap) {
    SingleBrokerCapacityStats singleBrokerCapacityStats = 
        new SingleBrokerCapacityStats(host, id, isEstimated, diskCap, cpuCap, nwInCap, nwOutCap);
    _brokerCapacityStats.add(singleBrokerCapacityStats);
    _isBrokerStatsEstimated = _isBrokerStatsEstimated || isEstimated;
    _hostFieldLength = Math.max(_hostFieldLength, host.length());
  }

  @Override
  protected void discardIrrelevantResponse() {
    // Discard irrelevant response.
    _brokerCapacityStats.clear();
  }

  /**
   * Return an object that can be further be used to encode into JSON
   */
  @Override
  public Map<String, Object> getJsonStructure() {
    // broker level statistics
    List<Map<String, Object>> brokerCapacityStats = new ArrayList<>(_brokerCapacityStats.size());
    for (SingleBrokerCapacityStats stats : _brokerCapacityStats) {
      Map<String, Object> brokerEntry = stats.getJSONStructure();
      brokerCapacityStats.add(brokerEntry);
    }

    Map<String, Object> stats = new HashMap<>(1);
    stats.put(BROKERS, brokerCapacityStats);
    return stats;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    // put broker stats.
    sb.append(String.format("%n%n%" + _hostFieldLength + "s%15s%22s%18s%22s%23s%n",
                            "HOST", "BROKER", "DISK_CAPACITY(MB)", "CPU_CAPACITY(MB)", "NW_IN_CAPACITY(KB/s)",
                            "NW_OUT_CAPACITY(KB/s)"));
    for (SingleBrokerCapacityStats stats : _brokerCapacityStats) {
      sb.append(String.format("%" + _hostFieldLength + "s,%14d,%19.3f,%14.3f,%19.3f,%19.3f%n",
                              stats.host(),
                              stats.id(),
                              stats.basicCapacityStats().diskCapacity(),
                              stats.basicCapacityStats().cpuCapacity(),
                              stats.basicCapacityStats().bytesInCapacity(),
                              stats.basicCapacityStats().bytesOutCapacity()));
    }

    return sb.toString();
  }
}
