/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import java.util.Map;


public class SingleBrokerUtilizationStats extends SingleBrokerStats {
  protected static final String BROKER_STATE = "BrokerState";
  protected final Broker.State _state;
  protected final BasicUtilizationStats _basicStats;

  SingleBrokerUtilizationStats(String host, int id, Broker.State state, double diskUtil, double cpuUtil, double leaderBytesInRate,
                    double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate, int numReplicas,
                    int numLeaders, boolean isEstimated, double capacity) {
        
    super(host, id, isEstimated);
    _state = state;
    _basicStats = new BasicUtilizationStats(diskUtil, cpuUtil, leaderBytesInRate, followerBytesInRate, bytesOutRate,
                                 potentialBytesOutRate, numReplicas, numLeaders, capacity);
  }

  public Broker.State state() {
    return _state;
  }

  BasicUtilizationStats basicUtilizationStats() {
    return _basicStats;
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> entry = _basicStats.getJsonStructure();
    entry.put(hostKey(), host());
    entry.put(brokerKey(), id());
    entry.put(BROKER_STATE, _state);
    return entry;
  }
}