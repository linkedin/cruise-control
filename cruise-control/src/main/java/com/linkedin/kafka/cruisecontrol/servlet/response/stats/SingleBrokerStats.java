/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import com.linkedin.kafka.cruisecontrol.model.Broker;
import java.util.Map;


public class SingleBrokerStats {
  protected static final String HOST = "Host";
  protected static final String BROKER = "Broker";
  protected static final String BROKER_STATE = "BrokerState";
  protected final String _host;
  protected final int _id;
  protected final Broker.State _state;
  protected final BasicStats _basicStats;
  protected final boolean _isEstimated;

  SingleBrokerStats(String host, int id, Broker.State state, double diskUtil, double cpuUtil, double leaderBytesInRate,
                    double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate, int numReplicas,
                    int numLeaders, boolean isEstimated, double capacity) {
    _host = host;
    _id = id;
    _state = state;
    _basicStats = new BasicStats(diskUtil, cpuUtil, leaderBytesInRate, followerBytesInRate, bytesOutRate,
                                 potentialBytesOutRate, numReplicas, numLeaders, capacity);
    _isEstimated = isEstimated;
  }

  public String host() {
    return _host;
  }

  public Broker.State state() {
    return _state;
  }

  public int id() {
    return _id;
  }

  BasicStats basicStats() {
    return _basicStats;
  }

  public boolean isEstimated() {
    return _isEstimated;
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> entry = _basicStats.getJsonStructure();
    entry.put(HOST, _host);
    entry.put(BROKER, _id);
    entry.put(BROKER_STATE, _state);
    return entry;
  }
}