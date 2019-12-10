/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.DiskStats;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import java.util.HashMap;
import java.util.Map;

@JsonResponseClass
public class SingleBrokerStats extends BasicStats {
  @JsonResponseField
  protected static final String HOST = "Host";
  @JsonResponseField
  protected static final String BROKER = "Broker";
  @JsonResponseField
  protected static final String BROKER_STATE = "BrokerState";
  @JsonResponseField(required = false)
  protected static final String DISK_STATE = "DiskState";
  protected final String _host;
  protected final int _id;
  protected final Broker.State _state;
  protected final boolean _isEstimated;
  protected final Map<String, DiskStats> _diskStatsByLogdir;

  SingleBrokerStats(String host, int id, Broker.State state, double diskUtil, double cpuUtil, double leaderBytesInRate,
                    double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate, int numReplicas,
                    int numLeaders, boolean isEstimated, double diskCapacity, Map<String, DiskStats> diskStatsByLogdir) {
    super(diskUtil, cpuUtil, leaderBytesInRate, followerBytesInRate, bytesOutRate,
          potentialBytesOutRate, numReplicas, numLeaders, diskCapacity);
    _host = host;
    _id = id;
    _state = state;
    _isEstimated = isEstimated;
    _diskStatsByLogdir = diskStatsByLogdir;
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

  /**
   * Get per-logdir disk statistics of the broker.
   *
   * @return The per-logdir disk statistics. This method is relevant only when the
   *         {@link com.linkedin.kafka.cruisecontrol.model.ClusterModel} has been created with a request to populate
   *         replica placement info, otherwise returns an empty map.
   */
  public Map<String, DiskStats> diskStatsByLogdir() {
    return _diskStatsByLogdir;
  }

  /**
   * @return True if the broker capacity has been estimated, false otherwise.
   */
  public boolean isEstimated() {
    return _isEstimated;
  }

  /**
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJSONStructure() {
    Map<String, Object> entry = super.getJSONStructure();
    entry.put(HOST, _host);
    entry.put(BROKER, _id);
    entry.put(BROKER_STATE, _state);
    if (!_diskStatsByLogdir.isEmpty()) {
      Map<String, Object>  diskStates = new HashMap<>(_diskStatsByLogdir.size());
      _diskStatsByLogdir.forEach((k, v) -> diskStates.put(k, v.getJSONStructure()));
      entry.put(DISK_STATE, diskStates);
    }
    return entry;
  }
}