/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import java.util.HashMap;
import java.util.Map;

@JsonResponseClass
public class BasicStats {
  @JsonResponseField
  protected static final String DISK_MB = "DiskMB";
  @JsonResponseField
  protected static final String DISK_PCT = "DiskPct";
  @JsonResponseField
  protected static final String CPU_PCT = "CpuPct";
  @JsonResponseField
  protected static final String LEADER_NW_IN_RATE = "LeaderNwInRate";
  @JsonResponseField
  protected static final String FOLLOWER_NW_IN_RATE = "FollowerNwInRate";
  @JsonResponseField
  protected static final String NW_OUT_RATE = "NwOutRate";
  @JsonResponseField
  protected static final String PNW_OUT_RATE = "PnwOutRate";
  @JsonResponseField
  protected static final String REPLICAS = "Replicas";
  @JsonResponseField
  protected static final String LEADERS = "Leaders";
  @JsonResponseField
  protected static final String DISK_CAPACITY_MB = "DiskCapacityMB";
  @JsonResponseField
  protected static final String NETWORK_IN_CAPACITY = "NetworkInCapacity";
  @JsonResponseField
  protected static final String NETWORK_OUT_CAPACITY = "NetworkOutCapacity";
  @JsonResponseField
  protected static final String NUM_CORE = "NumCore";
  protected double _diskUtil;
  protected double _cpuUtil;
  protected double _leaderBytesInRate;
  protected double _followerBytesInRate;
  protected double _bytesOutRate;
  protected double _potentialBytesOutRate;
  protected int _numReplicas;
  protected int _numLeaders;
  protected double _diskCapacity;
  protected double _networkInCapacity;
  protected double _networkOutCapacity;
  protected int _numCore;

  BasicStats(double diskUtil, double cpuUtil, double leaderBytesInRate, double followerBytesInRate, double bytesOutRate,
             double potentialBytesOutRate, int numReplicas, int numLeaders, double diskCapacity, double networkInCapacity,
             double networkOutCapacity, int numCore) {
    _diskUtil = Math.max(diskUtil, 0.0);
    _cpuUtil = Math.max(cpuUtil, 0.0);
    _leaderBytesInRate = Math.max(leaderBytesInRate, 0.0);
    _followerBytesInRate = Math.max(followerBytesInRate, 0.0);
    _bytesOutRate = Math.max(bytesOutRate, 0.0);
    _potentialBytesOutRate = Math.max(potentialBytesOutRate, 0.0);
    _numReplicas = numReplicas < 1 ? 0 : numReplicas;
    _numLeaders =  numLeaders < 1 ? 0 : numLeaders;
    _diskCapacity = Math.max(diskCapacity, 0.0);
    _networkInCapacity = Math.max(networkInCapacity, 0.0);
    _networkOutCapacity = Math.max(networkOutCapacity, 0.0);
    _numCore = Math.max(numCore, 0);
  }

  double diskUtil() {
    return _diskUtil;
  }

  // Return -1 if total disk space is invalid. Since unit is in percent, will return the digits without
  // percent sign. e.g. return 99.9 for 99.9%
  double diskUtilPct() {
    return _diskCapacity > 0 ? 100 * _diskUtil / _diskCapacity : -1.0;
  }

  double cpuUtil() {
    return _cpuUtil;
  }

  double leaderBytesInRate() {
    return _leaderBytesInRate;
  }

  double followerBytesInRate() {
    return _followerBytesInRate;
  }

  double bytesOutRate() {
    return _bytesOutRate;
  }

  double potentialBytesOutRate() {
    return _potentialBytesOutRate;
  }

  int numReplicas() {
    return _numReplicas;
  }

  int numLeaders() {
    return _numLeaders;
  }

  double diskCapacity() {
    return _diskCapacity;
  }

  double networkInCapacity() {
    return _networkInCapacity;
  }

  double networkOutCapacity() {
    return _networkOutCapacity;
  }

  int numCore() {
    return _numCore;
  }

  void addBasicStats(BasicStats basicStats) {
    _diskUtil += basicStats.diskUtil();
    _cpuUtil += basicStats.cpuUtil();
    _leaderBytesInRate += basicStats.leaderBytesInRate();
    _followerBytesInRate += basicStats.followerBytesInRate();
    _bytesOutRate += basicStats.bytesOutRate();
    _potentialBytesOutRate  += basicStats.potentialBytesOutRate();
    _numReplicas += basicStats.numReplicas();
    _numLeaders += basicStats.numLeaders();
    _diskCapacity += basicStats.diskCapacity();
    _numCore += basicStats.numCore();
    _networkInCapacity += basicStats.networkInCapacity();
    _networkOutCapacity += basicStats.networkOutCapacity();
  }

  /**
   * Return an object that can be further used to encode into JSON.
   *
   * @return The map describing basic statistics.
   */
  public Map<String, Object> getJSONStructure() {
    Map<String, Object> entry = new HashMap<>(13);
    entry.put(DISK_MB, diskUtil());
    entry.put(DISK_PCT, diskUtilPct());
    entry.put(CPU_PCT, cpuUtil());
    entry.put(LEADER_NW_IN_RATE, leaderBytesInRate());
    entry.put(FOLLOWER_NW_IN_RATE, followerBytesInRate());
    entry.put(NW_OUT_RATE, bytesOutRate());
    entry.put(PNW_OUT_RATE, potentialBytesOutRate());
    entry.put(REPLICAS, numReplicas());
    entry.put(LEADERS, numLeaders());
    entry.put(DISK_CAPACITY_MB, diskCapacity());
    entry.put(NETWORK_IN_CAPACITY, networkInCapacity());
    entry.put(NETWORK_OUT_CAPACITY, networkOutCapacity());
    entry.put(NUM_CORE, numCore());
    return entry;
  }
}
