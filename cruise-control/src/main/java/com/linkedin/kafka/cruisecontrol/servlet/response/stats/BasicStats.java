/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import java.util.HashMap;
import java.util.Map;


class BasicStats {
  protected static final String DISK_MB = "DiskMB";
  protected static final String DISK_PCT = "DiskPct";
  protected static final String CPU_PCT = "CpuPct";
  protected static final String LEADER_NW_IN_RATE = "LeaderNwInRate";
  protected static final String FOLLOWER_NW_IN_RATE = "FollowerNwInRate";
  protected static final String NW_OUT_RATE = "NwOutRate";
  protected static final String PNW_OUT_RATE = "PnwOutRate";
  protected static final String REPLICAS = "Replicas";
  protected static final String LEADERS = "Leaders";
  protected double _diskUtil;
  protected double _cpuUtil;
  protected double _leaderBytesInRate;
  protected double _followerBytesInRate;
  protected double _bytesOutRate;
  protected double _potentialBytesOutRate;
  protected int _numReplicas;
  protected int _numLeaders;
  protected double _diskCapacity;

  BasicStats(double diskUtil, double cpuUtil, double leaderBytesInRate,
             double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate,
             int numReplicas, int numLeaders, double diskCapacity) {
    _diskUtil = diskUtil < 0.0 ? 0.0 : diskUtil;
    _cpuUtil = cpuUtil < 0.0 ? 0.0 : cpuUtil;
    _leaderBytesInRate = leaderBytesInRate < 0.0 ? 0.0 : leaderBytesInRate;
    _followerBytesInRate = followerBytesInRate < 0.0 ? 0.0 : followerBytesInRate;
    _bytesOutRate = bytesOutRate < 0.0 ? 0.0 : bytesOutRate;
    _potentialBytesOutRate =  potentialBytesOutRate < 0.0 ? 0.0 : potentialBytesOutRate;
    _numReplicas = numReplicas < 1 ? 0 : numReplicas;
    _numLeaders =  numLeaders < 1 ? 0 : numLeaders;
    _diskCapacity = diskCapacity < 0.0 ? 0.0 : diskCapacity;
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
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> entry = new HashMap<>(9);
    entry.put(DISK_MB, diskUtil());
    entry.put(DISK_PCT, diskUtilPct());
    entry.put(CPU_PCT, cpuUtil());
    entry.put(LEADER_NW_IN_RATE, leaderBytesInRate());
    entry.put(FOLLOWER_NW_IN_RATE, followerBytesInRate());
    entry.put(NW_OUT_RATE, bytesOutRate());
    entry.put(PNW_OUT_RATE, potentialBytesOutRate());
    entry.put(REPLICAS, numReplicas());
    entry.put(LEADERS, numLeaders());
    return entry;
  }
}
