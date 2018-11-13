/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import java.util.HashMap;
import java.util.Map;


class BasicStats {
  private static final String DISK_MB = "DiskMB";
  private static final String CPU_PCT = "CpuPct";
  private static final String LEADER_NW_IN_RATE = "LeaderNwInRate";
  private static final String FOLLOWER_NW_IN_RATE = "FollowerNwInRate";
  private static final String NW_OUT_RATE = "NwOutRate";
  private static final String PNW_OUT_RATE = "PnwOutRate";
  private static final String REPLICAS = "Replicas";
  private static final String LEADERS = "Leaders";
  private double _diskUtil;
  private double _cpuUtil;
  private double _leaderBytesInRate;
  private double _followerBytesInRate;
  private double _bytesOutRate;
  private double _potentialBytesOutRate;
  private int _numReplicas;
  private int _numLeaders;

  BasicStats(double diskUtil, double cpuUtil, double leaderBytesInRate,
             double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate,
             int numReplicas, int numLeaders) {
    _diskUtil = diskUtil < 0.0 ? 0.0 : diskUtil;
    _cpuUtil = cpuUtil < 0.0 ? 0.0 : cpuUtil;
    _leaderBytesInRate = leaderBytesInRate < 0.0 ? 0.0 : leaderBytesInRate;
    _followerBytesInRate = followerBytesInRate < 0.0 ? 0.0 : followerBytesInRate;
    _bytesOutRate = bytesOutRate < 0.0 ? 0.0 : bytesOutRate;
    _potentialBytesOutRate =  potentialBytesOutRate < 0.0 ? 0.0 : potentialBytesOutRate;
    _numReplicas = numReplicas < 1 ? 0 : numReplicas;
    _numLeaders =  numLeaders < 1 ? 0 : numLeaders;
  }

  double diskUtil() {
    return _diskUtil;
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

  void addBasicStats(BasicStats basicStats) {
    _diskUtil += basicStats.diskUtil();
    _cpuUtil += basicStats.cpuUtil();
    _leaderBytesInRate += basicStats.leaderBytesInRate();
    _followerBytesInRate += basicStats.followerBytesInRate();
    _bytesOutRate += basicStats.bytesOutRate();
    _potentialBytesOutRate  += basicStats.potentialBytesOutRate();
    _numReplicas += basicStats.numReplicas();
    _numLeaders += basicStats.numLeaders();
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJSONStructure() {
    Map<String, Object> entry = new HashMap<>(8);
    entry.put(DISK_MB, diskUtil());
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
