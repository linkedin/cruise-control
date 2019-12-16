/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.DiskStats;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.AbstractCruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;


/**
 * Get broker level stats in human readable format.
 */
@JsonResponseClass
public class BrokerStats extends AbstractCruiseControlResponse {
  @JsonResponseField
  protected static final String HOSTS = "hosts";
  @JsonResponseField
  protected static final String BROKERS = "brokers";
  protected final List<SingleBrokerStats> _brokerStats;
  protected final SortedMap<String, SingleHostStats> _hostStats;
  protected int _hostFieldLength;
  protected int _logdirFieldLength;
  protected String _cachedPlainTextResponse;
  protected String _cachedJSONResponse;
  protected boolean _isBrokerStatsEstimated;

  public BrokerStats(KafkaCruiseControlConfig config) {
    super(config);
    _brokerStats = new ArrayList<>();
    _hostStats = new ConcurrentSkipListMap<>();
    _hostFieldLength = 0;
    _logdirFieldLength = 1;
    _cachedPlainTextResponse = null;
    _cachedJSONResponse = null;
    _isBrokerStatsEstimated = false;
  }

  /**
   * Add single broker stats.
   *
   * @param host Host name.
   * @param id Broker id.
   * @param state Broker state.
   * @param diskUtil Disk utilization.
   * @param cpuUtil CPU utilization.
   * @param leaderBytesInRate Leader bytes in rate.
   * @param followerBytesInRate Follower bytes in rate.
   * @param bytesOutRate Bytes out rate.
   * @param potentialBytesOutRate Potential bytes out rate.
   * @param numReplicas Number of replicas.
   * @param numLeaders Number of leaders.
   * @param isEstimated True if the broker capacity is estimated, false otherwise.
   * @param diskCapacity The disk capacity of broker.
   * @param diskStatsByLogdir Disk stats by logdir.
   */
  public void addSingleBrokerStats(String host, int id, Broker.State state, double diskUtil, double cpuUtil, double leaderBytesInRate,
                                   double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate,
                                   int numReplicas, int numLeaders, boolean isEstimated, double diskCapacity,
                                   Map<String, DiskStats> diskStatsByLogdir) {

    SingleBrokerStats singleBrokerStats =
        new SingleBrokerStats(host, id, state, diskUtil, cpuUtil, leaderBytesInRate, followerBytesInRate, bytesOutRate,
                              potentialBytesOutRate, numReplicas, numLeaders, isEstimated, diskCapacity, diskStatsByLogdir);
    _brokerStats.add(singleBrokerStats);
    _hostFieldLength = Math.max(_hostFieldLength, host.length());
    // Calculate field length to print logdir name in plaintext response, a padding of 10 is added for this field.
    // If there is no logdir information, this field will be of length of 1.
    _logdirFieldLength = Math.max(_logdirFieldLength,
                                  diskStatsByLogdir.keySet().stream().mapToInt(String::length).max().orElse(-10) + 10);
    _hostStats.computeIfAbsent(host, h -> new SingleHostStats(host, 0.0, 0.0, 0.0, 0.0,
                                                              0.0, 0.0, 0, 0, 0.0))
              .addBasicStats(singleBrokerStats);
    _isBrokerStatsEstimated = _isBrokerStatsEstimated || isEstimated;
  }

  public boolean isBrokerStatsEstimated() {
    return _isBrokerStatsEstimated;
  }

  protected String getJSONString() {
    Gson gson = new Gson();
    Map<String, Object> jsonStructure = getJsonStructure();
    jsonStructure.put(VERSION, JSON_VERSION);
    return gson.toJson(jsonStructure);
  }

  /**
   * @return An object that can be further be used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure() {
    List<Map<String, Object>> hostStats = new ArrayList<>(_hostStats.size());

    // host level statistics
    for (Map.Entry<String, SingleHostStats> entry : _hostStats.entrySet()) {
      hostStats.add(entry.getValue().getJSONStructure());
    }

    // broker level statistics
    List<Map<String, Object>> brokerStats = new ArrayList<>(_brokerStats.size());
    for (SingleBrokerStats stats : _brokerStats) {
      Map<String, Object> brokerEntry = stats.getJSONStructure();
      brokerStats.add(brokerEntry);
    }

    // consolidated
    Map<String, Object> stats = new HashMap<>(2);
    stats.put(HOSTS, hostStats);
    stats.put(BROKERS, brokerStats);
    return stats;
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedJSONResponse = getJSONString();
    _cachedPlainTextResponse = toString();
    // Discard irrelevant response.
    _brokerStats.clear();
    _hostStats.clear();
  }

  @Override
  public void discardIrrelevantResponse(CruiseControlParameters parameters) {
    if (_cachedJSONResponse == null || _cachedPlainTextResponse == null) {
      discardIrrelevantAndCacheRelevant(parameters);
      if (_cachedJSONResponse == null || _cachedPlainTextResponse == null) {
        throw new IllegalStateException("Failed to cache the relevant response.");
      }
    }
    _cachedResponse = parameters.json() ? _cachedJSONResponse : _cachedPlainTextResponse;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    boolean hasDiskInfo = !_brokerStats.get(0).diskStatsByLogdir().isEmpty();

    // put broker stats.
    sb.append(String.format("%n%n%" + _hostFieldLength + "s%15s%" + _logdirFieldLength + "s%26s%15s%25s%25s%20s%20s%20s%n",
                            "HOST", "BROKER", hasDiskInfo ? "LOGDIR" : "", "DISK(MB)/_(%)_", "CPU(%)", "LEADER_NW_IN(KB/s)",
                            "FOLLOWER_NW_IN(KB/s)", "NW_OUT(KB/s)", "PNW_OUT(KB/s)", "LEADERS/REPLICAS"));
    for (SingleBrokerStats stats : _brokerStats) {
      sb.append(String.format("%" + _hostFieldLength + "s,%14d,%" + _logdirFieldLength
                              + "s%19.3f/%05.2f,%14.3f,%24.3f,%24.3f,%19.3f,%19.3f,%14d/%d%n",
                              stats.host(),
                              stats.id(),
                              "",
                              stats.diskUtil(),
                              stats.diskUtilPct(),
                              stats.cpuUtil(),
                              stats.leaderBytesInRate(),
                              stats.followerBytesInRate(),
                              stats.bytesOutRate(),
                              stats.potentialBytesOutRate(),
                              stats.numLeaders(),
                              stats.numReplicas()));
      // If disk information is populated, put disk stats.
      if (hasDiskInfo) {
        Map<String, DiskStats> capacityByDisk = stats.diskStatsByLogdir();
        for (Map.Entry<String, DiskStats> entry : capacityByDisk.entrySet()) {
          DiskStats diskStats = entry.getValue();
          Double util = diskStats.utilization();
          sb.append(String.format("%" + (_hostFieldLength + 15 + _logdirFieldLength) + "s,"
                                  + (util == null ? "%19s/%5s," : "%19.3f/%05.2f,") + "%119d/%d%n",
                                  entry.getKey(),
                                  util == null ? "DEAD" : util,
                                  util == null ? "DEAD" : diskStats.utilizationPercentage(),
                                  diskStats.numLeaderReplicas(),
                                  diskStats.numReplicas()));
        }
      }
    }
    return sb.toString();
  }
}