/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.AbstractCruiseControlResponse;
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
public class BrokerStats extends AbstractCruiseControlResponse {
  private static final String HOST = "Host";
  private static final String HOSTS = "hosts";
  private static final String BROKERS = "brokers";
  private final List<SingleBrokerStats> _brokerStats;
  private final SortedMap<String, BasicStats> _hostStats;
  private int _hostFieldLength;
  private String _cachedPlainTextResponse;
  private String _cachedJSONResponse;
  private boolean _isBrokerStatsEstimated;

  public BrokerStats() {
    _brokerStats = new ArrayList<>();
    _hostStats = new ConcurrentSkipListMap<>();
    _hostFieldLength = 0;
    _cachedPlainTextResponse = null;
    _cachedJSONResponse = null;
    _isBrokerStatsEstimated = false;
  }

  public void addSingleBrokerStats(String host, int id, Broker.State state, double diskUtil, double cpuUtil, double leaderBytesInRate,
                                   double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate,
                                   int numReplicas, int numLeaders, boolean isEstimated, double capacity) {

    SingleBrokerStats singleBrokerStats =
        new SingleBrokerStats(host, id, state, diskUtil, cpuUtil, leaderBytesInRate, followerBytesInRate, bytesOutRate,
                              potentialBytesOutRate, numReplicas, numLeaders, isEstimated, capacity);
    _brokerStats.add(singleBrokerStats);
    _hostFieldLength = Math.max(_hostFieldLength, host.length());
    _hostStats.computeIfAbsent(host, h -> new BasicStats(0.0, 0.0, 0.0, 0.0,
                                                         0.0, 0.0, 0, 0, 0.0))
              .addBasicStats(singleBrokerStats.basicStats());
    _isBrokerStatsEstimated = _isBrokerStatsEstimated || isEstimated;
  }

  public boolean isBrokerStatsEstimated() {
    return _isBrokerStatsEstimated;
  }

  private String getJSONString() {
    Gson gson = new Gson();
    Map<String, Object> jsonStructure = getJsonStructure();
    jsonStructure.put(VERSION, JSON_VERSION);
    return gson.toJson(jsonStructure);
  }

  /**
   * Return an object that can be further be used to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    List<Map<String, Object>> hostStats = new ArrayList<>(_hostStats.size());

    // host level statistics
    for (Map.Entry<String, BasicStats> entry : _hostStats.entrySet()) {
      Map<String, Object> hostEntry = entry.getValue().getJSONStructure();
      hostEntry.put(HOST, entry.getKey());
      hostStats.add(hostEntry);
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
    // put host stats.
    sb.append(String.format("%n%" + _hostFieldLength + "s%26s%15s%25s%25s%20s%20s%20s%n",
                            "HOST", "DISK(MB)/_(%)_", "CPU(%)", "LEADER_NW_IN(KB/s)",
                            "FOLLOWER_NW_IN(KB/s)", "NW_OUT(KB/s)", "PNW_OUT(KB/s)", "LEADERS/REPLICAS"));
    for (Map.Entry<String, BasicStats> entry : _hostStats.entrySet()) {
      BasicStats stats = entry.getValue();
      sb.append(String.format("%" + _hostFieldLength + "s,%19.3f/%05.2f,%14.3f,%24.3f,%24.3f,%19.3f,%19.3f,%14d/%d%n",
                              entry.getKey(),
                              stats.diskUtil(),
                              stats.diskUtilPct(),
                              stats.cpuUtil(),
                              stats.leaderBytesInRate(),
                              stats.followerBytesInRate(),
                              stats.bytesOutRate(),
                              stats.potentialBytesOutRate(),
                              stats.numLeaders(),
                              stats.numReplicas()));
    }

    // put broker stats.
    sb.append(String.format("%n%n%" + _hostFieldLength + "s%15s%26s%15s%25s%25s%20s%20s%20s%n",
                            "HOST", "BROKER", "DISK(MB)/_(%)_", "CPU(%)", "LEADER_NW_IN(KB/s)",
                            "FOLLOWER_NW_IN(KB/s)", "NW_OUT(KB/s)", "PNW_OUT(KB/s)", "LEADERS/REPLICAS"));
    for (SingleBrokerStats stats : _brokerStats) {
      sb.append(String.format("%" + _hostFieldLength + "s,%14d,%19.3f/%05.2f,%14.3f,%24.3f,%24.3f,%19.3f,%19.3f,%14d/%d%n",
                              stats.host(),
                              stats.id(),
                              stats.basicStats().diskUtil(),
                              stats.basicStats().diskUtilPct(),
                              stats.basicStats().cpuUtil(),
                              stats.basicStats().leaderBytesInRate(),
                              stats.basicStats().followerBytesInRate(),
                              stats.basicStats().bytesOutRate(),
                              stats.basicStats().potentialBytesOutRate(),
                              stats.basicStats().numLeaders(),
                              stats.basicStats().numReplicas()));
    }

    return sb.toString();
  }
}