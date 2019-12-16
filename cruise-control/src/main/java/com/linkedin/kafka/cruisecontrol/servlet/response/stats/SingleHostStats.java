/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import java.util.Map;

@JsonResponseClass
public class SingleHostStats extends BasicStats {
  @JsonResponseField
  protected static final String HOST = "Host";
  private String _host;

  SingleHostStats(String host, double diskUtil, double cpuUtil, double leaderBytesInRate,
                  double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate,
                  int numReplicas, int numLeaders, double capacity) {
    super(diskUtil, cpuUtil, leaderBytesInRate, followerBytesInRate, bytesOutRate,
          potentialBytesOutRate, numReplicas, numLeaders, capacity);
    _host = host;
  }

  /**
   * Return an object that can be further used to encode into JSON.
   *
   * @return The map describing host statistics.
   */
  public Map<String, Object> getJSONStructure() {
    Map<String, Object> hostEntry = super.getJSONStructure();
    hostEntry.put(HOST, _host);
    return hostEntry;
  }
}
