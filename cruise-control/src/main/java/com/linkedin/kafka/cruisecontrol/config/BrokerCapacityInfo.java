/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.util.Map;


public class BrokerCapacityInfo {
  private final Map<Resource, Double> _capacity;
  private final String _estimationInfo;

  public BrokerCapacityInfo(Map<Resource, Double> capacity, String estimationInfo) {
    _capacity = capacity;
    _estimationInfo = estimationInfo == null ? "" : estimationInfo;
  }

  public BrokerCapacityInfo(Map<Resource, Double> capacity) {
    _capacity = capacity;
    _estimationInfo = "";
  }

  /**
   * @return The broker capacity for different resource types.
   */
  public Map<Resource, Double> capacity() {
    return _capacity;
  }

  /**
   * @return True if the capacity of the broker for at least one resource is based on an estimation, false otherwise.
   */
  public boolean isEstimated() {
    return !_estimationInfo.isEmpty();
  }

  /**
   * @return Empty string if no estimation, related estimation info otherwise.
   */
  public String estimationInfo() {
    return _estimationInfo;
  }
}
