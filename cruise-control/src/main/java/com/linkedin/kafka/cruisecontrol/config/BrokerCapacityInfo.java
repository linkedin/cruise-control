/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.util.Map;


public class BrokerCapacityInfo {
  private final Map<Resource, Double> _capacity;
  private final String _estimationInfo;
  private final Map<String, Double> _diskCapacityByLogDir;

  /**
   * BrokerCapacityInfo with the given capacity, estimation, and per absolute logDir disk capacity information.
   *
   * @param capacity Capacity information for each resource.
   * @param estimationInfo Description if there is any capacity estimation, null or empty string otherwise.
   * @param diskCapacityByLogDir Disk capacity by absolute logDir.
   */
  public BrokerCapacityInfo(Map<Resource, Double> capacity, String estimationInfo,
                            Map<String, Double> diskCapacityByLogDir) {
    _capacity = capacity;
    _estimationInfo = estimationInfo == null ? "" : estimationInfo;
    _diskCapacityByLogDir = diskCapacityByLogDir;
  }

  /**
   * BrokerCapacityInfo with no capacity information specified per absolute logDir.
   *
   * @param capacity Capacity information for each resource.
   * @param estimationInfo Description if there is any capacity estimation, null or empty string otherwise.
   */
  public BrokerCapacityInfo(Map<Resource, Double> capacity, String estimationInfo) {
    this(capacity, estimationInfo, null);
  }

  /**
   * BrokerCapacityInfo with no estimation.
   *
   * @param capacity Capacity information for each resource.
   * @param diskCapacityByLogDir Disk capacity by absolute logDir.
   */
  public BrokerCapacityInfo(Map<Resource, Double> capacity, Map<String, Double> diskCapacityByLogDir) {
    this(capacity, null, diskCapacityByLogDir);
  }

  /**
   * BrokerCapacityInfo with no estimation, no capacity information specified per absolute logDir.
   *
   * @param capacity Capacity information for each resource.
   */
  public BrokerCapacityInfo(Map<Resource, Double> capacity) {
    this(capacity, null, null);
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

  /**
   * @return Disk capacity by absolute logDir if the capacity is specified per logDir, null otherwise.
   */
  public Map<String, Double> diskCapacityByLogDir() {
    return _diskCapacityByLogDir;
  }
}
