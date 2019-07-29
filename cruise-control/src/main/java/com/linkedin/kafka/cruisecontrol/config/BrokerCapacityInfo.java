/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.util.Map;


public class BrokerCapacityInfo {
  public final static short DEFAULT_NUM_CPU_CORES = 1;
  private final static String DEFAULT_ESTIMATION_INFO = "";
  private final Map<Resource, Double> _capacity;
  private final String _estimationInfo;
  private final short _numCpuCores;

  /**
   * BrokerCapacityInfo with the given capacity, estimation, and number of CPU cores.
   *
   * @param capacity Capacity information for each resource.
   * @param estimationInfo Description if there is any capacity estimation, null or {@link #DEFAULT_ESTIMATION_INFO} otherwise.
   * @param numCpuCores Number of CPU cores.
   */
  public BrokerCapacityInfo(Map<Resource, Double> capacity, String estimationInfo, short numCpuCores) {
    _capacity = capacity;
    _estimationInfo = estimationInfo == null ? DEFAULT_ESTIMATION_INFO : estimationInfo;
    _numCpuCores = numCpuCores;
  }

  /**
   * BrokerCapacityInfo with default number of CPU cores.
   *
   * @param capacity Capacity information for each resource.
   * @param estimationInfo Description if there is any capacity estimation, null or {@link #DEFAULT_ESTIMATION_INFO} otherwise.
   */
  public BrokerCapacityInfo(Map<Resource, Double> capacity, String estimationInfo) {
    this(capacity, estimationInfo, DEFAULT_NUM_CPU_CORES);
  }

  /**
   * BrokerCapacityInfo with the given capacity and number of CPU cores.
   *
   * @param capacity Capacity information for each resource.
   * @param numCpuCores Number of CPU cores.
   */
  public BrokerCapacityInfo(Map<Resource, Double> capacity, short numCpuCores) {
    this(capacity, DEFAULT_ESTIMATION_INFO, numCpuCores);
  }

  /**
   * BrokerCapacityInfo with no estimation with default number of CPU cores.
   *
   * @param capacity Capacity information for each resource.
   */
  public BrokerCapacityInfo(Map<Resource, Double> capacity) {
    this(capacity, DEFAULT_ESTIMATION_INFO, DEFAULT_NUM_CPU_CORES);
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
   * @return {@link #DEFAULT_ESTIMATION_INFO} if no estimation, related estimation info otherwise.
   */
  public String estimationInfo() {
    return _estimationInfo;
  }

  /**
   * @return Number of CPU cores (if provided), {@link #DEFAULT_NUM_CPU_CORES} otherwise.
   */
  public short numCpuCores() {
    return _numCpuCores;
  }
}
