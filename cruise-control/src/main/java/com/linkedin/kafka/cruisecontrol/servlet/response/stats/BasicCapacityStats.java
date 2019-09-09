/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import java.util.HashMap;
import java.util.Map;


class BasicCapacityStats {
    private static final String DISK_CAPACITY = "DiskCapacity";
    private static final String CPU_CAPACITY = "CpuCapacity";
    private static final String NW_IN_CAPACITY = "NwInCapacity";
    private static final String NW_OUT_CAPACITY = "NwOutCapacity";
    private final double _diskCapacity;
    private final double _cpuCapacity;
    private final double _bytesInCapacity;
    private final double _bytesOutCapacity;

  BasicCapacityStats(double diskCapacity, double cpuCapacity, double nwInCapacity, double nwOutCapacity) {
    _diskCapacity = diskCapacity < 0.0 ? -1.0 : diskCapacity;
    _cpuCapacity = cpuCapacity < 0.0 ? -1.0 : cpuCapacity;
    _bytesInCapacity = nwInCapacity < 0.0 ? -1.0 : nwInCapacity;
    _bytesOutCapacity = nwOutCapacity < 0.0 ? -1.0 : nwOutCapacity;
  }

  double diskCapacity() {
    return _diskCapacity;
  }

  double cpuCapacity() {
    return _cpuCapacity;
  }

  double bytesInCapacity() {
    return _bytesInCapacity;
  }

  double bytesOutCapacity() {
    return _bytesOutCapacity;
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> entry = new HashMap<>(4);
    entry.put(DISK_CAPACITY, diskCapacity());
    entry.put(CPU_CAPACITY, cpuCapacity());
    entry.put(NW_IN_CAPACITY, bytesInCapacity());
    entry.put(NW_OUT_CAPACITY, bytesOutCapacity());
    return entry;
  }
}
