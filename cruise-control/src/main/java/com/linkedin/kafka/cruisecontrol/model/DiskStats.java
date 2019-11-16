/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import java.util.HashMap;
import java.util.Map;


/**
 * A helper class to store statistics about the {@link Disk}.
 */
public class DiskStats {
  private final int _numLeaderReplicas;
  private final int _numReplicas;
  // For dead disk, its utilization will be null.
  private final Double _utilization;
  private final double _capacity;
  private static final String DISK_MB = "DiskMB";
  private static final String DISK_PCT = "DiskPct";
  private static final String NUM_LEADER_REPLICAS = "NumLeaderReplicas";
  private static final String NUM_REPLICAS = "NumReplicas";
  private static final String DEAD_STATE = "DEAD";

  DiskStats(int numLeaderReplicas, int numReplicas, double utilization, double capacity) {
    _numLeaderReplicas = numLeaderReplicas;
    _numReplicas = numReplicas;
    _utilization = capacity > 0 ? utilization : null;
    _capacity = capacity;
  }

  public int numLeaderReplicas()  {
    return _numLeaderReplicas;
  }

  public int numReplicas()  {
    return _numReplicas;
  }

  public Double utilization()  {
    return _utilization;
  }

  public double capacity()  {
    return _capacity;
  }

  public Double utilizationPercentage() {
    return  _utilization == null ? null : _utilization * 100.0 / _capacity;
  }

  /**
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJSONStructure()   {
    Map<String, Object> entry = new HashMap<>(4);
    entry.put(DISK_MB, _utilization == null ? DEAD_STATE : _utilization);
    entry.put(DISK_PCT, _utilization == null ? DEAD_STATE : utilizationPercentage());
    entry.put(NUM_LEADER_REPLICAS, _numLeaderReplicas);
    entry.put(NUM_REPLICAS, _numReplicas);
    return entry;
  }
}
