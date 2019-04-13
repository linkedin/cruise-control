/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

public enum ClusterProperty {
  NUM_RACKS("numRacks"),
  NUM_BROKERS("numBrokers"),
  NUM_DEAD_BROKERS("numDeadBrokers"),
  NUM_BROKERS_WITH_BAD_DISK("numBrokersWithBadDisk"),
  NUM_REPLICAS("numReplicas"),
  NUM_TOPICS("numTopics"),
  MIN_REPLICATION("minReplication"),
  MAX_REPLICATION("maxReplication"),
  MEAN_CPU("meanCpu"),
  MEAN_DISK("meanDisk"),
  MEAN_NW_IN("meanNwIn"),
  MEAN_NW_OUT("meanNwOut"),
  POPULATE_REPLICA_PLACEMENT_INFO("populateReplicaPlacementInfo");

  private final String _clusterProperty;

  ClusterProperty(String clusterProperty) {
    _clusterProperty = clusterProperty;
  }

  public String clusterProperties() {
    return _clusterProperty;
  }

  @Override
  public String toString() {
    return _clusterProperty;
  }
}
