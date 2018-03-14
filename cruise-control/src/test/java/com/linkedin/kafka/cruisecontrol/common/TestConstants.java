/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class TestConstants {
  static final long SEED_BASE = 3140;
  static final long REPLICATION_SEED = 5234;
  static final long LEADER_SEED = 72033;
  static final long REPLICA_ASSIGNMENT_SEED = 1240;
  static final long TOPIC_POPULARITY_SEED = 7234;
  static final Map<Resource, Long> UTILIZATION_SEED_BY_RESOURCE;
  static {
    Map<Resource, Long> utilizationSeedByResource = new HashMap<>();
    utilizationSeedByResource.put(Resource.CPU, 100000L);
    utilizationSeedByResource.put(Resource.DISK, 300000L);
    utilizationSeedByResource.put(Resource.NW_IN, 500000L);
    utilizationSeedByResource.put(Resource.NW_OUT, 700000L);
    UTILIZATION_SEED_BY_RESOURCE = Collections.unmodifiableMap(utilizationSeedByResource);
  }

  public static final double LOW_BALANCE_PERCENTAGE = 1.05;
  public static final double MEDIUM_BALANCE_PERCENTAGE = 1.25;
  public static final double HIGH_BALANCE_PERCENTAGE = 1.65;
  public static final double HIGH_CAPACITY_THRESHOLD = 0.9;
  public static final double MEDIUM_CAPACITY_THRESHOLD = 0.8;
  public static final double LOW_CAPACITY_THRESHOLD = 0.7;
  public static final double LARGE_BROKER_CAPACITY = 300000.0;
  public static final double TYPICAL_CPU_CAPACITY = 100.0;
  public static final double MEDIUM_BROKER_CAPACITY = 200000.0;
  public static final double SMALL_BROKER_CAPACITY = 10.0;

  private TestConstants() {

  }

  public enum Distribution {
    UNIFORM, LINEAR, EXPONENTIAL
  }

  private final static int NUM_SNAPSHOTS = 2;
  // Cluster properties to be used as a base. Any changes specified in modified properties will be applied to this.
  public final static Map<ClusterProperty, Number> BASE_PROPERTIES;

  static {
    Map<ClusterProperty, Number> properties = new HashMap<>();
    properties.put(ClusterProperty.NUM_RACKS, 10);
    properties.put(ClusterProperty.NUM_BROKERS, 40);
    properties.put(ClusterProperty.NUM_DEAD_BROKERS, 0);
    properties.put(ClusterProperty.NUM_REPLICAS, 50001);
    properties.put(ClusterProperty.NUM_TOPICS, 3000);
    properties.put(ClusterProperty.MIN_REPLICATION, 3);
    properties.put(ClusterProperty.MAX_REPLICATION, 3);
    properties.put(ClusterProperty.MEAN_CPU, 0.01);
    properties.put(ClusterProperty.MEAN_DISK, 100.0);
    properties.put(ClusterProperty.MEAN_NW_IN, 100.0);
    properties.put(ClusterProperty.MEAN_NW_OUT, 100.0);
    BASE_PROPERTIES = Collections.unmodifiableMap(properties);

  }

  // Broker capacity (homogeneous cluster is assumed).
  public final static Map<Resource, Double> BROKER_CAPACITY;

  static {
    Map<Resource, Double> capacity = new HashMap<>();
    capacity.put(Resource.CPU, TestConstants.TYPICAL_CPU_CAPACITY);
    capacity.put(Resource.DISK, TestConstants.LARGE_BROKER_CAPACITY);
    capacity.put(Resource.NW_IN, TestConstants.LARGE_BROKER_CAPACITY);
    capacity.put(Resource.NW_OUT, TestConstants.MEDIUM_BROKER_CAPACITY);
    BROKER_CAPACITY = Collections.unmodifiableMap(capacity);
  }
}
