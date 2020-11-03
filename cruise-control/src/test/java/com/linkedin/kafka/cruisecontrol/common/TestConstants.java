/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T1;
import static com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomaly.TopicReplicationFactorAnomalyEntry;


public final class TestConstants {
  public static final String TOPIC0 = "topic0";
  public static final String TOPIC1 = "topic1";
  public static final String TOPIC2 = "topic2";
  public static final String TOPIC3 = "topic3";
  public static final long SEED_BASE = 3140;
  public static final long REPLICATION_SEED = 5234;
  public static final long LEADER_SEED = 72033;
  public static final long REPLICA_ASSIGNMENT_SEED = 1240;
  public static final long TOPIC_POPULARITY_SEED = 7234;
  public static final Map<Resource, Long> UTILIZATION_SEED_BY_RESOURCE;
  static {
    Map<Resource, Long> utilizationSeedByResource = new HashMap<>();
    utilizationSeedByResource.put(Resource.CPU, 100000L);
    utilizationSeedByResource.put(Resource.DISK, 300000L);
    utilizationSeedByResource.put(Resource.NW_IN, 500000L);
    utilizationSeedByResource.put(Resource.NW_OUT, 700000L);
    UTILIZATION_SEED_BY_RESOURCE = Collections.unmodifiableMap(utilizationSeedByResource);
  }

  public static final double ZERO_BALANCE_PERCENTAGE = 1.00;
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

  public static final String GOALS_VALUES;
  static {
    GOALS_VALUES = "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal,"
                   + "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal,"
                   + "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal,"
                   + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal,"
                   + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal,"
                   + "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal,"
                   + "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal,"
                   + "com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal,"
                   + "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal,"
                   + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal,"
                   + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal,"
                   + "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal,"
                   + "com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal,"
                   + "com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal,"
                   + "com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal";
  }

  public static final String DEFAULT_GOALS_VALUES;
  static {
    DEFAULT_GOALS_VALUES = "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal,"
                           + "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal,"
                           + "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal,"
                           + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal,"
                           + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal,"
                           + "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal,"
                           + "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal,"
                           + "com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal,"
                           + "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal,"
                           + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal,"
                           + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal,"
                           + "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal,"
                           + "com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal,"
                           + "com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal,"
                           + "com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal";
  }

  public enum Distribution {
    UNIFORM, LINEAR, EXPONENTIAL
  }

  private static final int NUM_SNAPSHOTS = 2;
  // Cluster properties to be used as a base. Any changes specified in modified properties will be applied to this.
  public static final Map<ClusterProperty, Number> BASE_PROPERTIES;

  static {
    Map<ClusterProperty, Number> properties = new HashMap<>();
    properties.put(ClusterProperty.NUM_RACKS, 10);
    properties.put(ClusterProperty.NUM_BROKERS, 40);
    properties.put(ClusterProperty.NUM_DEAD_BROKERS, 0);
    properties.put(ClusterProperty.NUM_BROKERS_WITH_BAD_DISK, 0);
    properties.put(ClusterProperty.NUM_REPLICAS, 50001);
    properties.put(ClusterProperty.NUM_TOPICS, 3000);
    properties.put(ClusterProperty.MIN_REPLICATION, 3);
    properties.put(ClusterProperty.MAX_REPLICATION, 3);
    properties.put(ClusterProperty.MEAN_CPU, 0.01);
    properties.put(ClusterProperty.MEAN_DISK, 100.0);
    properties.put(ClusterProperty.MEAN_NW_IN, 100.0);
    properties.put(ClusterProperty.MEAN_NW_OUT, 100.0);
    properties.put(ClusterProperty.POPULATE_REPLICA_PLACEMENT_INFO, 0);
    BASE_PROPERTIES = Collections.unmodifiableMap(properties);
  }

  // Broker and disk capacity (homogeneous cluster is assumed).
  public static final Map<Resource, Double> BROKER_CAPACITY;
  public static final Map<String, Double> DISK_CAPACITY;
  public static final String LOGDIR0 = "/mnt/i00";
  public static final String LOGDIR1 = "/mnt/i01";

  static {
    Map<Resource, Double> capacity = new HashMap<>();
    capacity.put(Resource.CPU, TestConstants.TYPICAL_CPU_CAPACITY);
    capacity.put(Resource.DISK, TestConstants.LARGE_BROKER_CAPACITY);
    capacity.put(Resource.NW_IN, TestConstants.LARGE_BROKER_CAPACITY);
    capacity.put(Resource.NW_OUT, TestConstants.MEDIUM_BROKER_CAPACITY);
    BROKER_CAPACITY = Collections.unmodifiableMap(capacity);
    // Disk capacity
    Map<String, Double> capacityByLogdir = new HashMap<>(2);
    capacityByLogdir.put(LOGDIR0, TestConstants.LARGE_BROKER_CAPACITY / 2);
    capacityByLogdir.put(LOGDIR1, TestConstants.LARGE_BROKER_CAPACITY / 2);
    DISK_CAPACITY = Collections.unmodifiableMap(capacityByLogdir);
  }

  // Broker capacity config file for test.
  public static final String JBOD_BROKER_CAPACITY_CONFIG_FILE = "testCapacityConfigJBOD.json";
  public static final String DEFAULT_BROKER_CAPACITY_CONFIG_FILE = "DefaultCapacityConfig.json";
  public static final String DEFAULT_CLUSTER_CONFIGS_FILE = "DefaultClusterConfigs.json";

  // Topic replication factor anomaly test.
  public static final TopicReplicationFactorAnomalyEntry TOPIC_REPLICATION_FACTOR_ANOMALY_ENTRY =
      new TopicReplicationFactorAnomalyEntry(T1, 0.5);
}
