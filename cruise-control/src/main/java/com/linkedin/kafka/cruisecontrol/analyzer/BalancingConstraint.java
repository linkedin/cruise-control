/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * A class that holds the information of balancing constraint of resources, balance and capacity thresholds, and self
 * healing distribution threshold multiplier.
 */
public class BalancingConstraint {
  private final List<Resource> _resources;
  private final Map<Resource, Double> _resourceBalancePercentage;
  private final Double _replicaBalancePercentage;
  private final Double _leaderReplicaBalancePercentage;
  private final Double _topicReplicaBalancePercentage;
  private final Double _goalViolationDistributionThresholdMultiplier;
  private final Map<Resource, Double> _capacityThreshold;
  private final Map<Resource, Double> _lowUtilizationThreshold;
  private final Long _maxReplicasPerBroker;

  /**
   * Constructor for Balancing Constraint.
   * (1) Sets resources in descending order of balancing priority.
   * (2) Initializes balance percentages, capacity thresholds, and the maximum number of replicas per broker with the
   * corresponding default values.
   */
  public BalancingConstraint(KafkaCruiseControlConfig config) {
    _resources = Collections.unmodifiableList(Arrays.asList(Resource.DISK, Resource.NW_IN, Resource.NW_OUT, Resource.CPU));
    _resourceBalancePercentage = new HashMap<>(_resources.size());
    _capacityThreshold = new HashMap<>(_resources.size());
    _lowUtilizationThreshold = new HashMap<>(_resources.size());

    // Set default values for balance percentages.
    _resourceBalancePercentage.put(Resource.DISK, config.getDouble(AnalyzerConfig.DISK_BALANCE_THRESHOLD_CONFIG));
    _resourceBalancePercentage.put(Resource.CPU, config.getDouble(AnalyzerConfig.CPU_BALANCE_THRESHOLD_CONFIG));
    _resourceBalancePercentage.put(Resource.NW_IN, config.getDouble(AnalyzerConfig.NETWORK_INBOUND_BALANCE_THRESHOLD_CONFIG));
    _resourceBalancePercentage.put(Resource.NW_OUT, config.getDouble(AnalyzerConfig.NETWORK_OUTBOUND_BALANCE_THRESHOLD_CONFIG));
    // Set default values for alive resource capacity threshold.
    _capacityThreshold.put(Resource.DISK, config.getDouble(AnalyzerConfig.DISK_CAPACITY_THRESHOLD_CONFIG));
    _capacityThreshold.put(Resource.CPU, config.getDouble(AnalyzerConfig.CPU_CAPACITY_THRESHOLD_CONFIG));
    _capacityThreshold.put(Resource.NW_IN, config.getDouble(AnalyzerConfig.NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG));
    _capacityThreshold.put(Resource.NW_OUT, config.getDouble(AnalyzerConfig.NETWORK_OUTBOUND_CAPACITY_THRESHOLD_CONFIG));
    // Set low utilization threshold
    _lowUtilizationThreshold.put(Resource.DISK, config.getDouble(AnalyzerConfig.DISK_LOW_UTILIZATION_THRESHOLD_CONFIG));
    _lowUtilizationThreshold.put(Resource.CPU, config.getDouble(AnalyzerConfig.CPU_LOW_UTILIZATION_THRESHOLD_CONFIG));
    _lowUtilizationThreshold.put(Resource.NW_IN, config.getDouble(AnalyzerConfig.NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG));
    _lowUtilizationThreshold.put(Resource.NW_OUT, config.getDouble(AnalyzerConfig.NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG));
    // Set default value for the maximum number of replicas per broker.
    _maxReplicasPerBroker = config.getLong(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG);
    // Set default value for the balance percentage of (1) replica, (2) leader replica and (3) topic replica distribution.
    _replicaBalancePercentage = config.getDouble(AnalyzerConfig.REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG);
    _leaderReplicaBalancePercentage = config.getDouble(AnalyzerConfig.LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG);
    _topicReplicaBalancePercentage = config.getDouble(AnalyzerConfig.TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG);
    _goalViolationDistributionThresholdMultiplier = config.getDouble(AnalyzerConfig.GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG);
  }

  Properties setProps(Properties props) {
    props.put(AnalyzerConfig.DISK_BALANCE_THRESHOLD_CONFIG, _resourceBalancePercentage.get(Resource.DISK).toString());
    props.put(AnalyzerConfig.CPU_BALANCE_THRESHOLD_CONFIG, _resourceBalancePercentage.get(Resource.CPU).toString());
    props.put(AnalyzerConfig.NETWORK_INBOUND_BALANCE_THRESHOLD_CONFIG, _resourceBalancePercentage.get(Resource.NW_IN).toString());
    props.put(AnalyzerConfig.NETWORK_OUTBOUND_BALANCE_THRESHOLD_CONFIG, _resourceBalancePercentage.get(Resource.NW_OUT).toString());

    props.put(AnalyzerConfig.DISK_CAPACITY_THRESHOLD_CONFIG, _capacityThreshold.get(Resource.DISK).toString());
    props.put(AnalyzerConfig.CPU_CAPACITY_THRESHOLD_CONFIG, _capacityThreshold.get(Resource.CPU).toString());
    props.put(AnalyzerConfig.NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG, _capacityThreshold.get(Resource.NW_IN).toString());
    props.put(AnalyzerConfig.NETWORK_OUTBOUND_CAPACITY_THRESHOLD_CONFIG, _capacityThreshold.get(Resource.NW_OUT).toString());

    props.put(AnalyzerConfig.DISK_LOW_UTILIZATION_THRESHOLD_CONFIG, _lowUtilizationThreshold.get(Resource.DISK).toString());
    props.put(AnalyzerConfig.CPU_LOW_UTILIZATION_THRESHOLD_CONFIG, _lowUtilizationThreshold.get(Resource.CPU).toString());
    props.put(AnalyzerConfig.NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG, _lowUtilizationThreshold.get(Resource.NW_IN).toString());
    props.put(AnalyzerConfig.NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG, _lowUtilizationThreshold.get(Resource.NW_OUT).toString());

    props.put(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, _maxReplicasPerBroker.toString());
    props.put(AnalyzerConfig.REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG, _replicaBalancePercentage.toString());
    props.put(AnalyzerConfig.LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG, _leaderReplicaBalancePercentage.toString());
    props.put(AnalyzerConfig.TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG, _topicReplicaBalancePercentage.toString());
    props.put(AnalyzerConfig.GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG,
              _goalViolationDistributionThresholdMultiplier.toString());
    return props;
  }

  /**
   * @return The balancing for different resources.
   */
  public List<Resource> resources() {
    return _resources;
  }

  /**
   * @return The maximum number of replicas per broker.
   */
  public Long maxReplicasPerBroker() {
    return _maxReplicasPerBroker;
  }

  /**
   * @return The replica balance percentage for {@link com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal}.
   */
  public Double replicaBalancePercentage() {
    return _replicaBalancePercentage;
  }

  /**
   * @return The leader replica balance percentage for {@link com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal}.
   */
  public Double leaderReplicaBalancePercentage() {
    return _leaderReplicaBalancePercentage;
  }

  /**
   * @return Topic replica balance percentage for
   * {@link com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal}.
   */
  public Double topicReplicaBalancePercentage() {
    return _topicReplicaBalancePercentage;
  }

  /**
   * @return Goal violation distribution threshold multiplier to be used in detection and fixing goal violations.
   */
  public Double goalViolationDistributionThresholdMultiplier() {
    return _goalViolationDistributionThresholdMultiplier;
  }

  /**
   * Get the balance percentage for the requested resource. We give a balance margin to avoid the case
   * that right after a rebalance we need to issue another rebalance.
   *
   * @param resource Resource for which the balance percentage will be provided.
   * @return Resource balance percentage.
   */
  public double resourceBalancePercentage(Resource resource) {
    return _resourceBalancePercentage.get(resource);
  }

  /**
   * Get the capacity threshold for the requested resource.
   *
   * @param resource Resource for which the capacity threshold will be provided.
   * @return Capacity threshold for the requested resource.
   */
  public double capacityThreshold(Resource resource) {
    return _capacityThreshold.get(resource);
  }

  /**
   * Get the low utilization threshold for a resource.
   * @param resource Resource for which the low utilization threshold will be provided.
   * @return The low utilization threshold.
   */
  public double lowUtilizationThreshold(Resource resource) {
    return _lowUtilizationThreshold.get(resource);
  }

  /**
   * Set resource balance percentage for the given resource.
   *
   * @param resource Resource for which the balance percentage will be set.
   * @param balancePercentage Balance percentage for the given resource.
   */
  private void setBalancePercentageFor(Resource resource, double balancePercentage) {
    if (balancePercentage < 1) {
      throw new IllegalArgumentException("Balance Percentage cannot be less than 1.0");
    }
    _resourceBalancePercentage.put(resource, balancePercentage);
  }

  /**
   * Set a common resource balance percentage for all resources.
   *
   * @param resourceBalancePercentage Common balance percentage for all resources.
   */
  void setResourceBalancePercentage(double resourceBalancePercentage) {
    for (Resource resource : _resources) {
      setBalancePercentageFor(resource, resourceBalancePercentage);
    }
  }

  /**
   * Set alive resource capacity threshold for the given resource.
   *
   * @param resource Resource for which the capacity threshold will be set.
   * @param capacityThreshold Capacity threshold for the given resource.
   */
  private void setCapacityThresholdFor(Resource resource, double capacityThreshold) {
    if (capacityThreshold <= 0 || capacityThreshold > 1) {
      throw new IllegalArgumentException("Capacity Threshold must be in (0, 1].");
    }
    _capacityThreshold.put(resource, capacityThreshold);
  }

  /**
   * Set alive resource capacity threshold for all resources.
   *
   * @param capacityThreshold Common capacity threshold for all resources in alive brokers.
   */
  void setCapacityThreshold(double capacityThreshold) {
    for (Resource resource : _resources) {
      setCapacityThresholdFor(resource, capacityThreshold);
    }
  }

  /**
   * Get string representation of {@link BalancingConstraint}.
   */
  @Override
  public String toString() {
    return String.format("BalancingConstraint[cpuBalancePercentage=%.4f,diskBalancePercentage=%.4f,"
                         + "inboundNwBalancePercentage=%.4f,outboundNwBalancePercentage=%.4f,cpuCapacityThreshold=%.4f,"
                         + "diskCapacityThreshold=%.4f,inboundNwCapacityThreshold=%.4f,outboundNwCapacityThreshold=%.4f,"
                         + "maxReplicasPerBroker=%d,replicaBalancePercentage=%.4f,leaderReplicaBalancePercentage=%.4f,"
                         + "topicReplicaBalancePercentage=%.4f,goalViolationDistributionThresholdMultiplier=%.4f]",
                         _resourceBalancePercentage.get(Resource.CPU), _resourceBalancePercentage.get(Resource.DISK),
                         _resourceBalancePercentage.get(Resource.NW_IN), _resourceBalancePercentage.get(Resource.NW_OUT),
                         _capacityThreshold.get(Resource.CPU), _capacityThreshold.get(Resource.DISK),
                         _capacityThreshold.get(Resource.NW_IN), _capacityThreshold.get(Resource.NW_OUT),
                         _maxReplicasPerBroker, _replicaBalancePercentage, _leaderReplicaBalancePercentage,
                         _topicReplicaBalancePercentage, _goalViolationDistributionThresholdMultiplier);
  }
}
