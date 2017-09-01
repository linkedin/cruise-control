/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * A class that holds the information of balancing constraint of resources in descending order of balancing priority,
 * balance percentage and capacity threshold for each resource.
 */
public class BalancingConstraint {
  private final List<Resource> _resources;
  private final Map<Resource, Double> _balancePercentage;
  private final Map<Resource, Double> _capacityThreshold;
  private final Map<Resource, Double> _lowUtilizationThreshold;

  /**
   * Constructor for Balancing Constraint.
   * (1) Sets resources in descending order of balancing priority.
   * (2) Initializes balance percentage and capacity threshold with corresponding default values.
   */
  public BalancingConstraint(KafkaCruiseControlConfig config) {
    _resources = Collections.unmodifiableList(Arrays.asList(Resource.DISK, Resource.NW_IN, Resource.NW_OUT, Resource.CPU));
    _balancePercentage = new HashMap<>();
    _capacityThreshold = new HashMap<>();
    _lowUtilizationThreshold = new HashMap<>();

    // Set default values for balance percentages.
    _balancePercentage.put(Resource.DISK, config.getDouble(KafkaCruiseControlConfig.DISK_BALANCE_THRESHOLD_CONFIG));
    _balancePercentage.put(Resource.CPU, config.getDouble(KafkaCruiseControlConfig.CPU_BALANCE_THRESHOLD_CONFIG));
    _balancePercentage.put(Resource.NW_IN, config.getDouble(KafkaCruiseControlConfig.NETWORK_INBOUND_BALANCE_THRESHOLD_CONFIG));
    _balancePercentage.put(Resource.NW_OUT, config.getDouble(KafkaCruiseControlConfig.NETWORK_OUTBOUND_BALANCE_THRESHOLD_CONFIG));
    // Set default values for healthy resource capacity threshold.
    _capacityThreshold.put(Resource.DISK, config.getDouble(KafkaCruiseControlConfig.DISK_CAPACITY_THRESHOLD_CONFIG));
    _capacityThreshold.put(Resource.CPU, config.getDouble(KafkaCruiseControlConfig.CPU_CAPACITY_THRESHOLD_CONFIG));
    _capacityThreshold.put(Resource.NW_IN, config.getDouble(KafkaCruiseControlConfig.NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG));
    _capacityThreshold.put(Resource.NW_OUT, config.getDouble(KafkaCruiseControlConfig.NETWORK_OUTBOUND_CAPACITY_THRESHOLD_CONFIG));
    // Set low utilization threshold
    _lowUtilizationThreshold.put(Resource.DISK, config.getDouble(KafkaCruiseControlConfig.DISK_LOW_UTILIZATION_THRESHOLD_CONFIG));
    _lowUtilizationThreshold.put(Resource.CPU, config.getDouble(KafkaCruiseControlConfig.CPU_LOW_UTILIZATION_THRESHOLD_CONFIG));
    _lowUtilizationThreshold.put(Resource.NW_IN, config.getDouble(KafkaCruiseControlConfig.NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG));
    _lowUtilizationThreshold.put(Resource.NW_OUT, config.getDouble(KafkaCruiseControlConfig.NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG));
  }

  Properties setProps(Properties props) {
    props.put(KafkaCruiseControlConfig.DISK_BALANCE_THRESHOLD_CONFIG, _balancePercentage.get(Resource.DISK).toString());
    props.put(KafkaCruiseControlConfig.CPU_BALANCE_THRESHOLD_CONFIG, _balancePercentage.get(Resource.CPU).toString());
    props.put(KafkaCruiseControlConfig.NETWORK_INBOUND_BALANCE_THRESHOLD_CONFIG, _balancePercentage.get(Resource.NW_IN).toString());
    props.put(KafkaCruiseControlConfig.NETWORK_OUTBOUND_BALANCE_THRESHOLD_CONFIG, _balancePercentage.get(Resource.NW_OUT).toString());

    props.put(KafkaCruiseControlConfig.DISK_CAPACITY_THRESHOLD_CONFIG, _capacityThreshold.get(Resource.DISK).toString());
    props.put(KafkaCruiseControlConfig.CPU_CAPACITY_THRESHOLD_CONFIG, _capacityThreshold.get(Resource.CPU).toString());
    props.put(KafkaCruiseControlConfig.NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG, _capacityThreshold.get(Resource.NW_IN).toString());
    props.put(KafkaCruiseControlConfig.NETWORK_OUTBOUND_CAPACITY_THRESHOLD_CONFIG, _capacityThreshold.get(Resource.NW_OUT).toString());

    props.put(KafkaCruiseControlConfig.DISK_LOW_UTILIZATION_THRESHOLD_CONFIG, _lowUtilizationThreshold.get(Resource.DISK).toString());
    props.put(KafkaCruiseControlConfig.CPU_LOW_UTILIZATION_THRESHOLD_CONFIG, _lowUtilizationThreshold.get(Resource.CPU).toString());
    props.put(KafkaCruiseControlConfig.NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG, _lowUtilizationThreshold.get(Resource.NW_IN).toString());
    props.put(KafkaCruiseControlConfig.NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG, _lowUtilizationThreshold.get(Resource.NW_OUT).toString());
    return props;
  }

  /**
   * Get the balancing for different resources.
   */
  public List<Resource> resources() {
    return _resources;
  }

  /**
   * Get the balance percentage for the requested resource. We give a balance margin to avoid the case
   * that right after a rebalance we need to issue another rebalance.
   *
   * @param resource Resource for which the balance percentage will be provided.
   * @return Resource balance percentage.
   */
  public double balancePercentage(Resource resource) {
    return _balancePercentage.get(resource);
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
   * @return the low utilization threshold.
   */
  public double lowUtilizationThreshold(Resource resource) {
    return _lowUtilizationThreshold.get(resource);
  }

  /**
   * Set resource balance percentage for the given resource.
   *
   * @param resource          Resource for which the balance percentage will be set.
   * @param balancePercentage Balance percentage for the given resource.
   * @throws AnalysisInputException
   */
  private void setBalancePercentageFor(Resource resource, double balancePercentage) throws AnalysisInputException {
    if (balancePercentage < 1) {
      throw new AnalysisInputException("Balance Percentage cannot be less than 1.0");
    }
    _balancePercentage.put(resource, balancePercentage);
  }

  /**
   * Set a common resource balance percentage for all resources.
   *
   * @param balancePercentage Common balance percentage for all resources.
   */
  void setBalancePercentage(double balancePercentage) throws AnalysisInputException {
    for (Resource resource : _resources) {
      setBalancePercentageFor(resource, balancePercentage);
    }
  }

  /**
   * Set healthy resource capacity threshold for the given resource.
   *
   * @param resource          Resource for which the capacity threshold will be set.
   * @param capacityThreshold Capacity threshold for the given resource.
   * @throws AnalysisInputException
   */
  private void setCapacityThresholdFor(Resource resource, double capacityThreshold) throws AnalysisInputException {
    if (capacityThreshold <= 0 || capacityThreshold > 1) {
      throw new AnalysisInputException("Capacity Threshold must be in (0, 1].");
    }
    _capacityThreshold.put(resource, capacityThreshold);
  }

  /**
   * Set healthy resource capacity threshold for all resources.
   *
   * @param capacityThreshold Common capacity threshold for all resources in healthy brokers.
   */
  void setCapacityThreshold(double capacityThreshold) throws AnalysisInputException {
    for (Resource resource : _resources) {
      setCapacityThresholdFor(resource, capacityThreshold);
    }
  }

  /**
   * Get string representation of Balancing Constraint in XML format.
   */
  @Override
  public String toString() {
    return String.format("<BalancingConstraint cpuBalancePercentage=\"%.4f\" diskBalancePercentage=\"%.4f\" " +
            "inboundNwBalancePercentage=\"%.4f\" outboundNwBalancePercentage=\"%.4f\" cpuCapacityThreshold=\"%.4f\" "
            + "diskCapacityThreshold=\"%.4f\" inboundNwCapacityThreshold=\"%.4f\" outboundNwCapacityThreshold=\"%.4f\">"
            + "%n</BalancingConstraint>%n", _balancePercentage.get(Resource.CPU), _balancePercentage.get(Resource.DISK),
        _balancePercentage.get(Resource.NW_IN), _balancePercentage.get(Resource.NW_OUT),
        _capacityThreshold.get(Resource.CPU), _capacityThreshold.get(Resource.DISK),
        _capacityThreshold.get(Resource.NW_IN), _capacityThreshold.get(Resource.NW_OUT));
  }
}
