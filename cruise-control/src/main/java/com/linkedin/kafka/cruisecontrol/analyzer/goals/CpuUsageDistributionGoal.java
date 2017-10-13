/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.common.Resource;


public class CpuUsageDistributionGoal extends ResourceDistributionGoal {

  /**
   * Constructor for Resource Distribution Goal.
   */
  public CpuUsageDistributionGoal() {
    super();
  }

  /**
   * Package private for unit test.
   */
  CpuUsageDistributionGoal(BalancingConstraint constraint) {
    super(constraint);
  }

  @Override
  protected Resource resource() {
    return Resource.CPU;
  }

  @Override
  public String name() {
    return CpuUsageDistributionGoal.class.getSimpleName();
  }

  @Override
  public String goalType() {
    return _goalType.name();
  }

  @Override
  public String goalDescription() {
    return "Attempt to make the cpu-utilization variance among all the brokers are within a certain range.";
  }
}
