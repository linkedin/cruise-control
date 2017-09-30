/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;


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
}
