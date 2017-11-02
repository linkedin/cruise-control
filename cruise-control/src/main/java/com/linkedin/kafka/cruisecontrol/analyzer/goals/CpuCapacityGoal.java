/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.common.Resource;


public class CpuCapacityGoal extends CapacityGoal {

  /**
   * Constructor for Cpu Capacity Goal.
   */
  public CpuCapacityGoal() {
    super();
  }

  /**
   * Package private for unit test.
   */
  CpuCapacityGoal(BalancingConstraint constraint) {
    super(constraint);
  }

  @Override
  protected Resource resource() {
    return Resource.CPU;
  }

  @Override
  public String name() {
    return CpuCapacityGoal.class.getSimpleName();
  }
}
