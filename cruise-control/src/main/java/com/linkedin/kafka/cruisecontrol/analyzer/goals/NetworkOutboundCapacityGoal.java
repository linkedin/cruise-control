/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.common.Resource;


public class NetworkOutboundCapacityGoal extends CapacityGoal {

  /**
   * Constructor for Network Outbound Capacity Goal.
   */
  public NetworkOutboundCapacityGoal() {
    super();
  }

  /**
   * Package private for unit test.
   */
  NetworkOutboundCapacityGoal(BalancingConstraint constraint) {
    super(constraint);
  }

  @Override
  protected Resource resource() {
    return Resource.NW_OUT;
  }

  @Override
  public String name() {
    return NetworkOutboundCapacityGoal.class.getSimpleName();
  }
}
