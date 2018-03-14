/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;


public class DiskCapacityGoal extends CapacityGoal {

  /**
   * Constructor for Disk Capacity Goal.
   */
  public DiskCapacityGoal() {
    super();
  }

  /**
   * Package private for unit test.
   */
  DiskCapacityGoal(BalancingConstraint constraint) {
    super(constraint);
  }

  @Override
  protected Resource resource() {
    return Resource.DISK;
  }

  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    // Leadership movement won't cause disk utilization change.
    return action.balancingAction() == ActionType.LEADERSHIP_MOVEMENT ? ACCEPT : super.actionAcceptance(action, clusterModel);
  }

  @Override
  public String name() {
    return DiskCapacityGoal.class.getSimpleName();
  }
}
