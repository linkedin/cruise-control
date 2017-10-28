/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;


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
  public boolean isProposalAcceptable(BalancingProposal proposal, ClusterModel clusterModel) {
    /// Leadership movement won't cause disk utilization change.
    return proposal.balancingAction() == BalancingAction.LEADERSHIP_MOVEMENT
        || super.isProposalAcceptable(proposal, clusterModel);
  }

  @Override
  public String name() {
    return DiskCapacityGoal.class.getSimpleName();
  }
}
