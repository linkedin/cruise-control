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


public class NetworkInboundUsageDistributionGoal extends ResourceDistributionGoal {

  /**
   * Constructor for Resource Distribution Goal.
   */
  public NetworkInboundUsageDistributionGoal() {
    super();
  }

  /**
   * Package private for unit test.
   */
  NetworkInboundUsageDistributionGoal(BalancingConstraint constraint) {
    super(constraint);
  }

  @Override
  protected Resource resource() {
    return Resource.NW_IN;
  }

  @Override
  public boolean isProposalAcceptable(BalancingProposal proposal, ClusterModel clusterModel) {
    /// Leadership movement won't cause inbound network utilization change.
    return proposal.balancingAction() == BalancingAction.LEADERSHIP_MOVEMENT
        || super.isProposalAcceptable(proposal, clusterModel);
  }

  @Override
  public String name() {
    return NetworkInboundUsageDistributionGoal.class.getSimpleName();
  }

}
