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
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;


public class DiskUsageDistributionGoal extends ResourceDistributionGoal {

  /**
   * Constructor for Resource Distribution Goal.
   */
  public DiskUsageDistributionGoal() {
    super();
  }

  /**
   * Package private for unit test.
   */
  DiskUsageDistributionGoal(BalancingConstraint constraint) {
    super(constraint);
  }

  @Override
  protected Resource resource() {
    return Resource.DISK;
  }

  @Override
  public boolean isProposalAcceptable(BalancingProposal proposal, ClusterModel clusterModel) {
    /// Leader ship movement won't cause disk utilization change.
    return proposal.balancingAction() == BalancingAction.LEADERSHIP_MOVEMENT
        || super.isProposalAcceptable(proposal, clusterModel);
  }

  @Override
  public String name() {
    return DiskUsageDistributionGoal.class.getSimpleName();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(1, _minMonitoredPartitionPercentage, true);
  }

  @Override
  public String goalType() {
    return _goalType.getType();
  }

  @Override
  public String goalDescription() {
    return "Attempt to make the disk-utilization variance among all the brokers are within a certain range.";
  }
}
