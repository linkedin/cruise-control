/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
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
  public boolean isActionAcceptable(BalancingAction action, ClusterModel clusterModel) {
    /// Leadership movement won't cause disk utilization change.
    return action.balancingAction() == ActionType.LEADERSHIP_MOVEMENT
        || super.isActionAcceptable(action, clusterModel);
  }

  @Override
  public String name() {
    return DiskUsageDistributionGoal.class.getSimpleName();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(1, _minMonitoredPartitionPercentage, true);
  }
}
