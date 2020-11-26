/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class GoalUtilsTest {

  @Test
  public void testComputeResourceUtilizationBalanceThreshold() {
    Resource resource = Resource.CPU;

    // Verify 1: Low utilization and compute balance threshold lower bound
    verifyComputingResourceUtilizationBalanceThreshold(0.3,
                                                       resource,
                                                       ResourceDistributionGoal.BALANCE_MARGIN,
                                                       true,
                                                       true,
                                                       0.4,
                                                       1.3,
                                                       1.2,
                                                       0.0);

    // Verify 2: Low utilization and compute balance threshold upper bound
    verifyComputingResourceUtilizationBalanceThreshold(0.3,
                                                       resource,
                                                       ResourceDistributionGoal.BALANCE_MARGIN,
                                                       false,
                                                       true,
                                                       0.4,
                                                       1.3,
                                                       1.2,
                                                       0.4 * ResourceDistributionGoal.BALANCE_MARGIN);

    // Verify 3: Not low utilization and compute balance threshold lower bound
    verifyComputingResourceUtilizationBalanceThreshold(0.3,
                                                       resource,
                                                       ResourceDistributionGoal.BALANCE_MARGIN,
                                                       true,
                                                       true,
                                                       0.2,
                                                       1.3,
                                                       1.2,
                                                       0.3 * (1 - ((1.3 * 1.2) - 1) * ResourceDistributionGoal.BALANCE_MARGIN));

    // Verify 4: Not low utilization and compute balance threshold upper bound
    verifyComputingResourceUtilizationBalanceThreshold(0.3,
                                                       resource,
                                                       ResourceDistributionGoal.BALANCE_MARGIN,
                                                       false,
                                                       true,
                                                       0.2,
                                                       1.3,
                                                       1.2,
                                                       0.3 * (1 + ((1.3 * 1.2) - 1) * ResourceDistributionGoal.BALANCE_MARGIN));

  }

  private void verifyComputingResourceUtilizationBalanceThreshold(double avgUtilizationPercentage,
                                                                  Resource resource,
                                                                  double balanceMargin,
                                                                  boolean isLowerThreshold,
                                                                  boolean isTriggeredByGoalViolation,
                                                                  double lowUtilizationThreshold,
                                                                  double resourceBalancePercentage,
                                                                  double goalViolationDistributionThresholdMultiplier,
                                                                  double expectedComputedBalanceThreshold) {

    BalancingConstraint mockBalanceConstraint = EasyMock.mock(BalancingConstraint.class);
    EasyMock.expect(mockBalanceConstraint.lowUtilizationThreshold(resource)).andReturn(lowUtilizationThreshold).anyTimes();
    EasyMock.expect(mockBalanceConstraint.resourceBalancePercentage(resource)).andReturn(resourceBalancePercentage).anyTimes();
    EasyMock.expect(mockBalanceConstraint.goalViolationDistributionThresholdMultiplier()).
        andReturn(goalViolationDistributionThresholdMultiplier).anyTimes();

    EasyMock.replay(mockBalanceConstraint);
    double computedBalanceThreshold = GoalUtils.computeResourceUtilizationBalanceThreshold(avgUtilizationPercentage,
        resource,
        mockBalanceConstraint,
        isTriggeredByGoalViolation,
        balanceMargin,
        isLowerThreshold);

    Assert.assertEquals(expectedComputedBalanceThreshold, computedBalanceThreshold, 0.0);
  }
}
