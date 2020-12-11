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

  private static final double AVG_UTILIZATION_PERCENTAGE = 0.3;
  private static final double DEFAULT_RESOURCE_BALANCE_THRESHOLD = 1.3;
  private static final double GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER = 1.2;

  @Test
  public void testComputeResourceUtilizationBalanceThreshold() {
    Resource resource = Resource.CPU;

    // Verify case 1: Low utilization and compute balance threshold lower bound
    verifyComputingResourceUtilizationBalanceThreshold(resource, true, 0.4, 0.0);

    // Verify case 2: Low utilization. The average utilization percentage * resource balance threshold > low resource utilization percentage
    double expectedComputedBalanceUpperLimit = 0.3 * (1 + ((1.3 * 1.2) - 1) * ResourceDistributionGoal.BALANCE_MARGIN);
    verifyComputingResourceUtilizationBalanceThreshold(resource, false, 0.4, expectedComputedBalanceUpperLimit);

    // Verify case 3: Low utilization. The average utilization percentage * resource balance threshold < low resource utilization percentage
    double lowUtilizationThreshold = 0.6;
    expectedComputedBalanceUpperLimit = lowUtilizationThreshold * ResourceDistributionGoal.BALANCE_MARGIN;
    verifyComputingResourceUtilizationBalanceThreshold(resource, false, lowUtilizationThreshold, expectedComputedBalanceUpperLimit);

    // Verify case 4: Not low utilization and compute balance threshold lower bound
    verifyComputingResourceUtilizationBalanceThreshold(resource, true, 0.2, 0.3 * (1 - ((1.3 * 1.2) - 1) * ResourceDistributionGoal.BALANCE_MARGIN));

    // Verify case 5: Not low utilization and compute balance threshold upper bound
    verifyComputingResourceUtilizationBalanceThreshold(resource, false, 0.2, 0.3 * (1 + ((1.3 * 1.2) - 1) * ResourceDistributionGoal.BALANCE_MARGIN));
  }

  private void verifyComputingResourceUtilizationBalanceThreshold(Resource resource,
                                                                  boolean isLowerThreshold,
                                                                  double lowUtilizationThreshold,
                                                                  double expectedComputedBalanceThreshold) {

    BalancingConstraint mockBalanceConstraint = EasyMock.mock(BalancingConstraint.class);
    EasyMock.expect(mockBalanceConstraint.lowUtilizationThreshold(resource)).andReturn(lowUtilizationThreshold).anyTimes();
    EasyMock.expect(mockBalanceConstraint.resourceBalancePercentage(resource)).andReturn(DEFAULT_RESOURCE_BALANCE_THRESHOLD).anyTimes();
    EasyMock.expect(mockBalanceConstraint.goalViolationDistributionThresholdMultiplier()).
        andReturn(GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER).anyTimes();

    EasyMock.replay(mockBalanceConstraint);
    double computedBalanceThreshold = GoalUtils.computeResourceUtilizationBalanceThreshold(AVG_UTILIZATION_PERCENTAGE,
                                                                                           resource,
                                                                                           mockBalanceConstraint,
                                                                                           true,
                                                                                           ResourceDistributionGoal.BALANCE_MARGIN,
                                                                                           isLowerThreshold);
    EasyMock.verify(mockBalanceConstraint);
    Assert.assertEquals(expectedComputedBalanceThreshold, computedBalanceThreshold, 0.0);
  }
}
