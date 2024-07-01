/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.Map;
import java.util.Set;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


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

  @Test
  public void testCanNotBeOverprovisioned() {
    Map<String, ProvisionRecommendation> recommendations =
        Map.of("r1", new ProvisionRecommendation.Builder(ProvisionStatus.OVER_PROVISIONED).numBrokers(3).build(), "r2",
               new ProvisionRecommendation.Builder(ProvisionStatus.OVER_PROVISIONED).numBrokers(2).build(), "r3",
               new ProvisionRecommendation.Builder(ProvisionStatus.OVER_PROVISIONED).numBrokers(1).build());

    assertFalse(GoalUtils.validateProvisionResponse(recommendations, clusterModel(4), 5));
    // num brokers < min brokers
    assertTrue(GoalUtils.validateProvisionResponse(recommendations, clusterModel(4), 6));
    // expected num brokers < max RF
    assertTrue(GoalUtils.validateProvisionResponse(recommendations, clusterModel(5), 5));

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> GoalUtils.validateProvisionResponse(
        Map.of("", new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(1).build()), clusterModel(4), 5));
    assertEquals(e.getMessage(), "passed recommendation status must be OVER_PROVISIONED");
  }

  private ClusterModel clusterModel(int maxRF) {
    ClusterModel mockClusterModel = EasyMock.mock(ClusterModel.class);
    EasyMock.expect(mockClusterModel.aliveBrokers())
            .andReturn(
                Set.of(EasyMock.mock(Broker.class), EasyMock.mock(Broker.class), EasyMock.mock(Broker.class), EasyMock.mock(Broker.class),
                       EasyMock.mock(Broker.class)))
            .times(2);
    EasyMock.expect(mockClusterModel.maxReplicationFactor()).andReturn(maxRF);
    EasyMock.replay(mockClusterModel);

    return mockClusterModel;
  }
}
