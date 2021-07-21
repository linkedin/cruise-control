/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.common.ClusterProperty;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RandomClusterUniformDistNewBrokerTest extends RandomClusterTest {

  /**
   * Constructor of Random Cluster Test.
   *  @param modifiedProperties  Modified cluster properties over the {@link TestConstants#BASE_PROPERTIES}.
   * @param goalNameByPriority  Goal name by priority.
   * @param replicaDistribution Distribution of replicas in the test cluster.
   * @param balancingConstraint The balancing constraints.
   * @param verifications       The verifications to make.
   */
  public RandomClusterUniformDistNewBrokerTest(Map<ClusterProperty, Number> modifiedProperties,
                                               List<String> goalNameByPriority,
                                               TestConstants.Distribution replicaDistribution,
                                               BalancingConstraint balancingConstraint,
                                               List<OptimizationVerifier.Verification> verifications) {
    super(modifiedProperties, goalNameByPriority, replicaDistribution, balancingConstraint, verifications);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return RandomClusterTest.data(TestConstants.Distribution.UNIFORM);
  }

  @Test
  public void testNewBrokers() throws Exception {
    super.testNewBrokers();
  }
}
