/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.common.ClusterProperty;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import java.util.Collection;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RandomClusterUniformDistNewBrokerTest extends RandomClusterTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws AnalysisInputException {
    return RandomClusterTest.data(TestConstants.Distribution.UNIFORM);
  }

  /**
   * Constructor of Random Cluster Test.
   *  @param modifiedProperties  Modified cluster properties over the {@link TestConstants#BASE_PROPERTIES}.
   * @param goalNameByPriority  Goal name by priority.
   * @param replicaDistribution Distribution of replicas in the test cluster.
   * @param balancingConstraint The balancing constraints.
   */
  public RandomClusterUniformDistNewBrokerTest(Map<ClusterProperty, Number> modifiedProperties,
                                               Map<Integer, String> goalNameByPriority,
                                               TestConstants.Distribution replicaDistribution,
                                               BalancingConstraint balancingConstraint) {
    super(modifiedProperties, goalNameByPriority, replicaDistribution, balancingConstraint);
  }

  @Test
  public void testNewBrokers() throws Exception {
    super.testNewBrokers();
  }
}
