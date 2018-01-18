/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

    package com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner;

    import com.linkedin.kafka.cruisecontrol.common.ClusterProperty;
    import com.linkedin.kafka.cruisecontrol.common.TestConstants;
    import java.util.Collection;
    import java.util.Map;
    import org.junit.Test;
    import org.junit.runner.RunWith;
    import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class KafkaAssignerRandomClusterUniformDistRebalanceTest extends KafkaAssignerRandomClusterTest {

  @Parameterized.Parameters(name = "{2}-{0}")
  public static Collection<Object[]> data() throws Exception {
    return KafkaAssignerRandomClusterTest.data(TestConstants.Distribution.UNIFORM);
  }

  /**
   * Constructor of Random Cluster Test.
   * @param testId Test id.
   * @param modifiedProperties  Modified cluster properties over the {@link TestConstants#BASE_PROPERTIES}.
   * @param goalNameByPriority  Goal name by priority.
   * @param replicaDistribution Distribution of replicas in the test cluster.
   */
  public KafkaAssignerRandomClusterUniformDistRebalanceTest(int testId,
                                                            Map<ClusterProperty, Number> modifiedProperties,
                                                            Map<Integer, String> goalNameByPriority,
                                                            TestConstants.Distribution replicaDistribution) {
    super(testId, modifiedProperties, goalNameByPriority, replicaDistribution);
  }

  @Test
  public void testRebalance() throws Exception {
    super.testRebalance();
  }
}
