/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner;

    import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
    import com.linkedin.kafka.cruisecontrol.common.TestConstants;
    import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
    import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

    import java.util.Collection;
    import java.util.HashMap;
    import java.util.Map;
    import java.util.ArrayList;
    import org.junit.Rule;
    import org.junit.Test;
    import org.junit.rules.ExpectedException;
    import org.junit.runner.RunWith;
    import org.junit.runners.Parameterized;

    import static com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerOptimizationVerifier.executeGoalsFor;
    import static org.junit.Assert.assertTrue;


/**
 * Unit test for testing with various deterministic clusters.
 */
@RunWith(Parameterized.class)
public class KafkaAssignerDeterministicClusterTest {
  private int _testId;
  private ClusterModel _cluster;
  private Map<Integer, String> _goalNameByPriority;
  private Class<Throwable> _exceptionClass;

  /**
   * Constructor for Deterministic Cluster Test.
   *
   * @param cluster             The state of the cluster.
   * @param goalNameByPriority  Name of goals by the order of execution priority.
   */
  public KafkaAssignerDeterministicClusterTest(int testId,
                                               ClusterModel cluster,
                                               Map<Integer, String> goalNameByPriority,
                                               Class<Throwable> exceptionClass) {
    _testId = testId;
    _cluster = cluster;
    _goalNameByPriority = goalNameByPriority;
    _exceptionClass = exceptionClass;
  }

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Parameterized.Parameters(name = "{2}-{0}")
  public static Collection<Object[]> data() throws Exception {
    Collection<Object[]> p = new ArrayList<>();

    Map<Integer, String> goalNameByPriority = new HashMap<>();
    goalNameByPriority.put(1, KafkaAssignerEvenRackAwareGoal.class.getName());

    int testId = 0;
    // Small cluster.
    p.add(params(testId++, DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY), goalNameByPriority, null));
    // Medium cluster.
    p.add(params(testId++, DeterministicCluster.mediumClusterModel(TestConstants.BROKER_CAPACITY), goalNameByPriority, null));
    // Rack-aware satisfiable.
    p.add(params(testId++, DeterministicCluster.rackAwareSatisfiable(), goalNameByPriority, null));
    // Rack-aware unsatisfiable.
    p.add(params(testId++, DeterministicCluster.rackAwareUnsatisfiable(), goalNameByPriority, OptimizationFailureException.class));

    return p;
  }

  private static Object[] params(int testId,
                                 ClusterModel cluster,
                                 Map<Integer, String> goalNameByPriority,
                                 Class<? extends Throwable> exceptionClass) throws Exception {
    return new Object[]{testId, cluster, goalNameByPriority, exceptionClass};
  }

  @Test
  public void test() throws Exception {
    if (_exceptionClass != null) {
      expected.expect(_exceptionClass);
    }

    assertTrue("Deterministic Cluster Test failed to optimize goals.",
               executeGoalsFor(_cluster, _goalNameByPriority).violatedGoalsAfterOptimization().isEmpty());
  }
}
