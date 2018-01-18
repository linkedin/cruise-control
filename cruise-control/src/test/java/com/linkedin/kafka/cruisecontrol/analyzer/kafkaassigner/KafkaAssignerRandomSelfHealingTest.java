/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner;

    import com.linkedin.kafka.cruisecontrol.common.ClusterProperty;
    import com.linkedin.kafka.cruisecontrol.common.RandomCluster;
    import com.linkedin.kafka.cruisecontrol.common.TestConstants;
    import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

    import java.util.ArrayList;
    import java.util.Collection;
    import java.util.Collections;
    import java.util.HashMap;
    import java.util.HashSet;
    import java.util.Map;

    import java.util.Set;
    import org.junit.Test;
    import org.junit.runner.RunWith;
    import org.junit.runners.Parameterized;
    import org.junit.runners.Parameterized.Parameters;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;

    import static com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerOptimizationVerifier.executeGoalsFor;
    import static org.junit.Assert.assertTrue;


@RunWith(Parameterized.class)
public class KafkaAssignerRandomSelfHealingTest {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaAssignerRandomSelfHealingTest.class);

  @Parameters(name = "{2}-{0}")
  public static Collection<Object[]> data() throws Exception {
    Collection<Object[]> p = new ArrayList<>();

    Map<Integer, String> goalNameByPriority = new HashMap<>();
    goalNameByPriority.put(1, KafkaAssignerEvenRackAwareGoal.class.getName());

    // -- TEST DECK #1: SINGLE DEAD BROKER.
    // Test: Single Goal.
    Map<ClusterProperty, Number> singleDeadBroker = new HashMap<>();
    singleDeadBroker.put(ClusterProperty.NUM_DEAD_BROKERS, 1);
    int testId = 0;
    for (Map.Entry<Integer, String> entry : goalNameByPriority.entrySet()) {
      p.add(params(testId++, singleDeadBroker, Collections.singletonMap(entry.getKey(), entry.getValue()), Collections.emptySet()));
      p.add(params(testId++, singleDeadBroker, Collections.singletonMap(entry.getKey(), entry.getValue()), Collections.singleton("T0")));
    }

    // Test: All Goals.
    p.add(params(testId++, singleDeadBroker, goalNameByPriority, Collections.emptySet()));
    p.add(params(testId++, singleDeadBroker, goalNameByPriority, Collections.singleton("T0")));

    // -- TEST DECK #2: MULTIPLE DEAD BROKERS.
    // Test: Single Goal.
    Map<ClusterProperty, Number> multipleDeadBrokers = new HashMap<>();
    multipleDeadBrokers.put(ClusterProperty.NUM_DEAD_BROKERS, 5);
    for (Map.Entry<Integer, String> entry : goalNameByPriority.entrySet()) {
      p.add(params(testId++, multipleDeadBrokers, Collections.singletonMap(entry.getKey(), entry.getValue()), Collections.emptySet()));
      p.add(params(testId++, multipleDeadBrokers, Collections.singletonMap(entry.getKey(), entry.getValue()), Collections.singleton("T0")));
    }
    // Test: All Goals.
    p.add(params(testId++, multipleDeadBrokers, goalNameByPriority, Collections.emptySet()));
    p.add(params(testId++, multipleDeadBrokers, goalNameByPriority, Collections.singleton("T0")));

    return p;
  }

  private int _testId;
  private Map<ClusterProperty, Number> _modifiedProperties;
  private Map<Integer, String> _goalNameByPriority;
  private Set<String> _excludedTopics;

  /**
   * Constructor of Self Healing Test.
   *
   * @param testId Test id.
   * @param modifiedProperties Modified cluster properties over the {@link TestConstants#BASE_PROPERTIES}.
   * @param goalNameByPriority Goal name by priority.
   * @param excludedTopics Excluded topics.
   */
  public KafkaAssignerRandomSelfHealingTest(int testId,
                                            Map<ClusterProperty, Number> modifiedProperties,
                                            Map<Integer, String> goalNameByPriority,
                                            Collection<String> excludedTopics) {
    _testId = testId;
    _modifiedProperties = modifiedProperties;
    _goalNameByPriority = goalNameByPriority;
    _excludedTopics = new HashSet<>(excludedTopics);
  }

  private static Object[] params(int testId,
                                 Map<ClusterProperty, Number> modifiedProperties,
                                 Map<Integer, String> goalNameByPriority,
                                 Collection<String> excludedTopics) throws Exception {
    return new Object[]{testId, modifiedProperties, goalNameByPriority, excludedTopics};
  }

  @Test
  public void test() throws Exception {
    // Create cluster properties by applying modified properties to base properties.
    Map<ClusterProperty, Number> clusterProperties = new HashMap<>(TestConstants.BASE_PROPERTIES);
    clusterProperties.putAll(_modifiedProperties);

    LOG.debug("Replica distribution: {}.", TestConstants.Distribution.UNIFORM);
    ClusterModel clusterModel = RandomCluster.generate(clusterProperties);
    RandomCluster.populate(clusterModel, clusterProperties, TestConstants.Distribution.UNIFORM, true);

    assertTrue("Self Healing Test failed to improve the existing state.",
               executeGoalsFor(clusterModel, _goalNameByPriority, _excludedTopics).violatedGoalsAfterOptimization().isEmpty());

  }
}
