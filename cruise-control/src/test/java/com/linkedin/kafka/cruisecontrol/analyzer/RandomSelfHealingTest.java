/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.ClusterProperty;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.model.RandomCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import java.util.Properties;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.*;
import static org.junit.Assert.assertTrue;


@RunWith(Parameterized.class)
public class RandomSelfHealingTest {
  private static final Logger LOG = LoggerFactory.getLogger(RandomSelfHealingTest.class);

  /**
   * Populate parameters for the {@link OptimizationVerifier}.
   *
   * @return Parameters for the {@link OptimizationVerifier}.
   */
  @Parameterized.Parameters(name = "{1}-{0}")
  public static Collection<Object[]> data() {
    Collection<Object[]> p = new ArrayList<>();

    // Sorted by priority.
    List<String> goalNameByPriority = Arrays.asList(RackAwareGoal.class.getName(),
                                                    ReplicaCapacityGoal.class.getName(),
                                                    DiskCapacityGoal.class.getName(),
                                                    NetworkInboundCapacityGoal.class.getName(),
                                                    NetworkOutboundCapacityGoal.class.getName(),
                                                    CpuCapacityGoal.class.getName(),
                                                    ReplicaDistributionGoal.class.getName(),
                                                    PotentialNwOutGoal.class.getName(),
                                                    DiskUsageDistributionGoal.class.getName(),
                                                    NetworkInboundUsageDistributionGoal.class.getName(),
                                                    NetworkOutboundUsageDistributionGoal.class.getName(),
                                                    CpuUsageDistributionGoal.class.getName(),
                                                    LeaderReplicaDistributionGoal.class.getName(),
                                                    TopicReplicaDistributionGoal.class.getName());

    List<String> kafkaAssignerGoals = Arrays.asList(KafkaAssignerEvenRackAwareGoal.class.getName(),
                                                    KafkaAssignerDiskUsageDistributionGoal.class.getName());

    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(2000L));
    BalancingConstraint constraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    constraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    constraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    List<OptimizationVerifier.Verification> verifications = Arrays.asList(NEW_BROKERS, BROKEN_BROKERS, REGRESSION);
    List<OptimizationVerifier.Verification> kafkaAssignerVerifications =
        Arrays.asList(BROKEN_BROKERS, REGRESSION, GOAL_VIOLATION);

    // -- TEST DECK #1: SINGLE DEAD BROKER.
    // Test: Single Goal.
    Map<ClusterProperty, Number> singleDeadBroker = Collections.singletonMap(ClusterProperty.NUM_DEAD_BROKERS, 1);
    int testId = 0;
    List<String> testGoal;
    for (int i = 0; i < goalNameByPriority.size(); i++) {
      testGoal = goalNameByPriority.subList(i, i + 1);
      p.add(params(testId++, singleDeadBroker, testGoal, constraint, Collections.emptySet(), verifications, true));
      p.add(params(testId++, singleDeadBroker, testGoal, constraint, Collections.emptySet(), verifications, false));
      p.add(params(testId++, singleDeadBroker, testGoal, constraint, Collections.singleton("T0"), verifications, true));
      p.add(params(testId++, singleDeadBroker, testGoal, constraint, Collections.singleton("T0"), verifications, false));
    }
    // In Kafka Assigner mode, the goal order is well-defined -- i.e. KafkaAssignerEvenRackAwareGoal is the first goal.
    testGoal = Collections.singletonList(KafkaAssignerEvenRackAwareGoal.class.getName());
    p.add(params(testId++, singleDeadBroker, testGoal, constraint, Collections.emptySet(), kafkaAssignerVerifications, true));
    p.add(params(testId++, singleDeadBroker, testGoal, constraint, Collections.emptySet(), kafkaAssignerVerifications, false));
    p.add(params(testId++, singleDeadBroker, testGoal, constraint, Collections.singleton("T0"), kafkaAssignerVerifications, true));
    p.add(params(testId++, singleDeadBroker, testGoal, constraint, Collections.singleton("T0"), kafkaAssignerVerifications, false));

    props.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(5100L));
    constraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    constraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    constraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    // Test: All Goals.
    p.add(params(testId++, singleDeadBroker, goalNameByPriority, constraint, Collections.emptySet(), verifications, true));
    p.add(params(testId++, singleDeadBroker, goalNameByPriority, constraint, Collections.singleton("T0"), verifications, true));
    p.add(params(testId++, singleDeadBroker, kafkaAssignerGoals, constraint, Collections.emptySet(), kafkaAssignerVerifications, true));
    p.add(params(testId++, singleDeadBroker, kafkaAssignerGoals, constraint, Collections.singleton("T0"), kafkaAssignerVerifications, true));

    // -- TEST DECK #2: MULTIPLE DEAD BROKERS.
    // Test: Single Goal.
    Map<ClusterProperty, Number> multipleDeadBrokers = Collections.singletonMap(ClusterProperty.NUM_DEAD_BROKERS, 5);
    for (int i = 0; i < goalNameByPriority.size(); i++) {
      testGoal = goalNameByPriority.subList(i, i + 1);
      p.add(params(testId++, multipleDeadBrokers, testGoal, constraint, Collections.emptySet(), verifications, true));
      p.add(params(testId++, multipleDeadBrokers, testGoal, constraint, Collections.singleton("T0"), verifications, true));
    }
    // In Kafka Assigner mode, the goal order is well-defined -- i.e. KafkaAssignerEvenRackAwareGoal is the first goal.
    testGoal = Collections.singletonList(KafkaAssignerEvenRackAwareGoal.class.getName());
    p.add(params(testId++, multipleDeadBrokers, testGoal, constraint, Collections.emptySet(), kafkaAssignerVerifications, true));
    p.add(params(testId++, multipleDeadBrokers, testGoal, constraint, Collections.singleton("T0"), kafkaAssignerVerifications, true));

    // Test: All Goals.
    p.add(params(testId++, multipleDeadBrokers, goalNameByPriority, constraint, Collections.emptySet(), verifications, true));
    p.add(params(testId++, multipleDeadBrokers, goalNameByPriority, constraint, Collections.singleton("T0"), verifications, true));
    p.add(params(testId++, multipleDeadBrokers, kafkaAssignerGoals, constraint, Collections.emptySet(), kafkaAssignerVerifications, true));
    p.add(params(testId++, multipleDeadBrokers, kafkaAssignerGoals, constraint, Collections.singleton("T0"), kafkaAssignerVerifications, true));

    return p;
  }

  private static Object[] params(int testId,
                                 Map<ClusterProperty, Number> modifiedProperties,
                                 List<String> goalNameByPriority,
                                 BalancingConstraint balancingConstraint,
                                 Collection<String> excludedTopics,
                                 List<OptimizationVerifier.Verification> verifications,
                                 boolean leaderInFirstPosition) {
    return new Object[]{
        testId, modifiedProperties, goalNameByPriority, balancingConstraint, excludedTopics, verifications, leaderInFirstPosition
    };
  }

  private int _testId;
  private Map<ClusterProperty, Number> _modifiedProperties;
  private List<String> _goalNameByPriority;
  private BalancingConstraint _balancingConstraint;
  private Set<String> _excludedTopics;
  private List<OptimizationVerifier.Verification> _verifications;
  private boolean _leaderInFirstPosition;

  /**
   * Constructor of Self Healing Test.
   *
   * @param testId Test id.
   * @param modifiedProperties Modified cluster properties over the {@link TestConstants#BASE_PROPERTIES}.
   * @param goalNameByPriority Goal name by priority.
   * @param balancingConstraint Balancing constraint.
   * @param excludedTopics Excluded topics.
   * @param verifications the verifications to make.
   * @param leaderInFirstPosition Leader of each partition is in the first position or not.
   */
  public RandomSelfHealingTest(int testId,
                               Map<ClusterProperty, Number> modifiedProperties,
                               List<String> goalNameByPriority,
                               BalancingConstraint balancingConstraint,
                               Collection<String> excludedTopics,
                               List<OptimizationVerifier.Verification> verifications,
                               boolean leaderInFirstPosition) {
    _testId = testId;
    _modifiedProperties = modifiedProperties;
    _goalNameByPriority = goalNameByPriority;
    _balancingConstraint = balancingConstraint;
    _excludedTopics = new HashSet<>(excludedTopics);
    _verifications = verifications;
    _leaderInFirstPosition = leaderInFirstPosition;
  }

  @Test
  public void test() throws Exception {
    // Create cluster properties by applying modified properties to base properties.
    Map<ClusterProperty, Number> clusterProperties = new HashMap<>(TestConstants.BASE_PROPERTIES);
    clusterProperties.putAll(_modifiedProperties);

    LOG.debug("Replica distribution: {}.", TestConstants.Distribution.UNIFORM);
    ClusterModel clusterModel = RandomCluster.generate(clusterProperties);
    RandomCluster.populate(clusterModel, clusterProperties, TestConstants.Distribution.UNIFORM, true,
                           _leaderInFirstPosition, _excludedTopics);

    assertTrue("Self Healing Test failed to improve the existing state.",
               OptimizationVerifier.executeGoalsFor(_balancingConstraint, clusterModel, _goalNameByPriority,
                                                    _excludedTopics, _verifications));

  }
}
