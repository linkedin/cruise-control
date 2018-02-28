/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
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
import com.linkedin.kafka.cruisecontrol.common.RandomCluster;
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
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.*;
import static org.junit.Assert.assertTrue;


@RunWith(Parameterized.class)
public class RandomSelfHealingTest {
  private static final Logger LOG = LoggerFactory.getLogger(RandomSelfHealingTest.class);

  /**
   * Populate parameters for the {@link OptimizationVerifier}. All brokers are alive.
   *
   * @return Parameters for the {@link OptimizationVerifier}.
   */
  @Parameters(name = "{1}-{0}")
  public static Collection<Object[]> data() {
    Collection<Object[]> p = new ArrayList<>();

    Map<Integer, String> goalNameByPriority = new HashMap<>();
    goalNameByPriority.put(1, RackAwareGoal.class.getName());
    goalNameByPriority.put(2, ReplicaCapacityGoal.class.getName());
    goalNameByPriority.put(3, DiskCapacityGoal.class.getName());
    goalNameByPriority.put(4, NetworkInboundCapacityGoal.class.getName());
    goalNameByPriority.put(5, NetworkOutboundCapacityGoal.class.getName());
    goalNameByPriority.put(6, CpuCapacityGoal.class.getName());
    goalNameByPriority.put(7, ReplicaDistributionGoal.class.getName());
    goalNameByPriority.put(8, PotentialNwOutGoal.class.getName());
    goalNameByPriority.put(9, DiskUsageDistributionGoal.class.getName());
    goalNameByPriority.put(10, NetworkInboundUsageDistributionGoal.class.getName());
    goalNameByPriority.put(11, NetworkOutboundUsageDistributionGoal.class.getName());
    goalNameByPriority.put(12, CpuUsageDistributionGoal.class.getName());
    goalNameByPriority.put(13, TopicReplicaDistributionGoal.class.getName());

    Map<Integer, String> kafkaAssignerGoals = new HashMap<>();
    kafkaAssignerGoals.put(0, KafkaAssignerEvenRackAwareGoal.class.getName());
    kafkaAssignerGoals.put(1, KafkaAssignerDiskUsageDistributionGoal.class.getName());

    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(2000L));
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    List<OptimizationVerifier.Verification> verifications = Arrays.asList(NEW_BROKERS, DEAD_BROKERS, REGRESSION);
    List<OptimizationVerifier.Verification> kafkaAssignerVerifications =
        Arrays.asList(DEAD_BROKERS, REGRESSION, GOAL_VIOLATION);

    // -- TEST DECK #1: SINGLE DEAD BROKER.
    // Test: Single Goal.
    Map<ClusterProperty, Number> singleDeadBroker = new HashMap<>();
    singleDeadBroker.put(ClusterProperty.NUM_DEAD_BROKERS, 1);
    int testId = 0;
    for (Map.Entry<Integer, String> entry : goalNameByPriority.entrySet()) {
      p.add(params(testId++, singleDeadBroker, Collections.singletonMap(entry.getKey(), entry.getValue()),
                   balancingConstraint, Collections.emptySet(), verifications, true));
      p.add(params(testId++, singleDeadBroker, Collections.singletonMap(entry.getKey(), entry.getValue()),
                   balancingConstraint, Collections.emptySet(), verifications, false));
      p.add(params(testId++, singleDeadBroker, Collections.singletonMap(entry.getKey(), entry.getValue()),
                   balancingConstraint, Collections.singleton("T0"), verifications, true));
      p.add(params(testId++, singleDeadBroker, Collections.singletonMap(entry.getKey(), entry.getValue()),
                   balancingConstraint, Collections.singleton("T0"), verifications, false));
    }
    p.add(params(testId++, singleDeadBroker, Collections.singletonMap(0, KafkaAssignerEvenRackAwareGoal.class.getName()),
                 balancingConstraint, Collections.emptySet(), kafkaAssignerVerifications, true));
    p.add(params(testId++, singleDeadBroker, Collections.singletonMap(0, KafkaAssignerEvenRackAwareGoal.class.getName()),
                 balancingConstraint, Collections.emptySet(), kafkaAssignerVerifications, false));
    p.add(params(testId++, singleDeadBroker, Collections.singletonMap(0, KafkaAssignerEvenRackAwareGoal.class.getName()),
                 balancingConstraint, Collections.singleton("T0"), kafkaAssignerVerifications, true));
    p.add(params(testId++, singleDeadBroker, Collections.singletonMap(0, KafkaAssignerEvenRackAwareGoal.class.getName()),
                 balancingConstraint, Collections.singleton("T0"), kafkaAssignerVerifications, false));

    props.setProperty(KafkaCruiseControlConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(5100L));
    balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    // Test: All Goals.
    p.add(params(testId++, singleDeadBroker, goalNameByPriority, balancingConstraint, Collections.emptySet(),
                 verifications, true));
    p.add(params(testId++, singleDeadBroker, goalNameByPriority, balancingConstraint, Collections.singleton("T0"),
                 verifications, true));
    p.add(params(testId++, singleDeadBroker, kafkaAssignerGoals, balancingConstraint, Collections.emptySet(),
                 kafkaAssignerVerifications, true));
    p.add(params(testId++, singleDeadBroker, kafkaAssignerGoals, balancingConstraint, Collections.singleton("T0"),
                 kafkaAssignerVerifications, true));

    // -- TEST DECK #2: MULTIPLE DEAD BROKERS.
    // Test: Single Goal.
    Map<ClusterProperty, Number> multipleDeadBrokers = new HashMap<>();
    multipleDeadBrokers.put(ClusterProperty.NUM_DEAD_BROKERS, 5);
    for (Map.Entry<Integer, String> entry : goalNameByPriority.entrySet()) {
      p.add(params(testId++, multipleDeadBrokers, Collections.singletonMap(entry.getKey(), entry.getValue()),
                   balancingConstraint, Collections.emptySet(), verifications, true));
      p.add(params(testId++, multipleDeadBrokers, Collections.singletonMap(entry.getKey(), entry.getValue()),
                   balancingConstraint, Collections.singleton("T0"), verifications, true));
    }
    p.add(params(testId++, multipleDeadBrokers, Collections.singletonMap(0, KafkaAssignerEvenRackAwareGoal.class.getName()),
                 balancingConstraint, Collections.emptySet(), kafkaAssignerVerifications, true));
    p.add(params(testId++, multipleDeadBrokers, Collections.singletonMap(0, KafkaAssignerEvenRackAwareGoal.class.getName()),
                 balancingConstraint, Collections.singleton("T0"), kafkaAssignerVerifications, true));

    // Test: All Goals.
    p.add(params(testId++, multipleDeadBrokers, goalNameByPriority, balancingConstraint, Collections.emptySet(),
                 verifications, true));
    p.add(params(testId++, multipleDeadBrokers, goalNameByPriority, balancingConstraint, Collections.singleton("T0"),
                 verifications, true));
    p.add(params(testId++, multipleDeadBrokers, kafkaAssignerGoals, balancingConstraint, Collections.emptySet(),
                 kafkaAssignerVerifications, true));
    p.add(params(testId++, multipleDeadBrokers, kafkaAssignerGoals, balancingConstraint, Collections.singleton("T0"),
                 kafkaAssignerVerifications, true));

    return p;
  }

  private static Object[] params(int testId,
                                 Map<ClusterProperty, Number> modifiedProperties,
                                 Map<Integer, String> goalNameByPriority,
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
  private Map<Integer, String> _goalNameByPriority;
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
   */
  public RandomSelfHealingTest(int testId,
                               Map<ClusterProperty, Number> modifiedProperties,
                               Map<Integer, String> goalNameByPriority,
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
