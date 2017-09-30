/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
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
import java.util.List;
import java.util.Map;

import java.util.Properties;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.*;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for testing with different goals and fixed cluster properties.
 */
@RunWith(Parameterized.class)
public class RandomGoalTest {
  private static final Logger LOG = LoggerFactory.getLogger(RandomGoalTest.class);

  private final static Random RANDOM = new Random(34534534);

  /**
   * Populate parameters for the {@link OptimizationVerifier}. All brokers are alive.
   *
   * @return Parameters for the {@link OptimizationVerifier}.
   */
  @Parameters
  public static Collection<Object[]> data() throws Exception {
    int goalRepetition = 4;
    Collection<Object[]> p = new ArrayList<>();

    List<String> goalsSortedByPriority = Arrays.asList(
        RackAwareGoal.class.getName(),
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
        TopicReplicaDistributionGoal.class.getName(),
        PreferredLeaderElectionGoal.class.getName(),
        LeaderBytesInDistributionGoal.class.getName());

    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(1500L));
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    List<OptimizationVerifier.Verification> verifications = Arrays.asList(NEW_BROKERS, DEAD_BROKERS, REGRESSION);

    // Test: Single goal at a time.
    int goalPriority = 1;
    for (String goalName: goalsSortedByPriority) {
      Map<Integer, String> singletonGoalNameByPriority = Collections.singletonMap(goalPriority, goalName);
      p.add(params(Collections.emptyMap(), singletonGoalNameByPriority, balancingConstraint, verifications));
      goalPriority++;
    }

    // Test: Consecutive repetition of the same goal (goalRepetition times each).
    goalPriority = 1;
    for (String goalName : goalsSortedByPriority) {
      Map<Integer, String> repeatedGoalNamesByPriority = new HashMap<>();
      for (int i = 0; i < goalRepetition; i++) {
        repeatedGoalNamesByPriority.put(goalPriority, goalName);
        goalPriority++;
      }
      p.add(params(Collections.emptyMap(), repeatedGoalNamesByPriority, balancingConstraint, verifications));
    }

    // Test: Nested repetition of the same goal (goalRepetition times each).
    goalPriority = 1;
    Map<Integer, String> nonRepetitiveGoalNamesByPriority = new HashMap<>();
    for (int i = 0; i < goalRepetition; i++) {
      for (String goalName : goalsSortedByPriority) {
        nonRepetitiveGoalNamesByPriority.put(goalPriority, goalName);
        goalPriority++;
      }
    }
    p.add(params(Collections.emptyMap(), nonRepetitiveGoalNamesByPriority, balancingConstraint, verifications));

    // Test: No goal.
    p.add(params(Collections.emptyMap(), Collections.emptyMap(), balancingConstraint, verifications));

    // Test shuffled soft goals.
    List<String> shuffledSoftGoalNames = new ArrayList<>(goalsSortedByPriority);
    // Remove the hard goals.
    shuffledSoftGoalNames.remove(RackAwareGoal.class.getName());
    shuffledSoftGoalNames.remove(ReplicaCapacityGoal.class.getName());
    shuffledSoftGoalNames.remove(CpuCapacityGoal.class.getName());
    shuffledSoftGoalNames.remove(DiskCapacityGoal.class.getName());
    shuffledSoftGoalNames.remove(NetworkInboundCapacityGoal.class.getName());
    shuffledSoftGoalNames.remove(NetworkOutboundCapacityGoal.class.getName());
    Collections.shuffle(shuffledSoftGoalNames, RANDOM);

    goalPriority = 1;
    Map<Integer, String> randomOrderedSoftGoalsByPriority = new HashMap<>();
    for (String goalName : shuffledSoftGoalNames) {
      randomOrderedSoftGoalsByPriority.put(goalPriority, goalName);
      goalPriority++;
    }
    p.add(params(Collections.emptyMap(), randomOrderedSoftGoalsByPriority, balancingConstraint, verifications));

    return p;
  }

  private static Object[] params(Map<ClusterProperty, Number> modifiedProperties,
                                 Map<Integer, String> goalNameByPriority,
                                 BalancingConstraint balancingConstraint,
                                 List<OptimizationVerifier.Verification> verifications) {
    return new Object[]{modifiedProperties, goalNameByPriority, balancingConstraint, verifications};
  }

  private Map<ClusterProperty, Number> _modifiedProperties;
  private Map<Integer, String> _goalNameByPriority;
  private BalancingConstraint _balancingConstraint;
  private List<OptimizationVerifier.Verification> _verifications;

  /**
   * Constructor of Random Goal Test.
   *
   * @param modifiedProperties Modified cluster properties over the {@link TestConstants#BASE_PROPERTIES}.
   * @param goalNameByPriority Goal name by priority.
   * @param balancingConstraint the balancing constraints.
   * @param verifications the verifications to make.
   */
  public RandomGoalTest(Map<ClusterProperty, Number> modifiedProperties,
                        Map<Integer, String> goalNameByPriority,
                        BalancingConstraint balancingConstraint,
                        List<OptimizationVerifier.Verification> verifications) {
    _modifiedProperties = modifiedProperties;
    _goalNameByPriority = goalNameByPriority;
    _balancingConstraint = balancingConstraint;
    _verifications = verifications;
  }

  @Test
  public void test() throws Exception {
    // Create cluster properties by applying modified properties to base properties.
    Map<ClusterProperty, Number> clusterProperties = new HashMap<>(TestConstants.BASE_PROPERTIES);
    clusterProperties.putAll(_modifiedProperties);

    LOG.debug("Replica distribution: {} || Goals: {}.", TestConstants.Distribution.EXPONENTIAL, _goalNameByPriority);
    ClusterModel clusterModel = RandomCluster.generate(clusterProperties);
    RandomCluster.populate(clusterModel, clusterProperties, TestConstants.Distribution.EXPONENTIAL);

    assertTrue("Random Goal Test failed to improve the existing state.",
        OptimizationVerifier.executeGoalsFor(_balancingConstraint, clusterModel, _goalNameByPriority, _verifications));
  }
}
