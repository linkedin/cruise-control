/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.BROKEN_BROKERS;
import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.REGRESSION;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for testing with various goal combinations using the deterministic cluster with one dead broker.
 */
@RunWith(Parameterized.class)
public class SelfHealingWithOptimizedGoalTest {
  private final BalancingConstraint _balancingConstraint;
  private final ClusterModel _cluster;
  private final List<String> _goalNameByPriority;
  private final List<OptimizationVerifier.Verification> _verifications;

  /**
   * Constructor for self-healing test.
   *
   * @param balancingConstraint Balancing constraint.
   * @param cluster             The state of the cluster.
   * @param goalNameByPriority  Name of goals by the order of execution priority.
   * @param verifications The verifications to make.
   */
  public SelfHealingWithOptimizedGoalTest(BalancingConstraint balancingConstraint,
                                          ClusterModel cluster,
                                          List<String> goalNameByPriority,
                                          List<OptimizationVerifier.Verification> verifications) {
    _balancingConstraint = balancingConstraint;
    _cluster = cluster;
    _goalNameByPriority = goalNameByPriority;
    _verifications = verifications;
  }

  /**
   * Populate parameters for the {@link OptimizationVerifier}.
   *
   * @return Parameters for the {@link OptimizationVerifier}.
   */
  @Parameterized.Parameters
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

    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(5L));
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setCapacityThreshold(TestConstants.LOW_CAPACITY_THRESHOLD);
    balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    List<OptimizationVerifier.Verification> verifications = Arrays.asList(BROKEN_BROKERS, REGRESSION);

    // ----------##TEST: RACK AWARE GOAL + ONE OTHER GOAL.
    for (int i = 1; i < goalNameByPriority.size(); i++) {
      List<String> testGoals = Arrays.asList(RackAwareGoal.class.getName(), goalNameByPriority.get(i));
      p.add(params(balancingConstraint, DeterministicCluster.deadBroker(TestConstants.BROKER_CAPACITY),
                   testGoals, verifications));
    }

    return p;
  }

  private static Object[] params(BalancingConstraint balancingConstraint,
      ClusterModel cluster,
      List<String> goalNameByPriority,
      List<OptimizationVerifier.Verification> verifications) {
    return new Object[]{balancingConstraint, cluster, goalNameByPriority, verifications};
  }

  @Test
  public void test() throws Exception {
    assertTrue("Self-healing test failed to improve the existing state.",
               OptimizationVerifier.executeGoalsFor(_balancingConstraint, _cluster, _goalNameByPriority, Collections.emptySet(),
                                                    _verifications, true));
  }
}
