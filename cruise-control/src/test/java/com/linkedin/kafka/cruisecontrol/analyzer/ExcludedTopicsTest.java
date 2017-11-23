/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;


/**
 * Unit test for testing goals with excluded topics under fixed cluster properties.
 */
@RunWith(Parameterized.class)
public class ExcludedTopicsTest {
  private static final Logger LOG = LoggerFactory.getLogger(ExcludedTopicsTest.class);

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Parameters
  public static Collection<Object[]> data() throws Exception {
    Collection<Object[]> params = new ArrayList<>();

    Set<String> goalNames = new HashSet<>();
    goalNames.add(RackAwareGoal.class.getName());
    goalNames.add(RackAwareCapacityGoal.class.getName());
    goalNames.add(CpuCapacityGoal.class.getName());
    goalNames.add(DiskCapacityGoal.class.getName());
    goalNames.add(NetworkInboundCapacityGoal.class.getName());
    goalNames.add(NetworkOutboundCapacityGoal.class.getName());
    goalNames.add(DiskUsageDistributionGoal.class.getName());
    goalNames.add(NetworkInboundUsageDistributionGoal.class.getName());
    goalNames.add(NetworkOutboundUsageDistributionGoal.class.getName());
    goalNames.add(CpuUsageDistributionGoal.class.getName());
    goalNames.add(PotentialNwOutGoal.class.getName());
    goalNames.add(TopicReplicaDistributionGoal.class.getName());
    goalNames.add(LeaderBytesInDistributionGoal.class.getName());
    goalNames.add(ReplicaDistributionGoal.class.getName());

    KafkaCruiseControlConfig config =
        new KafkaCruiseControlConfig(CruiseControlUnitTestUtils.getCruiseControlProperties());

    BalancingConstraint balancingConstraint = new BalancingConstraint(config);
    balancingConstraint.setBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    for (String goalName: goalNames) {
      Class<? extends Goal> goalClass = (Class<? extends Goal>) Class.forName(goalName);
      Goal goal;
      try {
        Constructor<? extends Goal> constructor = goalClass.getDeclaredConstructor(BalancingConstraint.class);
        constructor.setAccessible(true);
        goal = constructor.newInstance(balancingConstraint);
      } catch (NoSuchMethodException badConstructor) {
        //Try default constructor
        goal = goalClass.newInstance();
      }

      if (goalName.equals(RackAwareGoal.class.getName()) || goalName.equals(RackAwareCapacityGoal.class.getName())) {
        // Test: With excluded topics, rack aware satisfiable cluster (No exception, No proposal, Expected to look optimized)
        Object[] withExcludedTopicsTestParams = {goal, Collections.singleton("T1"), null,
            DeterministicCluster.rackAwareTestClusterModel(TestConstants.BROKER_CAPACITY), true};
        params.add(withExcludedTopicsTestParams);
        // Test: Without excluded topics, rack aware satisfiable cluster  (No exception, Proposal expected, Expected to
        // look optimized)
        Object[] noExcludedTopicsTestParams = {goal, Collections.emptySet(), null,
            DeterministicCluster.rackAwareTestClusterModel(TestConstants.BROKER_CAPACITY), true};
        params.add(noExcludedTopicsTestParams);
        // Test: With excluded topics, rack aware unsatisfiable cluster (No exception, No proposal, Expected to look
        // optimized)
        Object[] withExcludedTopicsUnsatisfiableTestParams =
            {goal, Collections.singleton("T1"), null,
                DeterministicCluster.rackUnawareTestClusterModel(TestConstants.BROKER_CAPACITY), true};
        params.add(withExcludedTopicsUnsatisfiableTestParams);
        // Test: Without excluded topics, rack aware unsatisfiable cluster  (Exception expected)
        Object[] noExcludedTopicsUnsatisfiableTestParams = {goal, Collections.emptySet(), AnalysisInputException.class,
            DeterministicCluster.rackUnawareTestClusterModel(TestConstants.BROKER_CAPACITY), null};
        params.add(noExcludedTopicsUnsatisfiableTestParams);
      } else if (goalName.equals(CpuCapacityGoal.class.getName()) ||
                 goalName.equals(DiskCapacityGoal.class.getName()) ||
                 goalName.equals(NetworkInboundCapacityGoal.class.getName()) ||
                 goalName.equals(NetworkOutboundCapacityGoal.class.getName())) {

        // Test: With single excluded topic, satisfiable cluster (No exception, No proposal for excluded topic,
        // Expected to look optimized)
        Object[] withSingleExcludedTopicTestParams = {goal, Collections.singleton("T1"), null,
            DeterministicCluster.excludedTopicsTestClusterModel(TestConstants.BROKER_CAPACITY), true};
        params.add(withSingleExcludedTopicTestParams);

        // Test: With all topics excluded, not satisfiable (Exception)
        ClusterModel clusterModel = DeterministicCluster.excludedTopicsTestClusterModel(TestConstants.BROKER_CAPACITY);
        Object[] withAllExcludedTopicTestParams = {goal, clusterModel.topics(), AnalysisInputException.class,
            clusterModel, null};
        params.add(withAllExcludedTopicTestParams);
      } else if (goalName.equals(DiskUsageDistributionGoal.class.getName()) ||
                 goalName.equals(NetworkInboundUsageDistributionGoal.class.getName()) ||
                 goalName.equals(NetworkOutboundUsageDistributionGoal.class.getName()) ||
                 goalName.equals(CpuUsageDistributionGoal.class.getName()) ||
                 goalName.equals(LeaderBytesInDistributionGoal.class.getName())) {

        // Test: With single excluded topic, balance not satisfiable cluster (No exception, No proposal for excluded topic,
        // Not expected to look optimized)
        Object[] withSingleExcludedTopicTestParams = {goal, Collections.singleton("T1"), null,
            DeterministicCluster.excludedTopicsTestClusterModel(TestConstants.BROKER_CAPACITY), false};
        params.add(withSingleExcludedTopicTestParams);

        // Test: With all topics excluded, balance not satisfiable (No exception, No proposal for excluded topic, Not
        // expected to look optimized)
        ClusterModel clusterModel = DeterministicCluster.excludedTopicsTestClusterModel(TestConstants.BROKER_CAPACITY);
            Object[] withAllExcludedTopicTestParams = {goal, clusterModel.topics(), null, clusterModel, false};
        params.add(withAllExcludedTopicTestParams);
      } else if (goalName.equals(PotentialNwOutGoal.class.getName())) {

        // Test: With single excluded topic, balance satisfiable cluster (No exception, No proposal for excluded topic,
        // Expected to look optimized)
        Object[] withSingleExcludedTopicTestParams = {goal, Collections.singleton("T1"), null,
            DeterministicCluster.excludedTopicsTestClusterModel(TestConstants.BROKER_CAPACITY), true};
        params.add(withSingleExcludedTopicTestParams);

        // Test: With all topics excluded, balance not satisfiable (No exception, No proposal for excluded topic, Not
        // expected to look optimized)
        ClusterModel clusterModel = DeterministicCluster.excludedTopicsTestClusterModel(TestConstants.BROKER_CAPACITY);
        Object[] withAllExcludedTopicTestParams = {goal, clusterModel.topics(), null, clusterModel, false};
        params.add(withAllExcludedTopicTestParams);
      } else if (goalName.equals(TopicReplicaDistributionGoal.class.getName()) ||
          goalName.equals(ReplicaDistributionGoal.class.getName())) {

        // Test: With single excluded topic, satisfiable cluster (No exception, No proposal for excluded topic,
        // Expected to look optimized)
        Object[] withSingleExcludedTopicTestParams = {goal, Collections.singleton("T1"), null,
            DeterministicCluster.excludedTopicsTestClusterModel(TestConstants.BROKER_CAPACITY), true};
        params.add(withSingleExcludedTopicTestParams);

        // Test: With all topics excluded, balance not satisfiable (No exception, No proposal for excluded topic,
        // Expected to look optimized)
        ClusterModel clusterModel = DeterministicCluster.excludedTopicsTestClusterModel(TestConstants.BROKER_CAPACITY);
        Object[] withAllExcludedTopicTestParams = {goal, clusterModel.topics(), null, clusterModel, true};
        params.add(withAllExcludedTopicTestParams);
      }
    }
    return params;
  }

  private Goal _goal;
  private Set<String> _excludedTopics;
  private Class<Throwable> _exceptionClass;
  private ClusterModel _clusterModel;
  private Boolean _expectedToOptimize;

  /**
   * Constructor of Excluded Topics Test.
   *
   * @param goal Goal to be tested.
   * @param excludedTopics Topics to be excluded from the goal.
   * @param exceptionClass Expected exception class (if any).
   * @param clusterModel Cluster model to be used for the test.
   * @param expectedToOptimize The expectation on whether the cluster state will be considered optimized or not.
   */
  public ExcludedTopicsTest(Goal goal, Set<String> excludedTopics, Class<Throwable> exceptionClass,
                            ClusterModel clusterModel, Boolean expectedToOptimize) {
    _goal = goal;
    _excludedTopics = excludedTopics;
    _exceptionClass = exceptionClass;
    _clusterModel = clusterModel;
    _expectedToOptimize = expectedToOptimize;
  }

  @Test
  public void test() throws Exception {
    LOG.debug("Testing goal: {}.", _goal.name());

    if (_exceptionClass == null) {
      Map<TopicPartition, List<Integer>> initDistribution = _clusterModel.getReplicaDistribution();

      if (_expectedToOptimize) {
        assertTrue("Excluded Topics Test failed to optimize \" + _goal.name() + \" with excluded topics.",
            _goal.optimize(_clusterModel, Collections.emptySet(), _excludedTopics));
      } else {
        assertTrue("Excluded Topics Test optimized " + _goal.name() + " with excluded topics.",
            !_goal.optimize(_clusterModel, Collections.emptySet(), _excludedTopics));
      }
      // Generated proposals cannot have the excluded topic.
      if (!_excludedTopics.isEmpty()) {
        Set<BalancingProposal> goalProposals = AnalyzerUtils.getDiff(initDistribution, _clusterModel);

        boolean proposalHasTopic = false;
        for (BalancingProposal proposal : goalProposals) {
          proposalHasTopic = _excludedTopics.contains(proposal.topic());
          if (proposalHasTopic) {
            break;
          }
        }
        assertTrue("Excluded topic partitions are included in proposals.", !proposalHasTopic);
      }
    } else {
      expected.expect(_exceptionClass);
      assertTrue("Excluded Topics Test failed to optimize with excluded topics.",
          _goal.optimize(_clusterModel, Collections.emptySet(), _excludedTopics));
    }
  }
}
