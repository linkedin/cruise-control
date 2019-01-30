/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
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
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T1;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T2;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalanced;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalanced2;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;


/**
 * Unit test for testing goals with excluded topics under fixed cluster properties.
 */
@RunWith(Parameterized.class)
public class ExcludedTopicsTest {

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Parameterized.Parameters(name = "{1}-{0}")
  public static Collection<Object[]> data() throws Exception {
    Collection<Object[]> p = new ArrayList<>();

    Set<String> noExclusion = Collections.emptySet();
    Set<String> excludeT1 = Collections.unmodifiableSet(Collections.singleton(T1));
    Set<String> excludeAllTopics = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(T1, T2)));
    Set<Integer> noDeadBroker = Collections.emptySet();
    Set<Integer> deadBroker0 = Collections.unmodifiableSet(Collections.singleton(0));

    // ============RackAwareGoal============
    // With excluded topics, rack aware satisfiable cluster, no dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(0, RackAwareGoal.class, excludeT1, null, DeterministicCluster.rackAwareSatisfiable(), noDeadBroker, true));
    // With excluded topics, rack aware satisfiable cluster, one dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(1, RackAwareGoal.class, excludeT1, null, DeterministicCluster.rackAwareSatisfiable(), deadBroker0, true));
    // Without excluded topics, rack aware satisfiable cluster, no dead brokers (No exception, Proposal expected, Expected to look optimized)
    p.add(params(2, RackAwareGoal.class, noExclusion, null, DeterministicCluster.rackAwareSatisfiable(), noDeadBroker, true));
    // Without excluded topics, rack aware satisfiable cluster, one dead broker (No exception, Proposal expected, Expected to look optimized)
    p.add(params(3, RackAwareGoal.class, noExclusion, null, DeterministicCluster.rackAwareSatisfiable(), deadBroker0, true));
    // With excluded topics, rack aware unsatisfiable cluster, no dead broker (No exception, No proposal, Expected to look optimized)
    p.add(params(4, RackAwareGoal.class, excludeT1, null, DeterministicCluster.rackAwareUnsatisfiable(), noDeadBroker, true));
    // With excluded topics, rack aware unsatisfiable cluster, one dead broker (Exception)
    p.add(params(5, RackAwareGoal.class, excludeT1, OptimizationFailureException.class, DeterministicCluster.rackAwareUnsatisfiable(), deadBroker0, null));
    // Test: Without excluded topics, rack aware unsatisfiable cluster, no dead brokers (Exception expected)
    p.add(params(6, RackAwareGoal.class, noExclusion, OptimizationFailureException.class, DeterministicCluster.rackAwareUnsatisfiable(), noDeadBroker, null));
    // Test: Without excluded topics, rack aware unsatisfiable cluster, one dead broker (Exception expected)
    p.add(params(7, RackAwareGoal.class, noExclusion, OptimizationFailureException.class, DeterministicCluster.rackAwareUnsatisfiable(), deadBroker0, null));

    for (Class<? extends Goal> goalClass : Arrays.asList(CpuCapacityGoal.class,
                                                         DiskCapacityGoal.class,
                                                         NetworkInboundCapacityGoal.class,
                                                         NetworkOutboundCapacityGoal.class,
                                                         ReplicaCapacityGoal.class)) {
      // Test: With single excluded topic, satisfiable cluster, no dead brokers (No exception, No proposal
      // for excluded topic, Expected to look optimized)
      p.add(params(0, goalClass, excludeT1, null, unbalanced(), noDeadBroker, true));
      // Test: With single excluded topic, satisfiable cluster, one dead brokers (No exception, No proposal
      // for excluded topic, Expected to look optimized)
      p.add(params(1, goalClass, excludeT1, null, unbalanced(), deadBroker0, true));
      // Test: With all topics excluded, no dead brokers, not satisfiable (Exception)
      p.add(params(2, goalClass, excludeAllTopics, OptimizationFailureException.class, unbalanced(), noDeadBroker, null));
      // Test: With all topics excluded, one dead brokers, not satisfiable, no exception.
      p.add(params(3, goalClass, excludeAllTopics, null, unbalanced(), deadBroker0, true));
    }

    for (Class<? extends Goal> goalClass : Arrays.asList(DiskUsageDistributionGoal.class,
                                                         NetworkInboundUsageDistributionGoal.class,
                                                         NetworkOutboundUsageDistributionGoal.class,
                                                         CpuUsageDistributionGoal.class)) {
      // Test: With single excluded topic, balance not satisfiable cluster, no dead broker (No exception, No proposal
      // for excluded topic, Not expected to look optimized)
      p.add(params(0, goalClass, excludeT1, null, unbalanced(), noDeadBroker, false));
      // Test: With single excluded topic, balance not satisfiable cluster, no dead broker (No exception, No proposal
      // for excluded topic, Not expected to look optimized)
      p.add(params(1, goalClass, excludeT1, null, unbalanced(), deadBroker0, true));
      // Test: With all topics excluded, no dead brokers, balance not satisfiable (No exception, No proposal for
      // excluded topics, Not expected to look optimized)
      p.add(params(2, goalClass, excludeAllTopics, null, unbalanced(), noDeadBroker, false));
      // Test: With all topics excluded, no dead brokers, balance not satisfiable (No exception, No proposal for
      // excluded topics, Not expected to look optimized)
      p.add(params(3, goalClass, excludeAllTopics, null, unbalanced(), deadBroker0, true));
    }

    // ============LeaderBytesInDistributionGoal============
    // Test: With single excluded topic, balance not satisfiable cluster, no dead broker (No exception, No proposal
    // for excluded topic, Not expected to look optimized)
    p.add(params(0, LeaderBytesInDistributionGoal.class, excludeT1, null, unbalanced(), noDeadBroker, false));
    // Test: With single excluded topic, balance not satisfiable cluster, no dead broker (No exception, No proposal
    // for excluded topic, Not expected to look optimized)
    p.add(params(1, LeaderBytesInDistributionGoal.class, excludeT1, null, unbalanced(), deadBroker0, false));
    // Test: With all topics excluded, no dead brokers, balance not satisfiable (No exception, No proposal for
    // excluded topics, Not expected to look optimized)
    p.add(params(2, LeaderBytesInDistributionGoal.class, excludeAllTopics, null, unbalanced(), noDeadBroker, false));
    // Test: With all topics excluded, no dead brokers, balance not satisfiable (No exception, No proposal for
    // excluded topics, Not expected to look optimized)
    p.add(params(3, LeaderBytesInDistributionGoal.class, excludeAllTopics, null, unbalanced(), deadBroker0, false));

    // ============PotentialNwOutGoal============
    // Test: With single excluded topic, balance satisfiable cluster, no dead brokers (No exception, No proposal
    // for excluded topic, Expected to look optimized)
    p.add(params(0, PotentialNwOutGoal.class, excludeT1, null, unbalanced(), noDeadBroker, true));
    // Test: With single excluded topic, balance satisfiable cluster, one dead brokers (No exception, No proposal
    // for excluded topic, Expected to look optimized)
    p.add(params(1, PotentialNwOutGoal.class, excludeT1, null, unbalanced(), deadBroker0, true));
    // Test: With all topics excluded, balance not satisfiable, no dead brokers (No exception, No proposal for
    // excluded topics, Not expected to look optimized)
    p.add(params(2, PotentialNwOutGoal.class, excludeAllTopics, null, unbalanced(), noDeadBroker, false));
    // Test: With all topics excluded, balance not satisfiable, one dead brokers (No exception, No proposal for
    // excluded topics, expected to look optimized)
    p.add(params(3, PotentialNwOutGoal.class, excludeAllTopics, null, unbalanced(), deadBroker0, true));

    // ============TopicReplicaDistributionGoal============
    // Test: With single excluded topic, satisfiable cluster, no dead broker (No exception, No proposal for
    // excluded topic, Expected to look optimized)
    p.add(params(0, TopicReplicaDistributionGoal.class, excludeT1, null, unbalanced(), noDeadBroker, true));
    // Test: With single excluded topic, satisfiable cluster, one dead broker (No exception, No proposal for
    // excluded topic, Expected to look optimized)
    p.add(params(1, TopicReplicaDistributionGoal.class, excludeT1, null, unbalanced(), deadBroker0, true));
    // Test: With all topics excluded, balance not satisfiable, no dead brokers (No exception, No proposal
    // for excluded topics, Expected to look optimized)
    p.add(params(2, TopicReplicaDistributionGoal.class, excludeAllTopics, null, unbalanced(), noDeadBroker, true));
    // Test: With all topics excluded, balance not satisfiable, one dead brokers (No exception, No proposal
    // for excluded topics, Expected to look optimized)
    p.add(params(3, TopicReplicaDistributionGoal.class, excludeAllTopics, null, unbalanced(), deadBroker0, true));

    // ============ReplicaDistributionGoal============
    // Test: With single excluded topic, satisfiable cluster, no dead broker (No exception, No proposal for
    // excluded topic, Expected to look optimized)
    p.add(params(0, ReplicaDistributionGoal.class, excludeT1, null, unbalanced2(), noDeadBroker, true));
    // Test: With single excluded topic, satisfiable cluster, one dead broker (No exception, No proposal for
    // excluded topic, Expected to look optimized)
    p.add(params(1, ReplicaDistributionGoal.class, excludeT1, null, unbalanced2(), deadBroker0, true));
    // Test: With all topics excluded, balance not satisfiable, no dead brokers (No exception, No proposal
    // for excluded topics, Expected to look optimized)
    p.add(params(2, ReplicaDistributionGoal.class, excludeAllTopics, null, unbalanced2(), noDeadBroker, false));
    // Test: With all topics excluded, balance not satisfiable, one dead brokers (No exception, No proposal
    // for excluded topics, Expected to look optimized)
    p.add(params(3, ReplicaDistributionGoal.class, excludeAllTopics, null, unbalanced2(), deadBroker0, true));

    // ============KafkaAssignerEvenRackAwareGoal============
    // With excluded topics, rack aware satisfiable cluster, no dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(0, KafkaAssignerEvenRackAwareGoal.class, excludeT1, null,
                 DeterministicCluster.rackAwareSatisfiable(), noDeadBroker, true));
    // With excluded topics, rack aware satisfiable cluster, one dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(1, KafkaAssignerEvenRackAwareGoal.class, excludeT1, null,
                 DeterministicCluster.rackAwareSatisfiable(), deadBroker0, true));
    // Without excluded topics, rack aware satisfiable cluster, no dead brokers (No exception, Proposal expected, Expected to look optimized)
    p.add(params(2, KafkaAssignerEvenRackAwareGoal.class, noExclusion, null,
                 DeterministicCluster.rackAwareSatisfiable(), noDeadBroker, true));
    // Without excluded topics, rack aware satisfiable cluster, one dead broker (No exception, Proposal expected, Expected to look optimized)
    p.add(params(3, KafkaAssignerEvenRackAwareGoal.class, noExclusion, null,
                 DeterministicCluster.rackAwareSatisfiable(), deadBroker0, true));
    // With excluded topics, rack aware unsatisfiable cluster, no dead broker (No exception, No proposal, Expected to look optimized)
    p.add(params(4, KafkaAssignerEvenRackAwareGoal.class, excludeT1, null,
                 DeterministicCluster.rackAwareUnsatisfiable(), noDeadBroker, true));
    // With excluded topics, rack aware unsatisfiable cluster, one dead broker (Exception)
    p.add(params(5, KafkaAssignerEvenRackAwareGoal.class, excludeT1, OptimizationFailureException.class,
                 DeterministicCluster.rackAwareUnsatisfiable(), deadBroker0, null));
    // Test: Without excluded topics, rack aware unsatisfiable cluster, no dead brokers (Exception expected)
    p.add(params(6, KafkaAssignerEvenRackAwareGoal.class, noExclusion, OptimizationFailureException.class,
                 DeterministicCluster.rackAwareUnsatisfiable(), noDeadBroker, null));
    // Test: Without excluded topics, rack aware unsatisfiable cluster, one dead broker (Exception expected)
    p.add(params(7, KafkaAssignerEvenRackAwareGoal.class, noExclusion, OptimizationFailureException.class,
                 DeterministicCluster.rackAwareUnsatisfiable(), deadBroker0, null));

    return p;
  }

  private int _testId;
  private Goal _goal;
  private OptimizationOptions _optimizationOptions;
  private Class<Throwable> _exceptionClass;
  private ClusterModel _clusterModel;
  private Boolean _expectedToOptimize;

  /**
   * Constructor of Excluded Topics Test.
   *
   * @param testId the test id
   * @param goal Goal to be tested.
   * @param excludedTopics Topics to be excluded from the goal.
   * @param exceptionClass Expected exception class (if any).
   * @param clusterModel Cluster model to be used for the test.
   * @param expectedToOptimize The expectation on whether the cluster state will be considered optimized or not.
   */
  public ExcludedTopicsTest(int testId,
                            Goal goal,
                            Set<String> excludedTopics,
                            Class<Throwable> exceptionClass,
                            ClusterModel clusterModel,
                            Boolean expectedToOptimize) {
    _testId = testId;
    _goal = goal;
    _optimizationOptions = new OptimizationOptions(excludedTopics);
    _exceptionClass = exceptionClass;
    _clusterModel = clusterModel;
    _expectedToOptimize = expectedToOptimize;
  }

  @Test
  public void test() throws Exception {
    if (_exceptionClass == null) {
      Map<TopicPartition, List<Integer>> initReplicaDistribution = _clusterModel.getReplicaDistribution();
      Map<TopicPartition, Integer> initLeaderDistribution = _clusterModel.getLeaderDistribution();

      Set<String> excludedTopics = _optimizationOptions.excludedTopics();
      if (_expectedToOptimize) {
        assertTrue("Excluded Topics Test failed to optimize " + _goal.name() + " with excluded topics.",
            _goal.optimize(_clusterModel, Collections.emptySet(), _optimizationOptions));
      } else {
        assertFalse("Excluded Topics Test optimized " + _goal.name() + " with excluded topics " + excludedTopics,
                    _goal.optimize(_clusterModel, Collections.emptySet(), _optimizationOptions));
      }
      // Generated proposals cannot have the excluded topic.
      if (!excludedTopics.isEmpty()) {
        Set<ExecutionProposal> goalProposals =
            AnalyzerUtils.getDiff(initReplicaDistribution, initLeaderDistribution, _clusterModel);

        for (ExecutionProposal proposal : goalProposals) {
          if (excludedTopics.contains(proposal.topic())) {
            for (int brokerId : proposal.replicasToRemove()) {
              if (_clusterModel.broker(brokerId).isAlive()) {
                fail(String.format("Proposal %s contains excluded topic %s, but the broker %d is still alive.",
                                   proposal, proposal.topic(), brokerId));
              }
            }
          }
        }
      }
    } else {
      expected.expect(_exceptionClass);
      assertTrue("Excluded Topics Test failed to optimize with excluded topics.",
          _goal.optimize(_clusterModel, Collections.emptySet(), _optimizationOptions));
    }
  }

  private static Object[] params(int tid,
                                 Class<? extends Goal> goalClass,
                                 Collection<String> excludedTopics,
                                 Class<? extends Throwable> exceptionClass,
                                 ClusterModel clusterModel,
                                 Collection<Integer> deadBrokers,
                                 Boolean expectedToOptimize) throws Exception {
    deadBrokers.forEach(id -> clusterModel.setBrokerState(id, Broker.State.DEAD));
    return new Object[]{tid, goal(goalClass), excludedTopics, exceptionClass, clusterModel, expectedToOptimize};
  }

  private static Goal goal(Class<? extends Goal> goalClass) throws Exception {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(1L));
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    try {
      Constructor<? extends Goal> constructor = goalClass.getDeclaredConstructor(BalancingConstraint.class);
      constructor.setAccessible(true);
      return constructor.newInstance(balancingConstraint);
    } catch (NoSuchMethodException badConstructor) {
      //Try default constructor
      return goalClass.newInstance();
    }
  }
}
