/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
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
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;


/**
 * Unit test for testing goals with excluded topics under fixed cluster properties.
 */
@RunWith(Parameterized.class)
public class ExcludedTopicsTest {

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Parameters(name = "{1}-{0}")
  public static Collection<Object[]> data() throws Exception {
    Collection<Object[]> p = new ArrayList<>();

    Set<String> noExclusion = Collections.emptySet();
    Set<String> excludeT1 = Collections.unmodifiableSet(Collections.singleton("T1"));
    Set<String> excludeAllTopics = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("T1", "T2")));
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
      // excluded topic, Not expected to look optimized)
      p.add(params(2, goalClass, excludeAllTopics, null, unbalanced(), noDeadBroker, false));
      // Test: With all topics excluded, no dead brokers, balance not satisfiable (No exception, No proposal for
      // excluded topic, Not expected to look optimized)
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
    // excluded topic, Not expected to look optimized)
    p.add(params(2, LeaderBytesInDistributionGoal.class, excludeAllTopics, null, unbalanced(), noDeadBroker, false));
    // Test: With all topics excluded, no dead brokers, balance not satisfiable (No exception, No proposal for
    // excluded topic, Not expected to look optimized)
    p.add(params(3, LeaderBytesInDistributionGoal.class, excludeAllTopics, null, unbalanced(), deadBroker0, false));

    // ============PotentialNwOutGoal============
    // Test: With single excluded topic, balance satisfiable cluster, no dead brokers (No exception, No proposal
    // for excluded topic, Expected to look optimized)
    p.add(params(0, PotentialNwOutGoal.class, excludeT1, null, unbalanced(), noDeadBroker, true));
    // Test: With single excluded topic, balance satisfiable cluster, one dead brokers (No exception, No proposal
    // for excluded topic, Expected to look optimized)
    p.add(params(1, PotentialNwOutGoal.class, excludeT1, null, unbalanced(), deadBroker0, true));
    // Test: With all topics excluded, balance not satisfiable, no dead brokers (No exception, No proposal for
    // excluded topic, Not expected to look optimized)
    p.add(params(2, PotentialNwOutGoal.class, excludeAllTopics, null, unbalanced(), noDeadBroker, false));
    // Test: With all topics excluded, balance not satisfiable, one dead brokers (No exception, No proposal for
    // excluded topic, expected to look optimized)
    p.add(params(3, PotentialNwOutGoal.class, excludeAllTopics, null, unbalanced(), deadBroker0, true));

    // ============TopicReplicaDistributionGoal============
    // Test: With single excluded topic, satisfiable cluster, no dead broker (No exception, No proposal for
    // excluded topic, Expected to look optimized)
    p.add(params(0, TopicReplicaDistributionGoal.class, excludeT1, null, unbalanced(), noDeadBroker, true));
    // Test: With single excluded topic, satisfiable cluster, one dead broker (No exception, No proposal for
    // excluded topic, Expected to look optimized)
    p.add(params(1, TopicReplicaDistributionGoal.class, excludeT1, null, unbalanced(), deadBroker0, true));
    // Test: With all topics excluded, balance not satisfiable, no dead brokers (No exception, No proposal
    // for excluded topic, Expected to look optimized)
    p.add(params(2, TopicReplicaDistributionGoal.class, excludeAllTopics, null, unbalanced(), noDeadBroker, true));
    // Test: With all topics excluded, balance not satisfiable, one dead brokers (No exception, No proposal
    // for excluded topic, Expected to look optimized)
    p.add(params(3, TopicReplicaDistributionGoal.class, excludeAllTopics, null, unbalanced(), deadBroker0, true));

    // ============ReplicaDistributionGoal============
    // Test: With single excluded topic, satisfiable cluster, no dead broker (No exception, No proposal for
    // excluded topic, Expected to look optimized)
    p.add(params(0, ReplicaDistributionGoal.class, excludeT1, null, unbalanced2(), noDeadBroker, true));
    // Test: With single excluded topic, satisfiable cluster, one dead broker (No exception, No proposal for
    // excluded topic, Expected to look optimized)
    p.add(params(1, ReplicaDistributionGoal.class, excludeT1, null, unbalanced2(), deadBroker0, true));
    // Test: With all topics excluded, balance not satisfiable, no dead brokers (No exception, No proposal
    // for excluded topic, Expected to look optimized)
    p.add(params(2, ReplicaDistributionGoal.class, excludeAllTopics, null, unbalanced2(), noDeadBroker, false));
    // Test: With all topics excluded, balance not satisfiable, one dead brokers (No exception, No proposal
    // for excluded topic, Expected to look optimized)
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
  private Set<String> _excludedTopics;
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
    _excludedTopics = excludedTopics;
    _exceptionClass = exceptionClass;
    _clusterModel = clusterModel;
    _expectedToOptimize = expectedToOptimize;
  }

  @Test
  public void test() throws Exception {
    if (_exceptionClass == null) {
      Map<TopicPartition, List<Integer>> initReplicaDistribution = _clusterModel.getReplicaDistribution();
      Map<TopicPartition, Integer> initLeaderDistribution = _clusterModel.getLeaderDistribution();

      if (_expectedToOptimize) {
        assertTrue("Excluded Topics Test failed to optimize " + _goal.name() + " with excluded topics.",
            _goal.optimize(_clusterModel, Collections.emptySet(), _excludedTopics));
      } else {
        assertFalse("Excluded Topics Test optimized " + _goal.name() + " with excluded topics " + _excludedTopics,
            _goal.optimize(_clusterModel, Collections.emptySet(), _excludedTopics));
      }
      // Generated proposals cannot have the excluded topic.
      if (!_excludedTopics.isEmpty()) {
        Set<ExecutionProposal> goalProposals =
            AnalyzerUtils.getDiff(initReplicaDistribution, initLeaderDistribution, _clusterModel);

        for (ExecutionProposal proposal : goalProposals) {
          if (_excludedTopics.contains(proposal.topic())) {
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
          _goal.optimize(_clusterModel, Collections.emptySet(), _excludedTopics));
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

  // two racks, three brokers, two partitions, one replica.
  private static ClusterModel unbalanced() {

    List<Integer> orderedRackIdsOfBrokers = Arrays.asList(0, 0, 1);
    ClusterModel cluster = DeterministicCluster.getHomogeneousDeterministicCluster(2, orderedRackIdsOfBrokers,
                                                                                   TestConstants.BROKER_CAPACITY);

    // Create topic partition.
    TopicPartition pInfoT10 = new TopicPartition("T1", 0);
    TopicPartition pInfoT20 = new TopicPartition("T2", 0);

    // Create replicas for topic: T1.
    cluster.createReplica("0", 0, pInfoT10, 0, true);
    cluster.createReplica("0", 0, pInfoT20, 0, true);

    AggregatedMetricValues aggregatedMetricValues =
        KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 2,
                                                                  TestConstants.LARGE_BROKER_CAPACITY / 2,
                                                                  TestConstants.MEDIUM_BROKER_CAPACITY / 2,
                                                                  TestConstants.LARGE_BROKER_CAPACITY / 2);

    // Create snapshots and push them to the cluster.
    cluster.setReplicaLoad("0", 0, pInfoT10, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad("0", 0, pInfoT20, aggregatedMetricValues, Collections.singletonList(1L));

    return cluster;
  }

 // two racks, three brokers, six partitions, one replica.
  private static ClusterModel unbalanced2() {

    ClusterModel cluster = unbalanced();
    // Create topic partition.
    TopicPartition pInfoT30 = new TopicPartition("T1", 1);
    TopicPartition pInfoT40 = new TopicPartition("T2", 1);
    TopicPartition pInfoT50 = new TopicPartition("T1", 2);
    TopicPartition pInfoT60 = new TopicPartition("T2", 2);
    // Create replicas for topic: T1.
    cluster.createReplica("0", 1, pInfoT30, 0, true);
    cluster.createReplica("0", 0, pInfoT40, 0, true);
    cluster.createReplica("0", 0, pInfoT50, 0, true);
    cluster.createReplica("0", 0, pInfoT60, 0, true);

    AggregatedMetricValues aggregatedMetricValues =
        KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues(TestConstants.LARGE_BROKER_CAPACITY / 2,
                                                                  TestConstants.LARGE_BROKER_CAPACITY / 2,
                                                                  TestConstants.MEDIUM_BROKER_CAPACITY / 2,
                                                                  TestConstants.LARGE_BROKER_CAPACITY / 2);

    // Create snapshots and push them to the cluster.
    cluster.setReplicaLoad("0", 1, pInfoT30, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad("0", 0, pInfoT40, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad("0", 0, pInfoT50, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad("0", 0, pInfoT60, aggregatedMetricValues, Collections.singletonList(1L));
    return cluster;
  }
}
