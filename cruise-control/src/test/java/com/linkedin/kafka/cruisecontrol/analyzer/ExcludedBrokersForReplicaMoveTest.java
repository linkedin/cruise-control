/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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

import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.RACK_BY_BROKER;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalanced;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalanced2;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;


/**
 * Unit test for testing goals with excluded brokers for replica move under fixed cluster properties.
 */
@RunWith(Parameterized.class)
public class ExcludedBrokersForReplicaMoveTest {

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Parameterized.Parameters(name = "{1}-{0}")
  public static Collection<Object[]> data() throws Exception {
    Collection<Object[]> p = new ArrayList<>();

    Set<Integer> noExclusion = Collections.emptySet();
    Set<Integer> excludeB1 = Collections.unmodifiableSet(Collections.singleton(1));
    Set<Integer> excludeB2 = Collections.unmodifiableSet(Collections.singleton(2));
    Set<Integer> excludeAllBrokers = Collections.unmodifiableSet(RACK_BY_BROKER.keySet());
    Set<Integer> noDeadBroker = Collections.emptySet();
    Set<Integer> deadBroker0 = Collections.unmodifiableSet(Collections.singleton(0));

    // ============RackAwareGoal============
    // With single excluded broker, rack aware satisfiable cluster, no dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(0, RackAwareGoal.class, excludeB1, null, DeterministicCluster.rackAwareSatisfiable(), noDeadBroker, true));
    // With single excluded broker, rack aware satisfiable cluster -- but with a broker excluded on the other rack, no
    // dead brokers (Exception)
    p.add(params(1, RackAwareGoal.class, excludeB2, OptimizationFailureException.class, DeterministicCluster.rackAwareSatisfiable(), noDeadBroker, null));
    // With single excluded broker, rack aware satisfiable cluster, one dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(2, RackAwareGoal.class, excludeB2, null, DeterministicCluster.rackAwareSatisfiable(), deadBroker0, true));
    // Without excluded broker, rack aware satisfiable cluster, no dead brokers (No exception, Proposal expected, Expected to look optimized)
    p.add(params(3, RackAwareGoal.class, noExclusion, null, DeterministicCluster.rackAwareSatisfiable(), noDeadBroker, true));
    // Without excluded broker, rack aware satisfiable cluster, one dead broker (No exception, Proposal expected, Expected to look optimized)
    p.add(params(4, RackAwareGoal.class, noExclusion, null, DeterministicCluster.rackAwareSatisfiable(), deadBroker0, true));
    // With single excluded broker, rack aware unsatisfiable cluster, no dead broker (Exception)
    p.add(params(5, RackAwareGoal.class, excludeB1, OptimizationFailureException.class, DeterministicCluster.rackAwareUnsatisfiable(), noDeadBroker, null));
    // With single excluded broker, rack aware unsatisfiable cluster, one dead broker (Exception)
    p.add(params(6, RackAwareGoal.class, excludeB1, OptimizationFailureException.class, DeterministicCluster.rackAwareUnsatisfiable(), deadBroker0, null));

    for (Class<? extends Goal> goalClass : Arrays.asList(CpuCapacityGoal.class,
                                                         DiskCapacityGoal.class,
                                                         NetworkInboundCapacityGoal.class,
                                                         NetworkOutboundCapacityGoal.class,
                                                         ReplicaCapacityGoal.class)) {
      // Test: With single excluded broker, satisfiable cluster, no dead brokers (No exception, No proposal
      // for excluded broker, Expected to look optimized)
      p.add(params(0, goalClass, excludeB1, null, unbalanced(), noDeadBroker, true));
      // Test: With single excluded broker, satisfiable cluster, one dead brokers (No exception, No proposal
      // for excluded broker, Expected to look optimized)
      p.add(params(1, goalClass, excludeB1, null, unbalanced(), deadBroker0, true));
      // Test: With all brokers excluded, no dead brokers, not satisfiable (Exception)
      p.add(params(2, goalClass, excludeAllBrokers, OptimizationFailureException.class, unbalanced(), noDeadBroker, null));
      // Test: With all brokers excluded, one dead brokers, not satisfiable, no exception.
      p.add(params(3, goalClass, excludeAllBrokers, null, unbalanced(), deadBroker0, true));
    }

    for (Class<? extends Goal> goalClass : Arrays.asList(DiskUsageDistributionGoal.class,
                                                         NetworkInboundUsageDistributionGoal.class,
                                                         NetworkOutboundUsageDistributionGoal.class,
                                                         CpuUsageDistributionGoal.class)) {
      // Test: With single excluded broker, balance not satisfiable cluster, no dead broker (No exception, No proposal
      // for excluded broker, Not expected to look optimized)
      p.add(params(0, goalClass, excludeB1, null, unbalanced(), noDeadBroker, false));
      // Test: With single excluded topic, balance not satisfiable cluster, no dead broker (No exception, No proposal
      // for excluded broker, Not expected to look optimized)
      p.add(params(1, goalClass, excludeB1, null, unbalanced(), deadBroker0, true));
      // Test: With all brokers excluded, no dead brokers, balance not satisfiable (No exception, No proposal for
      // excluded brokers, Not expected to look optimized)
      p.add(params(2, goalClass, excludeAllBrokers, null, unbalanced(), noDeadBroker, false));
      // Test: With all brokers excluded, no dead brokers, balance not satisfiable (No exception, No proposal for
      // excluded brokers, Not expected to look optimized)
      p.add(params(3, goalClass, excludeAllBrokers, null, unbalanced(), deadBroker0, true));
    }

    // ============LeaderBytesInDistributionGoal============
    // Test: With single excluded broker, balance not satisfiable cluster, no dead broker (No exception, No proposal
    // for excluded broker, Not expected to look optimized)
    p.add(params(0, LeaderBytesInDistributionGoal.class, excludeB1, null, unbalanced(), noDeadBroker, false));
    // Test: With single excluded broker, balance not satisfiable cluster, no dead broker (No exception, No proposal
    // for excluded broker, Not expected to look optimized)
    p.add(params(1, LeaderBytesInDistributionGoal.class, excludeB1, null, unbalanced(), deadBroker0, false));
    // Test: With all brokers excluded, no dead brokers, balance not satisfiable (No exception, No proposal for
    // excluded brokers, Not expected to look optimized)
    p.add(params(2, LeaderBytesInDistributionGoal.class, excludeAllBrokers, null, unbalanced(), noDeadBroker, false));
    // Test: With all brokers excluded, no dead brokers, balance not satisfiable (No exception, No proposal for
    // excluded brokers, Not expected to look optimized)
    p.add(params(3, LeaderBytesInDistributionGoal.class, excludeAllBrokers, null, unbalanced(), deadBroker0, false));

    // ============PotentialNwOutGoal============
    // Test: With single excluded broker, balance satisfiable cluster, no dead brokers (No exception, No proposal
    // for excluded broker, Expected to look optimized)
    p.add(params(0, PotentialNwOutGoal.class, excludeB1, null, unbalanced(), noDeadBroker, true));
    // Test: With single excluded broker, balance satisfiable cluster, one dead brokers (No exception, No proposal
    // for excluded broker, Expected to look optimized)
    p.add(params(1, PotentialNwOutGoal.class, excludeB1, null, unbalanced(), deadBroker0, true));
    // Test: With all brokers excluded, balance not satisfiable, no dead brokers (No exception, No proposal for
    // excluded brokers, Not expected to look optimized)
    p.add(params(2, PotentialNwOutGoal.class, excludeAllBrokers, null, unbalanced(), noDeadBroker, false));
    // Test: With all brokers excluded, balance not satisfiable, one dead brokers (No exception, No proposal for
    // excluded brokers, expected to look optimized)
    p.add(params(3, PotentialNwOutGoal.class, excludeAllBrokers, null, unbalanced(), deadBroker0, true));

    // ============TopicReplicaDistributionGoal============
    // Test: With single excluded broker, satisfiable cluster, no dead broker (No exception, No proposal for
    // excluded broker, Expected to look optimized)
    p.add(params(0, TopicReplicaDistributionGoal.class, excludeB1, null, unbalanced(), noDeadBroker, true));
    // Test: With single excluded broker, satisfiable cluster, one dead broker (No exception, No proposal for
    // excluded broker, Expected to look optimized)
    p.add(params(1, TopicReplicaDistributionGoal.class, excludeB1, null, unbalanced(), deadBroker0, true));
    // Test: With all brokers excluded, balance not satisfiable, no dead brokers (No exception, No proposal
    // for excluded brokers, Expected to look optimized)
    p.add(params(2, TopicReplicaDistributionGoal.class, excludeAllBrokers, null, unbalanced(), noDeadBroker, true));
    // Test: With all brokers excluded, balance not satisfiable, one dead brokers (No exception, No proposal
    // for excluded brokers, Expected to look optimized)
    p.add(params(3, TopicReplicaDistributionGoal.class, excludeAllBrokers, null, unbalanced(), deadBroker0, true));

    // ============ReplicaDistributionGoal============
    // Test: With single excluded broker, satisfiable cluster, no dead broker (No exception, No proposal for
    // excluded broker, Expected to look optimized)
    p.add(params(0, ReplicaDistributionGoal.class, excludeB1, null, unbalanced2(), noDeadBroker, false));
    // Test: With single excluded broker, satisfiable cluster, one dead broker (No exception, No proposal for
    // excluded broker, Expected to look optimized)
    p.add(params(1, ReplicaDistributionGoal.class, excludeB1, null, unbalanced2(), deadBroker0, true));
    // Test: With all brokers excluded, balance not satisfiable, no dead brokers (No exception, No proposal
    // for excluded brokers, Expected to look optimized)
    p.add(params(2, ReplicaDistributionGoal.class, excludeAllBrokers, null, unbalanced2(), noDeadBroker, false));
    // Test: With all brokers excluded, balance not satisfiable, one dead brokers (No exception, No proposal
    // for excluded brokers, Expected to look optimized)
    p.add(params(3, ReplicaDistributionGoal.class, excludeAllBrokers, null, unbalanced2(), deadBroker0, true));

    return p;
  }

  private int _testId;
  private Goal _goal;
  private OptimizationOptions _optimizationOptions;
  private Class<Throwable> _exceptionClass;
  private ClusterModel _clusterModel;
  private Boolean _expectedToOptimize;

  /**
   * Constructor of Excluded Brokers For Replica Move Test.
   *
   * @param testId the test id
   * @param goal Goal to be tested.
   * @param excludedBrokersForReplicaMove Brokers excluded from receiving replicas upon proposal generation.
   * @param exceptionClass Expected exception class (if any).
   * @param clusterModel Cluster model to be used for the test.
   * @param expectedToOptimize The expectation on whether the cluster state will be considered optimized or not.
   */
  public ExcludedBrokersForReplicaMoveTest(int testId,
                                           Goal goal,
                                           Set<Integer> excludedBrokersForReplicaMove,
                                           Class<Throwable> exceptionClass,
                                           ClusterModel clusterModel,
                                           Boolean expectedToOptimize) {
    _testId = testId;
    _goal = goal;
    _optimizationOptions = new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), excludedBrokersForReplicaMove);
    _exceptionClass = exceptionClass;
    _clusterModel = clusterModel;
    _expectedToOptimize = expectedToOptimize;
  }

  private boolean violatesExcludedBrokersForReplicaMove(Set<Integer> excludedBrokersForReplicaMove,
                                                        ExecutionProposal proposal) {
    int numOfflineOldReplicas =
        (int) proposal.oldReplicas().stream().filter(brokerId -> !_clusterModel.broker(brokerId).isAlive()).count();

    int numNewReplicasOnExcludedBrokers = 0;
    for (int i = 0; i < proposal.newReplicas().size(); i++) {
      int oldBroker = proposal.oldReplicas().get(i);
      int newBroker = proposal.newReplicas().get(i);
      if (oldBroker != newBroker && excludedBrokersForReplicaMove.contains(newBroker)) {
        numNewReplicasOnExcludedBrokers++;
      }
    }

    return numNewReplicasOnExcludedBrokers > numOfflineOldReplicas;
  }

  @Test
  public void test() throws Exception {
    if (_exceptionClass == null) {
      Map<TopicPartition, List<Integer>> initReplicaDistribution = _clusterModel.getReplicaDistribution();
      Map<TopicPartition, Integer> initLeaderDistribution = _clusterModel.getLeaderDistribution();

      Set<Integer> excludedBrokersForReplicaMove = _optimizationOptions.excludedBrokersForReplicaMove();
      if (_expectedToOptimize) {
        assertTrue("Failed to optimize " + _goal.name() + " with excluded brokers for replica move.",
                   _goal.optimize(_clusterModel, Collections.emptySet(), _optimizationOptions));
      } else {
        assertFalse("Optimized " + _goal.name() + " with excluded brokers for replicaMove " + excludedBrokersForReplicaMove,
                    _goal.optimize(_clusterModel, Collections.emptySet(), _optimizationOptions));
      }
      // Generated proposals cannot move replicas to the excluded brokers for replica move.
      if (!excludedBrokersForReplicaMove.isEmpty()) {
        Set<ExecutionProposal> goalProposals =
            AnalyzerUtils.getDiff(initReplicaDistribution, initLeaderDistribution, _clusterModel);
        for (ExecutionProposal proposal : goalProposals) {
          if (proposal.hasReplicaAction() && violatesExcludedBrokersForReplicaMove(excludedBrokersForReplicaMove, proposal)) {
            fail(String.format("A replica move in %s to an excluded broker for replica move %s.",
                               proposal, excludedBrokersForReplicaMove));
          }
        }
      }
    } else {
      expected.expect(_exceptionClass);
      assertTrue("Failed to optimize with excluded brokers for replica move.",
                 _goal.optimize(_clusterModel, Collections.emptySet(), _optimizationOptions));
    }
  }

  private static Object[] params(int tid,
                                 Class<? extends Goal> goalClass,
                                 Set<Integer> excludedBrokersForReplicaMove,
                                 Class<? extends Throwable> exceptionClass,
                                 ClusterModel clusterModel,
                                 Collection<Integer> deadBrokers,
                                 Boolean expectedToOptimize) throws Exception {
    deadBrokers.forEach(id -> clusterModel.setBrokerState(id, Broker.State.DEAD));
    return new Object[]{tid, goal(goalClass), excludedBrokersForReplicaMove, exceptionClass, clusterModel, expectedToOptimize};
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
