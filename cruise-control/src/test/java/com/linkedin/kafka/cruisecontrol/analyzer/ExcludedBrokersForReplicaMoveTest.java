/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.MinTopicLeadersPerBrokerGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
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

import static com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUnitTestUtils.goal;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.RACK_BY_BROKER;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalanced;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalanced2;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalanced3;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalanced4;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalanced5;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalancedWithAFollower;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.minLeaderReplicaPerBrokerSatisfiable;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.minLeaderReplicaPerBrokerSatisfiable2;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.rackAwareSatisfiable;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.rackAwareUnsatisfiable;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.rackAwareSatisfiable2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;


/**
 * Unit test for testing goals with excluded brokers for replica move under fixed cluster properties.
 */
@RunWith(Parameterized.class)
public class ExcludedBrokersForReplicaMoveTest {

  @Rule
  public ExpectedException _expected = ExpectedException.none();

  private final int _testId;
  private final Goal _goal;
  private final OptimizationOptions _optimizationOptions;
  private final Class<Throwable> _exceptionClass;
  private final ClusterModel _clusterModel;
  private final Boolean _expectedToOptimize;
  private final Boolean _expectedToGenerateProposals;

  /**
   * Constructor of Excluded Brokers For Replica Move Test.
   *
   * @param testId the test id
   * @param goal Goal to be tested.
   * @param excludedBrokersForReplicaMove Brokers excluded from receiving replicas upon proposal generation.
   * @param exceptionClass Expected exception class (if any).
   * @param clusterModel Cluster model to be used for the test.
   * @param expectedToOptimize The expectation on whether the cluster state will be considered optimized or not.
   * @param expectedToGenerateProposals The expectation on whether the optimization process will generate proposals or not,
   *                                   or {@code null} if proposal generation is irrelevant due to exception.
   */
  public ExcludedBrokersForReplicaMoveTest(int testId,
                                           Goal goal,
                                           Set<Integer> excludedBrokersForReplicaMove,
                                           Class<Throwable> exceptionClass,
                                           ClusterModel clusterModel,
                                           Boolean expectedToOptimize,
                                           Boolean expectedToGenerateProposals) {
    _testId = testId;
    _goal = goal;
    _optimizationOptions = new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), excludedBrokersForReplicaMove);
    _exceptionClass = exceptionClass;
    _clusterModel = clusterModel;
    _expectedToOptimize = expectedToOptimize;
    _expectedToGenerateProposals = expectedToGenerateProposals;
  }

  /**
   * Populate parameters for the parametrized test.
   * @return Populated parameters.
   */
  @Parameterized.Parameters(name = "{1}-{0}")
  public static Collection<Object[]> data() throws Exception {
    Collection<Object[]> p = new ArrayList<>();

    Set<Integer> noExclusion = Collections.emptySet();
    Set<Integer> excludeB0 = Collections.singleton(0);
    Set<Integer> excludeB1 = Collections.singleton(1);
    Set<Integer> excludeB2 = Collections.singleton(2);
    Set<Integer> excludeAllBrokers = Collections.unmodifiableSet(RACK_BY_BROKER.keySet());
    Set<Integer> excludeB1B2 = new HashSet<>(RACK_BY_BROKER.keySet());
    excludeB1B2.removeAll(excludeB0);
    Set<Integer> noDeadBroker = Collections.emptySet();
    Set<Integer> deadBroker0 = Collections.singleton(0);
    Set<Integer> deadBroker1 = Collections.singleton(1);
    Set<Integer> deadBroker2 = Collections.singleton(2);

    // ============RackAwareGoal============
    // With single excluded broker, rack aware satisfiable cluster, no dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(0, RackAwareGoal.class, excludeB1, null, rackAwareSatisfiable(), noDeadBroker, true, true));
    // With single excluded broker, rack aware satisfiable cluster -- but with a broker excluded on the other rack, no
    // dead brokers (Exception)
    p.add(params(1, RackAwareGoal.class, excludeB2, OptimizationFailureException.class, rackAwareSatisfiable(),
                 noDeadBroker, null, null));
    // With single excluded broker, rack aware satisfiable cluster, one dead broker on this rack (Exception)
    p.add(params(2, RackAwareGoal.class, excludeB2, OptimizationFailureException.class, rackAwareSatisfiable(),
                 deadBroker0, null, null));
    // Without excluded broker, rack aware satisfiable cluster, no dead brokers (No exception, Proposal expected, Expected to look optimized)
    p.add(params(3, RackAwareGoal.class, noExclusion, null, rackAwareSatisfiable(), noDeadBroker, true, true));
    // Without excluded broker, rack aware satisfiable cluster, one dead broker (No exception, Proposal expected, Expected to look optimized)
    p.add(params(4, RackAwareGoal.class, noExclusion, null, rackAwareSatisfiable(), deadBroker0, true, true));
    // With single excluded broker, rack aware unsatisfiable cluster, no dead broker (Exception)
    p.add(params(5, RackAwareGoal.class, excludeB1, OptimizationFailureException.class, rackAwareUnsatisfiable(),
                 noDeadBroker, null, null));
    // With single excluded broker, rack aware unsatisfiable cluster, one dead broker (Exception)
    p.add(params(6, RackAwareGoal.class, excludeB1, OptimizationFailureException.class, rackAwareUnsatisfiable(),
                 deadBroker0, null, null));

    // ============RackAwareDistributionGoal============
    // With single excluded broker, rack aware satisfiable cluster, no dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(0, RackAwareDistributionGoal.class, excludeB1, null, rackAwareSatisfiable(), noDeadBroker, true, true));
    // With single excluded broker, rack aware satisfiable cluster -- but with a broker excluded on the other rack, no
    // dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(1, RackAwareDistributionGoal.class, excludeB2, null, rackAwareSatisfiable(),
                 noDeadBroker, true, false));
    // With single excluded broker, rack aware satisfiable cluster, one dead broker on this rack (Exception)
    p.add(params(2, RackAwareDistributionGoal.class, excludeB2, OptimizationFailureException.class, rackAwareSatisfiable(),
                 deadBroker0, null, null));
    // Without excluded broker, rack aware satisfiable cluster, no dead brokers (No exception, Proposal expected, Expected to look optimized)
    p.add(params(3, RackAwareDistributionGoal.class, noExclusion, null, rackAwareSatisfiable(), noDeadBroker, true, true));
    // Without excluded broker, rack aware satisfiable cluster, one dead broker (No exception, Proposal expected, Expected to look optimized)
    p.add(params(4, RackAwareDistributionGoal.class, noExclusion, null, rackAwareSatisfiable(), deadBroker0, true, true));
    // With single excluded broker, rack aware unsatisfiable but rack aware distribution satisfiable cluster, no dead broker
    // (No exception, No proposal, Expected to look optimized)
    p.add(params(5, RackAwareDistributionGoal.class, excludeB1, null, rackAwareUnsatisfiable(), noDeadBroker, true, false));
    // With single excluded broker, rack aware unsatisfiable cluster, one dead broker (Exception)
    p.add(params(6, RackAwareDistributionGoal.class, excludeB1, OptimizationFailureException.class, rackAwareUnsatisfiable(),
                 deadBroker0, null, null));
    // With single excluded broker, rack aware satisfiable cluster -- but with a broker excluded on the first rack, no
    // dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(7, RackAwareDistributionGoal.class, excludeB0, null, rackAwareSatisfiable2(),
                 noDeadBroker, true, true));

    // ============ReplicaCapacityGoal============
    // Test: With single excluded broker, satisfiable cluster, no dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(0, ReplicaCapacityGoal.class, excludeB1, null, unbalanced(), noDeadBroker, true, false));
    // Test: With single excluded broker, satisfiable cluster, one dead broker (No exception, Generates proposals, Expected to look optimized)
    p.add(params(1, ReplicaCapacityGoal.class, excludeB1, null, unbalanced(), deadBroker0, true, true));
    // Test: With all brokers excluded, no dead brokers, not satisfiable (Exception)
    p.add(params(2, ReplicaCapacityGoal.class, excludeAllBrokers, OptimizationFailureException.class, unbalanced(), noDeadBroker, null, null));
    // Test: With all brokers excluded, one dead broker, not satisfiable (Exception)
    p.add(params(3, ReplicaCapacityGoal.class, excludeAllBrokers, OptimizationFailureException.class, unbalanced(), deadBroker0, null, null));

    for (Class<? extends Goal> goalClass : Arrays.asList(CpuCapacityGoal.class,
                                                         DiskCapacityGoal.class,
                                                         NetworkInboundCapacityGoal.class,
                                                         NetworkOutboundCapacityGoal.class)) {
      // Test: With single excluded broker, satisfiable cluster, no dead brokers (No exception, Generates proposals, Expected to look optimized)
      p.add(params(0, goalClass, excludeB1, null, unbalanced(), noDeadBroker, true, true));
      // Test: With single excluded broker, satisfiable cluster, one dead broker (Exception)
      p.add(params(1, goalClass, excludeB1, OptimizationFailureException.class, unbalanced(), deadBroker0, null, null));
      // Test: With all brokers excluded, no dead brokers, not satisfiable (Exception)
      p.add(params(2, goalClass, excludeAllBrokers, OptimizationFailureException.class, unbalanced(), noDeadBroker, null, null));
      // Test: With all brokers excluded, one dead broker, not satisfiable (Exception)
      p.add(params(3, goalClass, excludeAllBrokers, OptimizationFailureException.class, unbalanced(), deadBroker0, null, null));
      // Test: With single excluded broker, unsatisfiable cluster, no dead brokers (Exception)
      p.add(params(4, goalClass, excludeB1, OptimizationFailureException.class, unbalanced2(), noDeadBroker, null, null));
    }

    for (Class<? extends Goal> goalClass : Arrays.asList(DiskUsageDistributionGoal.class,
                                                         NetworkInboundUsageDistributionGoal.class,
                                                         NetworkOutboundUsageDistributionGoal.class,
                                                         CpuUsageDistributionGoal.class)) {
      // Test: With single excluded broker, balance not satisfiable cluster, no dead broker (No exception, Generates proposals,
      // Expected to look optimized)
      p.add(params(0, goalClass, excludeB1, null, unbalanced(), noDeadBroker, true, true));
      // Test: With single excluded topic, balance not satisfiable cluster, no dead broker (No exception, Generates proposals,
      // Expected to look optimized)
      p.add(params(1, goalClass, excludeB1, null, unbalanced(), deadBroker0, true, true));
      // Test: With all brokers excluded, balance not satisfiable, no dead brokers (Exception)
      p.add(params(2, goalClass, excludeAllBrokers, OptimizationFailureException.class, unbalanced(), noDeadBroker, null, null));
      // Test: With all brokers excluded, one dead broker, (Exception)
      p.add(params(3, goalClass, excludeAllBrokers, OptimizationFailureException.class, unbalanced(), deadBroker0, null, null));
    }

    // ============MinTopicLeadersPerBrokerGoal============
    Properties configOverrides = new Properties();
    configOverrides.put(AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG, TestConstants.TOPIC_MUST_HAVE_LEADER_REPLICAS_ON_BROKERS);

    // Without excluded broker, no dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(0, MinTopicLeadersPerBrokerGoal.class, noExclusion, null,
                 minLeaderReplicaPerBrokerSatisfiable(), noDeadBroker, true, true, configOverrides));
    p.add(params(1, MinTopicLeadersPerBrokerGoal.class, noExclusion, null,
                 minLeaderReplicaPerBrokerSatisfiable2(), noDeadBroker, true, true, configOverrides));

    // With single excluded broker, no dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(2, MinTopicLeadersPerBrokerGoal.class, excludeB1, null,
                 minLeaderReplicaPerBrokerSatisfiable(), noDeadBroker, true, true, configOverrides));
    p.add(params(3, MinTopicLeadersPerBrokerGoal.class, excludeB1, null,
                 minLeaderReplicaPerBrokerSatisfiable2(), noDeadBroker, true, true, configOverrides));

    // With single excluded broker, no dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(4, MinTopicLeadersPerBrokerGoal.class, excludeB2, null,
                 minLeaderReplicaPerBrokerSatisfiable(), noDeadBroker, true, false, configOverrides));
    p.add(params(5, MinTopicLeadersPerBrokerGoal.class, excludeB2, null,
                 minLeaderReplicaPerBrokerSatisfiable2(), noDeadBroker, true, true, configOverrides));

    // Without excluded broker, one dead broker (No exception, Generates proposals, Expected to look optimized)
    p.add(params(6, MinTopicLeadersPerBrokerGoal.class, noExclusion, null,
                 minLeaderReplicaPerBrokerSatisfiable(), deadBroker0, true, true, configOverrides));
    p.add(params(7, MinTopicLeadersPerBrokerGoal.class, noExclusion, null,
                 minLeaderReplicaPerBrokerSatisfiable2(), deadBroker0, true, true, configOverrides));

    // Without excluded broker, one dead broker (No exception, Generates proposals, Expected to look optimized)
    p.add(params(8, MinTopicLeadersPerBrokerGoal.class, noExclusion, null,
                 minLeaderReplicaPerBrokerSatisfiable(), deadBroker2, true, true, configOverrides));
    p.add(params(9, MinTopicLeadersPerBrokerGoal.class, noExclusion, null,
                 minLeaderReplicaPerBrokerSatisfiable2(), deadBroker2, true, true, configOverrides));

    // With a single excluded broker, one dead brokers, not satisfiable (Exception)
    p.add(params(10, MinTopicLeadersPerBrokerGoal.class, excludeB1, OptimizationFailureException.class,
                 minLeaderReplicaPerBrokerSatisfiable(), deadBroker2, null, null, configOverrides));
    p.add(params(11, MinTopicLeadersPerBrokerGoal.class, excludeB1, OptimizationFailureException.class,
                 minLeaderReplicaPerBrokerSatisfiable2(), deadBroker2, null, null, configOverrides));

    // With a single excluded broker, one dead brokers, not satisfiable (Exception)
    p.add(params(12, MinTopicLeadersPerBrokerGoal.class, excludeB0, OptimizationFailureException.class,
                 minLeaderReplicaPerBrokerSatisfiable(), deadBroker2, null, null, configOverrides));
    // With single excluded broker, one dead brokers, satisfiable
    p.add(params(13, MinTopicLeadersPerBrokerGoal.class, excludeB0, null,
                 minLeaderReplicaPerBrokerSatisfiable2(), deadBroker2, true, true, configOverrides));

    // ============LeaderBytesInDistributionGoal============
    // Test: With single excluded broker, balance not satisfiable cluster, no dead broker (No exception, No proposal,
    // Not expected to look optimized)
    p.add(params(0, LeaderBytesInDistributionGoal.class, excludeB1, null, unbalanced(), noDeadBroker, false, false));
    // Test: With single excluded broker, balance not satisfiable cluster, one dead broker (No exception, No proposal,
    // Not expected to look optimized)
    p.add(params(1, LeaderBytesInDistributionGoal.class, excludeB1, null, unbalanced(), deadBroker0, false, false));
    // Test: With all brokers excluded, no dead broker, (Exception)
    p.add(params(2, LeaderBytesInDistributionGoal.class, excludeAllBrokers, OptimizationFailureException.class, unbalanced(), noDeadBroker,
                 null, null));
    // Test: With all brokers excluded, one dead broker, (Exception)
    p.add(params(3, LeaderBytesInDistributionGoal.class, excludeAllBrokers, OptimizationFailureException.class, unbalanced(), deadBroker0,
                 null, null));
    // Test: With single excluded broker, balance satisfiable cluster, no dead broker (No exception, Generates proposals,
    // Expected to look optimized)
    p.add(params(0, LeaderBytesInDistributionGoal.class, excludeB1, null, unbalancedWithAFollower(), noDeadBroker,
                 true, true));

    // ============PotentialNwOutGoal============
    // Test: With single excluded broker, balance satisfiable cluster, no dead brokers (No exception, Generates proposals,
    // Expected to look optimized)
    p.add(params(0, PotentialNwOutGoal.class, excludeB1, null, unbalanced(), noDeadBroker, true, true));
    // Test: With single excluded broker, balance satisfiable cluster, one dead broker (No exception, Generates proposals,
    // Not expected to look optimized)
    p.add(params(1, PotentialNwOutGoal.class, excludeB1, null, unbalanced(), deadBroker0, false, true));
    // Test: With all brokers excluded, balance not satisfiable, no dead brokers (No exception, No proposal,
    // Not expected to look optimized)
    p.add(params(2, PotentialNwOutGoal.class, excludeAllBrokers, null, unbalanced(), noDeadBroker, false, false));
    // Test: With all brokers excluded, balance not satisfiable, one dead broker (Exception)
    p.add(params(3, PotentialNwOutGoal.class, excludeAllBrokers, OptimizationFailureException.class, unbalanced(), deadBroker0, null, null));

    // ============TopicReplicaDistributionGoal============
    // Test: With single excluded broker, satisfiable cluster, no dead broker (No exception, Generates proposals, Expected to look optimized)
    p.add(params(0, TopicReplicaDistributionGoal.class, excludeB1, null, unbalanced5(), noDeadBroker, true, true));
    // Test: With single excluded broker, already satisfiable cluster, one dead broker (No exception, Generates proposals,
    // Expected to look optimized)
    p.add(params(1, TopicReplicaDistributionGoal.class, excludeB1, null, unbalanced(), deadBroker0, true, true));
    // Test: With all brokers excluded, balance not satisfiable, no dead brokers (Exception)
    p.add(params(2, TopicReplicaDistributionGoal.class, excludeAllBrokers, OptimizationFailureException.class, unbalanced4(), noDeadBroker,
                 null, null));
    // Test: With all brokers excluded, balance not satisfiable, one dead broker (Exception)
    p.add(params(3, TopicReplicaDistributionGoal.class, excludeAllBrokers, OptimizationFailureException.class, unbalanced4(), deadBroker0,
                 null, null));
    // ============ReplicaDistributionGoal============
    // Test: With single excluded broker, satisfiable cluster, no dead broker (No exception, Generates proposals,
    // Expected to look optimized)
    p.add(params(0, ReplicaDistributionGoal.class, excludeB1, null, unbalanced2(), noDeadBroker, true, true));
    // Test: With single excluded broker, satisfiable cluster, one dead broker (No exception, Generates proposals,
    // Expected to look optimized)
    p.add(params(1, ReplicaDistributionGoal.class, excludeB1, null, unbalanced2(), deadBroker0, true, true));
    // Test: With all brokers excluded, balance not satisfiable, no dead brokers (Exception)
    p.add(params(2, ReplicaDistributionGoal.class, excludeAllBrokers, OptimizationFailureException.class, unbalanced2(), noDeadBroker, null, null));
    // Test: With all brokers excluded, balance not satisfiable, one dead broker (Exception)
    p.add(params(3, ReplicaDistributionGoal.class, excludeAllBrokers, OptimizationFailureException.class, unbalanced2(), deadBroker0, null, null));
    // Test: With two excluded broker, satisfiable cluster, one dead broker (No exception, Generates proposals,
    // Not expected to look optimized)
    p.add(params(4, ReplicaDistributionGoal.class, excludeB1B2, null, unbalanced2(), noDeadBroker, true, true));

    // ============LeaderReplicaDistributionGoal============
    // Test: With single excluded broker, satisfiable cluster, no dead broker (No exception, No proposal,
    // Expected to look optimized)
    p.add(params(0, LeaderReplicaDistributionGoal.class, excludeB1, null, unbalanced3(), noDeadBroker, true, false));
    // Test: With single excluded broker, unsatisfiable cluster, one dead broker (No exception, Generates proposals,
    // Not expected to look optimized)
    p.add(params(1, LeaderReplicaDistributionGoal.class, excludeB1, null, unbalanced3(), deadBroker0, false, true));
    // Test: With all brokers excluded, balance not satisfiable, no dead brokers (Exception)
    p.add(params(2, LeaderReplicaDistributionGoal.class, excludeAllBrokers, OptimizationFailureException.class, unbalanced3(), noDeadBroker,
                 null, null));
    // Test: With all brokers excluded, satisfiable cluster, one dead broker (Exception)
    p.add(params(3, LeaderReplicaDistributionGoal.class, excludeB2, OptimizationFailureException.class, unbalanced3(), deadBroker0, null, null));
    // Test: With single excluded broker, unsatisfiable cluster, one dead broker with followers (No exception, Generates proposals,
    // Not expected to look optimized)
    p.add(params(4, LeaderReplicaDistributionGoal.class, excludeB0, null, unbalanced3(), deadBroker1, false, true));
    return p;
  }

  private boolean violatesExcludedBrokersForReplicaMove(Set<Integer> excludedBrokersForReplicaMove,
                                                        ExecutionProposal proposal) {
    int numOfflineOldReplicas =
        (int) proposal.oldReplicas().stream().filter(r -> !_clusterModel.broker(r.brokerId()).isAlive()).count();

    int numNewReplicasOnExcludedBrokers = 0;
    for (int i = 0; i < proposal.newReplicas().size(); i++) {
      int oldBroker = proposal.oldReplicas().get(i).brokerId();
      int newBroker = proposal.newReplicas().get(i).brokerId();
      if (oldBroker != newBroker && excludedBrokersForReplicaMove.contains(newBroker)) {
        numNewReplicasOnExcludedBrokers++;
      }
    }

    return numNewReplicasOnExcludedBrokers > numOfflineOldReplicas;
  }

  @Test
  public void test() throws Exception {
    // Before the optimization, goals are expected to be undecided wrt their provision status.
    assertEquals(ProvisionStatus.UNDECIDED, _goal.provisionResponse().status());
    if (_exceptionClass == null) {
      Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = _clusterModel.getReplicaDistribution();
      Map<TopicPartition, ReplicaPlacementInfo> initLeaderDistribution = _clusterModel.getLeaderDistribution();

      Set<Integer> excludedBrokersForReplicaMove = _optimizationOptions.excludedBrokersForReplicaMove();
      if (_expectedToOptimize) {
        assertTrue("Failed to optimize " + _goal.name() + " with excluded brokers for replica move.",
                   _goal.optimize(_clusterModel, Collections.emptySet(), _optimizationOptions));
      } else {
        assertFalse("Optimized " + _goal.name() + " with excluded brokers for replicaMove " + excludedBrokersForReplicaMove,
                    _goal.optimize(_clusterModel, Collections.emptySet(), _optimizationOptions));
      }
      // The cluster cannot be underprovisioned, because _exceptionClass was null.
      assertNotEquals(ProvisionStatus.UNDER_PROVISIONED, _goal.provisionResponse().status());
      // Generated proposals cannot move replicas to the excluded brokers for replica move.
      if (!excludedBrokersForReplicaMove.isEmpty()) {
        Set<ExecutionProposal> goalProposals =
            AnalyzerUtils.getDiff(initReplicaDistribution, initLeaderDistribution, _clusterModel);
        assertEquals(_expectedToGenerateProposals, !goalProposals.isEmpty());
        for (ExecutionProposal proposal : goalProposals) {
          if (proposal.hasReplicaAction() && violatesExcludedBrokersForReplicaMove(excludedBrokersForReplicaMove, proposal)) {
            fail(String.format("A replica move in %s to an excluded broker for replica move %s.",
                               proposal, excludedBrokersForReplicaMove));
          }
        }
      }
    } else {
      _expected.expect(_exceptionClass);
      assertTrue("Failed to optimize with excluded brokers for replica move.",
                 _goal.optimize(_clusterModel, Collections.emptySet(), _optimizationOptions));
      assertEquals(ProvisionStatus.UNDER_PROVISIONED, _goal.provisionResponse().status());
    }
  }

  private static Object[] params(int tid,
                                 Class<? extends Goal> goalClass,
                                 Set<Integer> excludedBrokersForReplicaMove,
                                 Class<? extends Throwable> exceptionClass,
                                 ClusterModel clusterModel,
                                 Collection<Integer> deadBrokers,
                                 Boolean expectedToOptimize,
                                 Boolean expectedToGenerateProposals) throws Exception {
    return params(tid, goalClass, excludedBrokersForReplicaMove, exceptionClass, clusterModel,
                  deadBrokers, expectedToOptimize, expectedToGenerateProposals, null);
  }

  private static Object[] params(int tid,
      Class<? extends Goal> goalClass,
      Set<Integer> excludedBrokersForReplicaMove,
      Class<? extends Throwable> exceptionClass,
      ClusterModel clusterModel,
      Collection<Integer> deadBrokers,
      Boolean expectedToOptimize,
      Boolean expectedToGenerateProposals,
      Properties configOverrides) throws Exception {
    deadBrokers.forEach(id -> clusterModel.setBrokerState(id, Broker.State.DEAD));
    Goal goal = configOverrides == null ? goal(goalClass) : goal(goalClass, configOverrides);
    return new Object[]{tid, goal, excludedBrokersForReplicaMove, exceptionClass, clusterModel, expectedToOptimize, expectedToGenerateProposals};
  }
}
