/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner;

    import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
    import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
    import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
    import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
    import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
    import com.linkedin.kafka.cruisecontrol.model.Broker;
    import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

    import java.util.ArrayList;
    import java.util.Collection;
    import java.util.Collections;
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

    import static org.junit.Assert.*;


/**
 * Unit test for testing goals with excluded topics under fixed cluster properties.
 */
@RunWith(Parameterized.class)
public class KafkaAssignerExcludedTopicsTest {

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Parameters(name = "{1}-{0}")
  public static Collection<Object[]> data() throws Exception {
    Collection<Object[]> p = new ArrayList<>();

    Set<String> noExclusion = Collections.emptySet();
    Set<String> excludeT1 = Collections.unmodifiableSet(Collections.singleton("T1"));
    Set<Integer> noDeadBroker = Collections.emptySet();
    Set<Integer> deadBroker0 = Collections.unmodifiableSet(Collections.singleton(0));

    int testId = 0;
    // With excluded topics, rack aware satisfiable cluster, no dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(testId++, KafkaAssignerEvenRackAwareGoal.class, excludeT1, null,
                 DeterministicCluster.rackAwareSatisfiable(), noDeadBroker, true));
    // With excluded topics, rack aware satisfiable cluster, one dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(testId++, KafkaAssignerEvenRackAwareGoal.class, excludeT1, null,
                 DeterministicCluster.rackAwareSatisfiable(), deadBroker0, true));
    // Without excluded topics, rack aware satisfiable cluster, no dead brokers (No exception, Proposal expected, Expected to look optimized)
    p.add(params(testId++, KafkaAssignerEvenRackAwareGoal.class, noExclusion, null,
                 DeterministicCluster.rackAwareSatisfiable(), noDeadBroker, true));
    // Without excluded topics, rack aware satisfiable cluster, one dead broker (No exception, Proposal expected, Expected to look optimized)
    p.add(params(testId++, KafkaAssignerEvenRackAwareGoal.class, noExclusion, null,
                 DeterministicCluster.rackAwareSatisfiable(), deadBroker0, true));
    // With excluded topics, rack aware unsatisfiable cluster, no dead broker (No exception, No proposal, Expected to look optimized)
    p.add(params(testId++, KafkaAssignerEvenRackAwareGoal.class, excludeT1, null,
                 DeterministicCluster.rackAwareUnsatisfiable(), noDeadBroker, true));
    // With excluded topics, rack aware unsatisfiable cluster, one dead broker (Exception)
    p.add(params(testId++, KafkaAssignerEvenRackAwareGoal.class, excludeT1, OptimizationFailureException.class,
                 DeterministicCluster.rackAwareUnsatisfiable(), deadBroker0, null));
    // Test: Without excluded topics, rack aware unsatisfiable cluster, no dead brokers (Exception expected)
    p.add(params(testId++, KafkaAssignerEvenRackAwareGoal.class, noExclusion, OptimizationFailureException.class,
                 DeterministicCluster.rackAwareUnsatisfiable(), noDeadBroker, null));
    // Test: Without excluded topics, rack aware unsatisfiable cluster, one dead broker (Exception expected)
    p.add(params(testId++, KafkaAssignerEvenRackAwareGoal.class, noExclusion, OptimizationFailureException.class,
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
   * Constructor of Kafka Assigner Excluded Topics Test.
   *
   * @param goal Goal to be tested.
   * @param excludedTopics Topics to be excluded from the goal.
   * @param exceptionClass Expected exception class (if any).
   * @param clusterModel Cluster model to be used for the test.
   * @param expectedToOptimize The expectation on whether the cluster state will be considered optimized or not.
   */
  public KafkaAssignerExcludedTopicsTest(int testId,
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
      Map<TopicPartition, List<Integer>> initDistribution = _clusterModel.getReplicaDistribution();

      if (_expectedToOptimize) {
        assertTrue("Excluded Topics Test failed to optimize " + _goal.name() + " with excluded topics.",
                   _goal.optimize(_clusterModel, Collections.emptySet(), _excludedTopics));
      } else {
        assertTrue("Excluded Topics Test optimized " + _goal.name() + " with excluded topics " + _excludedTopics,
                   !_goal.optimize(_clusterModel, Collections.emptySet(), _excludedTopics));
      }
      // Generated proposals cannot have the excluded topic.
      if (!_excludedTopics.isEmpty()) {
        Set<BalancingProposal> goalProposals = AnalyzerUtils.getDiff(initDistribution, _clusterModel);

        boolean proposalHasTopic = false;
        for (BalancingProposal proposal : goalProposals) {
          proposalHasTopic = _excludedTopics.contains(proposal.topic())
                             && _clusterModel.broker(proposal.sourceBrokerId()).isAlive();
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

  private static Object[] params(int tid,
                                 Class<? extends Goal> goalClass,
                                 Collection<String> excludedTopics,
                                 Class<? extends Throwable> exceptionClass,
                                 ClusterModel clusterModel,
                                 Collection<Integer> deadBrokers,
                                 Boolean expectedToOptimize) throws Exception {
    deadBrokers.forEach(id -> clusterModel.setBrokerState(id, Broker.State.DEAD));
    return new Object[]{tid, goalClass.newInstance(), excludedTopics, exceptionClass, clusterModel, expectedToOptimize};
  }
}
