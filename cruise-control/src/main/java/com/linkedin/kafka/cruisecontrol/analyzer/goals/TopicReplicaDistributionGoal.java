/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;

import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.REPLICA_SWAP;
import static com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils.EPSILON;


/**
 * Class for achieving the following soft goal:
 * <p>
 * SOFT GOAL#2: Balance collocations of replicas of the same topic.
 */
public class TopicReplicaDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(TopicReplicaDistributionGoal.class);

  private Map<String, ReplicaDistributionTarget> _replicaDistributionTargetByTopic;
  private String _currentRebalanceTopic;
  private List<String> _topicsToRebalance;
  private int _numRebalancedTopics;

  /**
   * Constructor for Topic Replica Distribution Goal. Initially replica distribution target by topic is null.
   */
  public TopicReplicaDistributionGoal() {

  }

  TopicReplicaDistributionGoal(BalancingConstraint balancingConstraint) {
    _balancingConstraint = balancingConstraint;
  }

  /**
   * @deprecated
   * Please use {@link #actionAcceptance(BalancingAction, ClusterModel)} instead.
   */
  @Override
  public boolean isActionAcceptable(BalancingAction action, ClusterModel clusterModel) {
    return actionAcceptance(action, clusterModel) == ACCEPT;
  }

  /**
   * Check whether given action is acceptable by this goal. An action is acceptable if the number of topic replicas
   * at the source broker are more than the number of topic replicas at the destination (remote) broker.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    switch (action.balancingAction()) {
      case REPLICA_SWAP:
        return ACCEPT;
      case LEADERSHIP_MOVEMENT:
      case REPLICA_MOVEMENT:
        String topic = action.topic();
        int numLocalTopicReplicas = clusterModel.broker(action.sourceBrokerId()).replicasOfTopicInBroker(topic).size();
        int numRemoteTopicReplicas = clusterModel.broker(action.destinationBrokerId()).replicasOfTopicInBroker(topic).size();

        return numRemoteTopicReplicas < numLocalTopicReplicas ? ACCEPT : REPLICA_REJECT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new TopicReplicaDistributionGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(1, 0.0, true);
  }

  /**
   * Get the name of this goal. Name of a goal provides an identification for the goal in human readable format.
   */
  @Override
  public String name() {
    return TopicReplicaDistributionGoal.class.getSimpleName();
  }

  /**
   * Get brokers that the rebalance process will go over to apply balancing actions to rep licas they contain.
   *
   * @param clusterModel The state of the cluster.
   * @return A collection of brokers that the rebalance process will go over to apply balancing actions to replicas
   * they contain.
   */
  @Override
  protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
    if (!clusterModel.deadBrokers().isEmpty()) {
      return clusterModel.deadBrokers();
    }

    if (_currentRebalanceTopic == null) {
      return Collections.emptySortedSet();
    }
    // Brokers having over minimum number of replicas per broker for the current rebalance topic are eligible for balancing.
    SortedSet<Broker> brokersToBalance = new TreeSet<>();
    int minNumReplicasPerBroker = _replicaDistributionTargetByTopic.get(_currentRebalanceTopic).minNumReplicasPerBroker();
    brokersToBalance.addAll(clusterModel.brokers().stream()
        .filter(broker -> broker.replicasOfTopicInBroker(_currentRebalanceTopic).size() > minNumReplicasPerBroker)
        .collect(Collectors.toList()));
    return brokersToBalance;
  }

  /**
   * Check if requirements of this goal are not violated if this action is applied to the given cluster state,
   * false otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model.
   * @return True if requirements of this goal are not violated if this action is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    // This method is not used by this goal.
    return false;
  }

  /**
   * Initiates replica distribution target for each non-excluded topic in the given cluster.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposals.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, Set<String> excludedTopics) {
    _numRebalancedTopics = 0;
    _topicsToRebalance = new ArrayList<>(clusterModel.topics());
    if (clusterModel.deadBrokers().isEmpty()) {
      _topicsToRebalance.removeAll(excludedTopics);
    }

    if (_topicsToRebalance.isEmpty()) {
      LOG.warn("All topics are excluded from {}.", name());
      _currentRebalanceTopic = null;
    } else {
      _currentRebalanceTopic = _topicsToRebalance.get(_numRebalancedTopics);
    }

    _replicaDistributionTargetByTopic = new HashMap<>();

    Set<Broker> brokers = clusterModel.healthyBrokers();
    // Populate a map of replica distribution target by each non-excluded topic in the cluster.
    for (String topic : _topicsToRebalance) {
      ReplicaDistributionTarget replicaDistributionTarget =
          new ReplicaDistributionTarget(clusterModel.numTopicReplicas(topic), brokers);
      for (Broker broker : brokers) {
        replicaDistributionTarget.setBrokerEligibilityForReceivingReplica(broker.id(),
            broker.replicasOfTopicInBroker(topic).size());
      }
      _replicaDistributionTargetByTopic.put(topic, replicaDistributionTarget);
    }
  }

  /**
   * Update goal state after one round of self-healing / rebalance.
   *
   * @param clusterModel The state of the cluster.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws OptimizationFailureException {

    if (!clusterModel.selfHealingEligibleReplicas().isEmpty()) {
      // Sanity check: No self-healing eligible replica should remain at a decommissioned broker.
      for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
        if (!replica.broker().isAlive()) {
          throw new OptimizationFailureException(
              "Self healing failed to move the replica away from decommissioned broker.");
        }
      }
      finish();   // Finish self healing.
    } else if (_currentRebalanceTopic == null || ++_numRebalancedTopics == _topicsToRebalance.size()) {
      finish();   // Finish rebalance.
    } else {
      // Set the current topic to rebalance.
      _currentRebalanceTopic = _topicsToRebalance.get(_numRebalancedTopics);
    }
  }

  /**
   * Heal the given cluster without violating the requirements of optimized goals.
   *
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   */
  private void healCluster(ClusterModel clusterModel, Set<Goal> optimizedGoals) throws OptimizationFailureException {
    // Move self healed replicas (if their broker is overloaded or they reside at dead brokers) to eligible ones.
    for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
      String topic = replica.topicPartition().topic();
      ReplicaDistributionTarget replicaDistributionTarget = _replicaDistributionTargetByTopic.get(topic);
      replicaDistributionTarget.moveSelfHealingEligibleReplicaToEligibleBroker(clusterModel, replica,
          replica.broker().replicasOfTopicInBroker(topic).size(), optimizedGoals);
    }
  }

  /**
   * Rebalance the given broker without violating the constraints of the current goal and optimized goals.
   *
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param excludedTopics The topics that should be excluded from the optimization action.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    Set<String> excludedTopics) throws OptimizationFailureException {

    if (!clusterModel.selfHealingEligibleReplicas().isEmpty() && !broker.isAlive() && !broker.replicas().isEmpty()) {
      healCluster(clusterModel, optimizedGoals);
    } else {
      SortedSet<Replica> topicReplicasInBroker = new TreeSet<>(broker.replicasOfTopicInBroker(_currentRebalanceTopic));
      // Move local topic replicas to eligible brokers.
      _replicaDistributionTargetByTopic.get(_currentRebalanceTopic)
                                       .moveReplicasInSourceBrokerToEligibleBrokers(clusterModel,
                                                                                    topicReplicasInBroker,
                                                                                    optimizedGoals,
                                                                                    excludedTopics);
    }
  }

  private class TopicReplicaDistributionGoalStatsComparator implements ClusterModelStatsComparator {
    private String _reasonForLastNegativeResult;

    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // Standard deviation of number of topic replicas over brokers in the current must be less than the
      // pre-optimized stats.
      double stdDev1 = stats1.topicReplicaStats().get(Statistic.ST_DEV).doubleValue();
      double stdDev2 = stats2.topicReplicaStats().get(Statistic.ST_DEV).doubleValue();
      int result = AnalyzerUtils.compare(stdDev2, stdDev1, EPSILON);
      if (result < 0) {
        _reasonForLastNegativeResult =
            String.format("Violated %s. [Standard Deviation of Topic Replica Distribution] " +
                              "post-optimization:%.3f pre-optimization:%.3f",
                          name(), stdDev1, stdDev2);
      }
      return result;
    }

    @Override
    public String explainLastComparison() {
      return _reasonForLastNegativeResult;
    }
  }
}
