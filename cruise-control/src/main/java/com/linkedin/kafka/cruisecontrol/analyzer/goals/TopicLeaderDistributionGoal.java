/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
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
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils.EPSILON;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionAbstractGoal.ChangeType.ADD;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionAbstractGoal.ChangeType.REMOVE;


/**
 * Soft goal to balance the number of leader replicas of each topic.
 * <ul>
 * <li>Under: (the average number of topic leader replicas per broker) * (1 + topic leader replica count balance percentage)</li>
 * <li>Above: (the average number of topic leader replicas per broker) * Math.max(0, 1 - topic leader replica count balance percentage)</li>
 * </ul>
 * Also see: {@link com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#TOPIC_LEADER_COUNT_BALANCE_THRESHOLD_CONFIG},
 * {@link com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG},
 * and {@link #balancePercentageWithMargin(OptimizationOptions)}.
 */
public class TopicLeaderDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(TopicLeaderDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  // Flag to indicate whether the self healing failed to relocate all offline replicas away from dead brokers or broken
  // disks in its initial attempt and currently omitting the replica balance limit to relocate remaining replicas.
  private boolean _fixOfflineReplicasOnly;

  private final Map<String, Set<Integer>> _brokerIdsAboveBalanceUpperLimitByTopic;
  private final Map<String, Set<Integer>> _brokerIdsUnderBalanceLowerLimitByTopic;
  // Must contain only the topics to be rebalanced.
  private final Map<String, Double> _avgTopicReplicasOnAliveBroker;
  // Must contain all topics to ensure that the lower priority goals work w/o an NPE.
  private final Map<String, Integer> _balanceUpperLimitByTopic;
  private final Map<String, Integer> _balanceLowerLimitByTopic;

  /**
   * A soft goal to balance collocations of leader replicas of the same topic.
   */
  public TopicLeaderDistributionGoal() {
    _brokerIdsAboveBalanceUpperLimitByTopic = new HashMap<>();
    _brokerIdsUnderBalanceLowerLimitByTopic = new HashMap<>();
    _avgTopicReplicasOnAliveBroker = new HashMap<>();
    _balanceUpperLimitByTopic = new HashMap<>();
    _balanceLowerLimitByTopic = new HashMap<>();
  }

  public TopicLeaderDistributionGoal(BalancingConstraint balancingConstraint) {
    this();
    _balancingConstraint = balancingConstraint;
  }

  /**
   * To avoid churns, we add a balance margin to the user specified rebalance threshold. e.g. when user sets the
   * threshold to be {@link BalancingConstraint#topicLeaderBalancePercentage()}, we use
   * ({@link BalancingConstraint#topicLeaderBalancePercentage()}-1)*{@link #BALANCE_MARGIN} instead.
   *
   * @param optimizationOptions Options to adjust balance percentage with margin in case goal optimization is triggered
   * by goal violation detector.
   * @return The rebalance threshold with a margin.
   */
  private double balancePercentageWithMargin(OptimizationOptions optimizationOptions) {
    double balancePercentage = optimizationOptions.isTriggeredByGoalViolation()
                               ? _balancingConstraint.topicLeaderBalancePercentage()
                                 * _balancingConstraint.goalViolationDistributionThresholdMultiplier()
                               : _balancingConstraint.topicLeaderBalancePercentage();

    return (balancePercentage - 1) * BALANCE_MARGIN;
  }

  /**
   * @param topic Topic for which the upper limit is requested.
   * @param optimizationOptions Options to adjust balance upper limit in case goal optimization is triggered by goal
   * violation detector.
   * @return The topic replica balance upper threshold in number of topic replicas.
   */
  private int balanceUpperLimit(String topic, OptimizationOptions optimizationOptions) {
    return (int) Math.ceil(_avgTopicReplicasOnAliveBroker.get(topic)
                           * (1 + balancePercentageWithMargin(optimizationOptions)));
  }

  /**
   * @param topic Topic for which the lower limit is requested.
   * @param optimizationOptions Options to adjust balance lower limit in case goal optimization is triggered by goal
   * violation detector.
   * @return The replica balance lower threshold in number of topic replicas.
   */
  private int balanceLowerLimit(String topic, OptimizationOptions optimizationOptions) {
    return (int) Math.floor(_avgTopicReplicasOnAliveBroker.get(topic)
                            * Math.max(0, (1 - balancePercentageWithMargin(optimizationOptions))));
  }

  /**
   * Check whether the given action is acceptable by this goal. An action is acceptable if the number of topic leader replicas at
   * (1) the source broker does not go under the allowed limit.
   * (2) the destination broker does not go over the allowed limit.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    Replica sourceReplica = sourceBroker.replica(action.topicPartition());
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    String sourceTopic = action.topic();

    boolean accept = true;

    switch (action.balancingAction()) {
      case INTER_BROKER_REPLICA_SWAP:
        String destinationTopic = action.destinationTopic();
        Replica destinationReplica = destinationBroker.replica(action.destinationTopicPartition());
        if (sourceTopic.equals(destinationTopic) && sourceReplica.isLeader() == destinationReplica.isLeader()) {
          break;
        }
        if (sourceReplica.isLeader()) {
          accept &= isLeaderMovementSatisfiable(sourceTopic, sourceBroker, destinationBroker);
        }
        if (destinationReplica.isLeader()) {
          accept &= isLeaderMovementSatisfiable(destinationTopic, destinationBroker, sourceBroker);
        }
        break;
      case INTER_BROKER_REPLICA_MOVEMENT:
        if (sourceReplica.isLeader()) {
          accept = isLeaderMovementSatisfiable(sourceTopic, sourceBroker, destinationBroker);
        }
        break;
      case LEADERSHIP_MOVEMENT:
        accept = isLeaderMovementSatisfiable(sourceTopic, sourceBroker, destinationBroker);
        break;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }

    return accept ? ACCEPT : REPLICA_REJECT;
  }

  private boolean isLeaderMovementSatisfiable(String topic, Broker sourceBroker, Broker destinationBroker) {
    return isReplicaCountUnderBalanceUpperLimitAfterChange(topic, destinationBroker, ADD)
            && isReplicaCountAboveBalanceLowerLimitAfterChange(topic, sourceBroker, REMOVE);
  }

  private boolean isReplicaCountUnderBalanceUpperLimitAfterChange(String topic,
                                                                  Broker broker,
                                                                  ReplicaDistributionGoal.ChangeType changeType) {
    int numTopicReplicas = broker.numLeadersOfTopicInBroker(topic);
    int brokerBalanceUpperLimit = broker.isAlive() ? _balanceUpperLimitByTopic.get(topic) : 0;

    return changeType == ADD ? numTopicReplicas + 1 <= brokerBalanceUpperLimit : numTopicReplicas - 1 <= brokerBalanceUpperLimit;
  }

  private boolean isReplicaCountAboveBalanceLowerLimitAfterChange(String topic,
                                                                  Broker broker,
                                                                  ReplicaDistributionGoal.ChangeType changeType) {
    int numTopicReplicas = broker.numLeadersOfTopicInBroker(topic);
    int brokerBalanceLowerLimit = broker.isAlive() ? _balanceLowerLimitByTopic.get(topic) : 0;

    return changeType == ADD ? numTopicReplicas + 1 >= brokerBalanceLowerLimit : numTopicReplicas - 1 >= brokerBalanceLowerLimit;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new TopicLeaderDistrGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
  }

  /**
   * Get the name of this goal. Name of a goal provides an identification for the goal in human readable format.
   */
  @Override
  public String name() {
    return TopicLeaderDistributionGoal.class.getSimpleName();
  }

  @Override
  public boolean isHardGoal() {
    return false;
  }

  /**
   * Get brokers that the rebalance process will go over to apply balancing actions to replicas they contain.
   *
   * @param clusterModel The state of the cluster.
   * @return A collection of brokers that the rebalance process will go over to apply balancing actions to replicas
   * they contain.
   */
  @Override
  protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
    return clusterModel.brokers();
  }

  /**
   * Get the set of topics to rebalance. If there are self healing eligible replicas, gets only their topics.
   * Otherwise gets all topics except excludedTopics.
   *
   * @return The set of topics to rebalance.
   */
  private Set<String> topicsToRebalance(ClusterModel clusterModel, Set<String> excludedTopics) {
    Set<String> topicsToRebalance;
    if (!clusterModel.selfHealingEligibleReplicas().isEmpty()) {
      topicsToRebalance = new HashSet<>();
      for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
        topicsToRebalance.add(replica.topicPartition().topic());
      }
    } else {
      topicsToRebalance = new HashSet<>(clusterModel.topics());
      topicsToRebalance.removeAll(excludedTopics);
    }

    if (topicsToRebalance.isEmpty()) {
      LOG.warn("All topics are excluded from {}.", name());
    }

    return topicsToRebalance;
  }

  /**
   * Initiates this goal.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    Set<String> topicsToRebalance = topicsToRebalance(clusterModel, excludedTopics);

    // Initialize the average leader replicas on an alive broker.
    for (String topic : clusterModel.topics()) {
      int numTopicLeaders = clusterModel.numTopicLeaders(topic);
      _avgTopicReplicasOnAliveBroker.put(topic, (numTopicLeaders / (double) clusterModel.aliveBrokers().size()));
      _balanceUpperLimitByTopic.put(topic, balanceUpperLimit(topic, optimizationOptions));
      _balanceLowerLimitByTopic.put(topic, balanceLowerLimit(topic, optimizationOptions));
      // Retain only the topics to rebalance in _avgTopicReplicasOnAliveBroker
      if (!topicsToRebalance.contains(topic)) {
        _avgTopicReplicasOnAliveBroker.remove(topic);
      }
    }
    _fixOfflineReplicasOnly = false;
  }

  /**
   * Check if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model. Assumed to be
   * of type {@link ActionType#INTER_BROKER_REPLICA_MOVEMENT} or {@link ActionType#INTER_BROKER_REPLICA_SWAP}.
   * @return True if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    if (action.balancingAction() == ActionType.INTER_BROKER_REPLICA_SWAP) {
      return true;
    }

    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    // The action must be executed if currently fixing offline replicas only and the offline source replica is proposed
    // to be moved to another broker.
    if (_fixOfflineReplicasOnly && sourceBroker.replica(action.topicPartition()).isCurrentOffline()) {
      return action.balancingAction() == ActionType.INTER_BROKER_REPLICA_MOVEMENT;
    }

    //Check that destination and source would not become unbalanced.
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    String sourceTopic = action.topic();

    return isReplicaCountUnderBalanceUpperLimitAfterChange(sourceTopic, destinationBroker, ADD) &&
           isReplicaCountAboveBalanceLowerLimitAfterChange(sourceTopic, sourceBroker, REMOVE);
  }

  /**
   * Update goal state after one round of self-healing / rebalance.
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    if (!_brokerIdsAboveBalanceUpperLimitByTopic.isEmpty()) {
      _brokerIdsAboveBalanceUpperLimitByTopic.clear();
      _succeeded = false;
    }
    if (!_brokerIdsUnderBalanceLowerLimitByTopic.isEmpty()) {
      _brokerIdsUnderBalanceLowerLimitByTopic.clear();
      _succeeded = false;
    }
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    try {
      GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    } catch (OptimizationFailureException ofe) {
      if (_fixOfflineReplicasOnly) {
        throw ofe;
      }
      _fixOfflineReplicasOnly = true;
      LOG.info("Ignoring topic replica balance limit to move replicas from dead brokers/disks.");
      return;
    }
    // Sanity check: No replica should be moved to a broker, which used to host any replica of the same partition on its broken disk.
    GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
    finish();
  }

  @Override
  public void finish() {
    _finished = true;
  }

  private static boolean skipBrokerRebalance(Broker broker,
                                             ClusterModel clusterModel,
                                             Collection<Replica> replicas,
                                             boolean requireLessReplicas,
                                             boolean requireMoreReplicas,
                                             boolean hasOfflineTopicReplicas,
                                             boolean moveImmigrantReplicaOnly) {
    boolean hasImmigrantTopicReplicas = replicas.stream().anyMatch(replica -> broker.immigrantReplicas().contains(replica));
    if (broker.isAlive() && !requireMoreReplicas && !requireLessReplicas) {
      LOG.trace("Skip rebalance: Broker {} is already within the limit for replicas {}.", broker, replicas);
      return true;
    } else if (!clusterModel.newBrokers().isEmpty() && !broker.isNew() && !requireLessReplicas) {
      LOG.trace("Skip rebalance: Cluster has new brokers and this broker {} is not new, but does not require less load "
                + "for replicas {}. Hence, it does not have any offline replicas.", broker, replicas);
      return true;
    } else if (!clusterModel.selfHealingEligibleReplicas().isEmpty() && requireLessReplicas
               && !hasOfflineTopicReplicas && !hasImmigrantTopicReplicas) {
      LOG.trace("Skip rebalance: Cluster is in self-healing mode and the broker {} requires less load, but none of its "
                + "current offline or immigrant replicas are from the topic being balanced {}.", broker, replicas);
      return true;
    } else if (moveImmigrantReplicaOnly && requireLessReplicas && !hasImmigrantTopicReplicas) {
      LOG.trace("Skip rebalance: Only immigrant replicas can be moved, but none of broker {}'s "
                + "current immigrant replicas are from the topic being balanced {}.", broker, replicas);
      return true;
    }

    return false;
  }

  private static Set<Replica> retainCurrentOfflineBrokerReplicas(Broker broker, Collection<Replica> replicas) {
    Set<Replica> offlineReplicas = new HashSet<>(replicas);
    offlineReplicas.retainAll(broker.currentOfflineReplicas());

    return offlineReplicas;
  }

  private boolean isTopicExcludedFromRebalance(String topic) {
    return _avgTopicReplicasOnAliveBroker.get(topic) == null;
  }

  /**
   * Rebalance the given broker without violating the constraints of the current goal and optimized goals.
   *
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) {
    LOG.debug("Rebalancing broker {} [limits] lower: {} upper: {}.", broker.id(), _balanceLowerLimitByTopic,
              _balanceUpperLimitByTopic);

    for (String topic : broker.topics()) {
      if (isTopicExcludedFromRebalance(topic)) {
        continue;
      }

      Collection<Replica> leaderReplicas = broker.leadersOfTopicInBroker(topic);
      int numLeaderReplicas = leaderReplicas.size();
      int numOfflineTopicReplicas = retainCurrentOfflineBrokerReplicas(broker, leaderReplicas).size();

      boolean requireLessReplicas = numOfflineTopicReplicas > 0 || numLeaderReplicas > _balanceUpperLimitByTopic.get(topic);
      boolean requireMoreReplicas = broker.isAlive() && numLeaderReplicas - numOfflineTopicReplicas < _balanceLowerLimitByTopic.get(topic);

      if (skipBrokerRebalance(broker, clusterModel, leaderReplicas, requireLessReplicas, requireMoreReplicas, numOfflineTopicReplicas > 0,
                              optimizationOptions.onlyMoveImmigrantReplicas())) {
        continue;
      }

      // Update broker ids over the balance limit for logging purposes.
      if (requireLessReplicas && rebalanceByMovingLeadershipOut(broker, topic, clusterModel, optimizedGoals, optimizationOptions)) {
        _brokerIdsAboveBalanceUpperLimitByTopic.computeIfAbsent(topic, t -> new HashSet<>()).add(broker.id());
        LOG.debug("Failed to sufficiently decrease leaders of topic {} in broker {} with leadership movements. Leaders: {}.",
                  topic, broker.id(), broker.numLeadersOfTopicInBroker(topic));
      }
      if (requireMoreReplicas && rebalanceByMovingLeadershipIn(broker, topic, clusterModel, optimizedGoals, optimizationOptions)) {
        _brokerIdsUnderBalanceLowerLimitByTopic.computeIfAbsent(topic, t -> new HashSet<>()).add(broker.id());
        LOG.debug("Failed to sufficiently increase leaders of topic {} in broker {} with leadership movements. Leaders: {}.",
                  topic, broker.id(), broker.numLeadersOfTopicInBroker(topic));
      }
      if (!_brokerIdsAboveBalanceUpperLimitByTopic.getOrDefault(topic, Collections.emptySet()).contains(broker.id())
          && !_brokerIdsUnderBalanceLowerLimitByTopic.getOrDefault(topic, Collections.emptySet()).contains(broker.id())) {
        LOG.debug("Successfully balanced leaders of topic {} in broker {} by moving leadership. Leaders: {}",
                  topic, broker.id(), broker.numLeadersOfTopicInBroker(topic));
      }
    }
  }

  /**
   * Attempt to decrease the number of topic partitions led by the broker first by moving the leadership to follower
   * replicas, and second by swapping leader replicas with follower replicas of partitions not hosted by the broker
   * currently.
   *
   * @param broker              Broker that leads too many partitions of the given topic.
   * @param topic               Topic to rebalance.
   * @param clusterModel        The state of the cluster.
   * @param optimizedGoals      Optimized goals.
   * @param optimizationOptions Options to take into account during optimization.
   * @return true if rebalancing was not successful.
   */
  private boolean rebalanceByMovingLeadershipOut(Broker broker,
                                                 String topic,
                                                 ClusterModel clusterModel,
                                                 Set<Goal> optimizedGoals,
                                                 OptimizationOptions optimizationOptions) {
    if (!clusterModel.deadBrokers().isEmpty()) {
      return true;
    }

    // Try to rebalance by moving leadership to follower replicas.
    for (Replica leader : broker.leadersOfTopicInBroker(topic)) {
      final Set<Broker> candidateBrokers = clusterModel.partition(leader.topicPartition()).partitionBrokers().stream()
              .filter(b -> b != broker && !b.replica(leader.topicPartition()).isCurrentOffline())
              .filter(b -> b.numLeadersOfTopicInBroker(topic) < _balanceUpperLimitByTopic.get(topic))
              .collect(Collectors.toSet());
      Broker b = maybeApplyBalancingAction(clusterModel,
              leader,
              candidateBrokers,
              ActionType.LEADERSHIP_MOVEMENT,
              optimizedGoals,
              optimizationOptions);
      if (b != null && broker.numLeadersOfTopicInBroker(topic) <= _balanceUpperLimitByTopic.get(topic)) {
        return false;
      }
    }

    // Try to rebalance by swapping one of the broker follower replicas with a leader replica from another broker.
    Set<TopicPartition> brokerPartitions = broker.replicasOfTopicInBroker(topic).stream()
            .map(Replica::topicPartition)
            .collect(Collectors.toSet());;
    for (Replica leader : broker.leadersOfTopicInBroker(topic)) {
      Iterable<Broker> candidateBrokers = clusterModel.brokers().stream()
              .filter(b -> b.numLeadersOfTopicInBroker(topic) < _balanceUpperLimitByTopic.get(topic))
              .collect(Collectors.toList());
      for (Broker candidateBroker : candidateBrokers) {
        List<Replica> candidateReplicas = candidateBroker.replicasOfTopicInBroker(topic).stream()
                .filter(r -> !r.isLeader() && !r.isCurrentOffline() && !brokerPartitions.contains(r.topicPartition()))
                .collect(Collectors.toList());
        Replica r = maybeApplySwapAction(clusterModel, leader, new TreeSet<>(candidateReplicas), optimizedGoals, optimizationOptions);
        if (r != null && broker.numLeadersOfTopicInBroker(topic) <= _balanceUpperLimitByTopic.get(topic)) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Attempt to increase the number of topic partitions led by the broker first by moving the leadership to follower
   * replicas hosted by the broker, and second by swapping follower replicas hosted by the broker with leader replicas
   * of partitions not currently hosted by the broker.
   *
   * @param broker              Broker that leads too few partitions of the given topic.
   * @param topic               Topic to rebalance.
   * @param clusterModel        The state of the cluster.
   * @param optimizedGoals      Optimized goals.
   * @param optimizationOptions Options to take into account during optimization.
   * @return true if rebalancing was not successful.
   */
  private boolean rebalanceByMovingLeadershipIn(Broker broker,
                                                String topic,
                                                ClusterModel clusterModel,
                                                Set<Goal> optimizedGoals,
                                                OptimizationOptions optimizationOptions) {
    if (!clusterModel.deadBrokers().isEmpty() ||
            optimizationOptions.excludedBrokersForLeadership().contains(broker.id())) {
      return true;
    }

    // Try to rebalance by making one of the replicas the broker is already hosting a leader.
    Set<Broker> candidateBrokers = Collections.singleton(broker);
    for (Replica replica : broker.replicasOfTopicInBroker(topic)) {
      if (replica.isLeader() || replica.isCurrentOffline()) {
        continue;
      }
      Broker b = maybeApplyBalancingAction(clusterModel,
              clusterModel.partition(replica.topicPartition()).leader(),
              candidateBrokers,
              ActionType.LEADERSHIP_MOVEMENT,
              optimizedGoals,
              optimizationOptions);
      if (b != null && broker.numLeadersOfTopicInBroker(topic) >= _balanceLowerLimitByTopic.get(topic)) {
        return false;
      }
    }

    // Try to rebalance by swapping one of the broker non-leader replicas with a leader replica from another broker.
    Set<TopicPartition> brokerPartitions = broker.replicasOfTopicInBroker(topic).stream()
            .map(Replica::topicPartition)
            .collect(Collectors.toSet());
    candidateBrokers = clusterModel.brokers().stream()
            .filter(b -> b.numLeadersOfTopicInBroker(topic) > _balanceLowerLimitByTopic.get(topic))
            .collect(Collectors.toSet());
    for (Broker candidateBroker : candidateBrokers) {
      Iterable<Replica> leaders = candidateBroker.leaderReplicas().stream()
              .filter(r -> r.topicPartition().topic().equals(topic) && !r.isCurrentOffline())
              .filter(r -> !brokerPartitions.contains(r.topicPartition()))
              .collect(Collectors.toList());
      for (Replica leader : leaders) {
        List<Replica> candidateReplicas = broker.replicasOfTopicInBroker(topic).stream()
                .filter(r -> !r.isLeader())
                .collect(Collectors.toList());
        Replica r = maybeApplySwapAction(clusterModel, leader, new TreeSet<>(candidateReplicas), optimizedGoals, optimizationOptions);
        if (r != null && broker.numLeadersOfTopicInBroker(topic) >= _balanceLowerLimitByTopic.get(topic)) {
          return false;
        }
      }
    }

    return true;
  }

  private class TopicLeaderDistrGoalStatsComparator implements ClusterModelStatsComparator {
    private String _reasonForLastNegativeResult;

    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // Standard deviation of number of topic leader replicas over brokers in the current must be less than the
      // pre-optimized stats.
      double stdDev1 = stats1.topicLeaderStats().get(Statistic.ST_DEV).doubleValue();
      double stdDev2 = stats2.topicLeaderStats().get(Statistic.ST_DEV).doubleValue();
      int result = AnalyzerUtils.compare(stdDev2, stdDev1, EPSILON);
      if (result < 0) {
        _reasonForLastNegativeResult = String.format("Violated %s. [Std Deviation of Topic Leader Replica Distribution] post-"
                                                     + "optimization:%.3f pre-optimization:%.3f", name(), stdDev1, stdDev2);
      }
      return result;
    }

    @Override
    public String explainLastComparison() {
      return _reasonForLastNegativeResult;
    }
  }
}
