/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.common.Utils;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.LEADERSHIP_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.INTER_BROKER_REPLICA_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;


/**
 * HARD GOAL: Generate leadership movement and leader replica movement proposals to ensure that each alive broker that
 * is not excluded for replica leadership moves has at least the minimum number (specified by
 * {@link AnalyzerConfig#MIN_TOPIC_LEADERS_PER_BROKER_CONFIG}) of leader replica of each topic in a configured set of topics
 * (specified by {@link AnalyzerConfig#TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG}).
 */
public class MinTopicLeadersPerBrokerGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(MinTopicLeadersPerBrokerGoal.class);
  private final String _replicaSortName = replicaSortName(this, true, false);
  private Set<String> _mustHaveTopicLeadersPerBroker;

  public MinTopicLeadersPerBrokerGoal() {

  }

  /**
   * Package private for unit test.
   */
  MinTopicLeadersPerBrokerGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new GoalUtils.HardGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
  }

  @Override
  public String name() {
    return MinTopicLeadersPerBrokerGoal.class.getSimpleName();
  }

  @Override
  public boolean isHardGoal() {
    return true;
  }

  /**
   * Check whether given action is acceptable by this goal. An action is acceptable by a goal if it satisfies
   * requirements of the goal. Requirements(hard goal): A configured set of topics that should have at least a minimum number
   * of leader replicas on each alive broker that is not excluded for replica leadership moves
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   *         {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    if (!actionAffectsRelevantTopics(action)) {
      return ACCEPT;
    }
    switch (action.balancingAction()) {
      case LEADERSHIP_MOVEMENT:
      case INTER_BROKER_REPLICA_MOVEMENT:
        Replica replicaToBeRemoved = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
        return doesLeaderRemoveViolateOptimizedGoal(replicaToBeRemoved) ? REPLICA_REJECT : ACCEPT;
      case INTER_BROKER_REPLICA_SWAP:
        Replica srcReplicaToSwap = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
        Replica dstReplicaToSwap = clusterModel.broker(action.destinationBrokerId()).replica(action.destinationTopicPartition());
        return acceptReplicaSwap(srcReplicaToSwap, dstReplicaToSwap);
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  private ActionAcceptance acceptReplicaSwap(Replica srcReplicaToSwap, Replica dstReplicaToSwap) {
    if (!srcReplicaToSwap.isLeader() && !dstReplicaToSwap.isLeader()) {
      // Swapping two follower replicas is always accepted
      return ACCEPT;
    }
    String srcTopicName = srcReplicaToSwap.topicPartition().topic();
    String dstTopicName = dstReplicaToSwap.topicPartition().topic();
    if (srcReplicaToSwap.isLeader() && dstReplicaToSwap.isLeader() && Objects.equals(srcTopicName, dstTopicName)) {
      // Swapping two leader replicas of the same topic is always accepted
      return ACCEPT;
    }
    if (doesLeaderRemoveViolateOptimizedGoal(srcReplicaToSwap) || doesLeaderRemoveViolateOptimizedGoal(dstReplicaToSwap)) {
      return REPLICA_REJECT;
    }
    return ACCEPT;
  }

  /**
   * Check whether this goal is violated after removing the given leader replica. It is violated if there is not be enough
   * leader replicas on that broker for that topic
   *
   * @param replicaToBeRemoved replica to be removed
   * @return {@code true} if the given replica move would violate the optimized goal (i.e. the move is not acceptable),
   * {@code false} otherwise.
   */
  private boolean doesLeaderRemoveViolateOptimizedGoal(Replica replicaToBeRemoved) {
    if (!replicaToBeRemoved.isLeader()) {
      // Moving a follower replica does not violate/affect this goal
      return false;
    }
    int topicLeaderCountOnSourceBroker = replicaToBeRemoved.broker().numLeadersFor(replicaToBeRemoved.topicPartition().topic());
    return topicLeaderCountOnSourceBroker <= minTopicLeadersPerBroker();
  }

  /**
   * This is a hard goal; hence, the proposals are not limited to dead broker replicas in case of self-healing.
   * Sanity Check: The total number of leader replicas of certain topics is sufficient so that these leader replicas
   * can be moved to satisfy this goal.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    _mustHaveTopicLeadersPerBroker = Collections.unmodifiableSet(
        Utils.getTopicNamesMatchedWithPattern(_balancingConstraint.topicsWithMinLeadersPerBrokerPattern(), clusterModel::topics));
    if (_mustHaveTopicLeadersPerBroker.isEmpty()) {
      return;
    }
    // Sanity checks
    validateTopicsWithMinLeaderIsNotExcluded(optimizationOptions);
    validateEnoughLeaderToDistribute(clusterModel, optimizationOptions);
    validateBrokersAllowedReplicaMoveExist(clusterModel, optimizationOptions);
    boolean onlyMoveImmigrantReplicas = optimizationOptions.onlyMoveImmigrantReplicas();
    new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(), onlyMoveImmigrantReplicas)
                              .addSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnIncludedTopics(_mustHaveTopicLeadersPerBroker))
                              .maybeAddPriorityFunc(ReplicaSortFunctionFactory.prioritizeImmigrants(), !onlyMoveImmigrantReplicas)
                              .trackSortedReplicasFor(_replicaSortName, clusterModel);
  }

  private int minTopicLeadersPerBroker() {
    return _balancingConstraint.minTopicLeadersPerBroker();
  }

  private void validateTopicsWithMinLeaderIsNotExcluded(OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    if (optimizationOptions.excludedTopics().isEmpty()) {
      return;
    }
    Set<String> shouldNotBeExcludedTopics = new HashSet<>();
    _mustHaveTopicLeadersPerBroker.forEach(topicName -> {
      if (optimizationOptions.excludedTopics().contains(topicName)) {
        shouldNotBeExcludedTopics.add(topicName);
      }
    });
    if (!shouldNotBeExcludedTopics.isEmpty()) {
      throw new OptimizationFailureException(String.format("[%s] Topics that must have a minimum number of leaders per broker cannot be excluded."
                                                           + " This error implies a config error. Topics should not be excluded=[%s] (see %s).",
                                                           name(), String.join(", ", shouldNotBeExcludedTopics),
                                                           AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG));
    }
  }

  private void validateEnoughLeaderToDistribute(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    Map<String, Integer> numLeadersByTopicNames = clusterModel.numLeadersPerTopic(_mustHaveTopicLeadersPerBroker);
    Set<Broker> eligibleBrokersForLeadership = eligibleBrokersForLeadership(clusterModel, optimizationOptions);
    int totalMinimumLeaderCount = eligibleBrokersForLeadership.size() * minTopicLeadersPerBroker();

    for (Map.Entry<String, Integer> numLeadersPerTopic : numLeadersByTopicNames.entrySet()) {
      if (numLeadersPerTopic.getValue() < totalMinimumLeaderCount) {
        ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
            .numPartitions(totalMinimumLeaderCount).topic(numLeadersPerTopic.getKey()).build();
        throw new OptimizationFailureException(
            String.format("[%s] Cannot distribute %d leaders over %d broker(s) with minimum required per broker leader count %d for topic %s.",
                          name(), numLeadersPerTopic.getValue(), eligibleBrokersForLeadership.size(), minTopicLeadersPerBroker(),
                          numLeadersPerTopic.getKey()), recommendation);
      }
    }
  }

  private void validateBrokersAllowedReplicaMoveExist(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    Set<Integer> brokersAllowedReplicaMove = GoalUtils.aliveBrokersNotExcludedForReplicaMove(clusterModel, optimizationOptions);
    if (brokersAllowedReplicaMove.isEmpty()) {
      // Handle the case when all alive brokers are excluded from replica moves.
      ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
          .numBrokers(clusterModel.maxReplicationFactor()).build();
      throw new OptimizationFailureException(String.format("[%s] All alive brokers are excluded from replica moves.", name()), recommendation);
    }
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
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    Replica replicaToBeMoved = sourceBroker.replica(action.topicPartition());

    if (replicaToBeMoved.broker().replica(action.topicPartition()).isCurrentOffline()) {
      return action.balancingAction() == ActionType.INTER_BROKER_REPLICA_MOVEMENT;
    }
    // Moving leader replica from more abundant broker is considered as self-satisfied
    return sourceBroker.numLeadersFor(replicaToBeMoved.topicPartition().topic()) > minTopicLeadersPerBroker();
  }

  private boolean actionAffectsRelevantTopics(BalancingAction action) {
    if (_mustHaveTopicLeadersPerBroker.contains(action.topic())) {
      return true;
    }
    return action.balancingAction() == ActionType.INTER_BROKER_REPLICA_SWAP &&
           _mustHaveTopicLeadersPerBroker.contains(action.destinationTopic());
  }

  /**
   * Update goal state after one round of self-healing / rebalance.
   *
   *  @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    // Sanity check: No replica should be moved to a broker, which used to host any replica of the same partition on its broken disk.
    GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
    ensureBrokersAllHaveEnoughLeaderOfTopics(clusterModel, optimizationOptions);
    finish();
  }

  private void ensureBrokersAllHaveEnoughLeaderOfTopics(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    if (_mustHaveTopicLeadersPerBroker.isEmpty()) {
      return; // Early termination to avoid some unnecessary computation
    }
    for (Broker broker : clusterModel.aliveBrokers()) {
      if (!isEligibleToHaveLeaders(broker, optimizationOptions)) {
        continue;
      }
      for (String mustHaveLeaderPerBrokerTopicName : _mustHaveTopicLeadersPerBroker) {
        int leaderCount = broker.numLeadersFor(mustHaveLeaderPerBrokerTopicName);
        if (leaderCount < minTopicLeadersPerBroker()) {
          throw new OptimizationFailureException(String.format("[%s] Broker %d has insufficient per-broker leaders for topic %s (required: %d "
                                                               + "current: %d).", name(), broker.id(), mustHaveLeaderPerBrokerTopicName,
                                                               minTopicLeadersPerBroker(), leaderCount));
        }
      }
    }
  }

  /**
   * Rebalance the given broker without violating the constraints of the current goal and optimized goals.
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    LOG.debug("balancing broker {}, optimized goals = {}", broker, optimizedGoals);
    moveAwayOfflineReplicas(broker, clusterModel, optimizedGoals, optimizationOptions);
    if (_mustHaveTopicLeadersPerBroker.isEmpty()) {
      return; // Early termination to avoid some unnecessary computation
    }
    if (!(broker.isAlive() && isEligibleToHaveLeaders(broker, optimizationOptions))) {
      return;
    }
    for (String topicMustHaveLeaderPerBroker : _mustHaveTopicLeadersPerBroker) {
      maybeMoveLeaderOfTopicToBroker(topicMustHaveLeaderPerBroker, broker, clusterModel, optimizedGoals, optimizationOptions);
    }
  }

  private void maybeMoveLeaderOfTopicToBroker(String topicMustHaveLeaderPerBroker,
                                              Broker broker,
                                              ClusterModel clusterModel,
                                              Set<Goal> optimizedGoals,
                                              OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    int topicLeaderCountOnReceiverBroker = broker.numLeadersFor(topicMustHaveLeaderPerBroker);
    if (topicLeaderCountOnReceiverBroker >= minTopicLeadersPerBroker()) {
      return; // This broker has enough leader replica(s) for the given topic
    }
    // Try to elect follower replica(s) of the interested topic on this broker to be leader
    List<Replica> followerReplicas = broker.trackedSortedReplicas(_replicaSortName)
                                          .sortedReplicas(false).stream().filter(
                                              replica -> !replica.isLeader()
                                                         && replica.topicPartition().topic().equals(topicMustHaveLeaderPerBroker))
                                          .collect(Collectors.toList());

    for (Replica followerReplica : followerReplicas) {
      Replica leader = clusterModel.partition(followerReplica.topicPartition()).leader();
      if (leader.broker().numLeadersFor(topicMustHaveLeaderPerBroker) > minTopicLeadersPerBroker()) {
        if (maybeApplyBalancingAction(clusterModel, leader, Collections.singleton(broker),
                                      LEADERSHIP_MOVEMENT, optimizedGoals, optimizationOptions) != null) {
          topicLeaderCountOnReceiverBroker++;
          if (topicLeaderCountOnReceiverBroker >= minTopicLeadersPerBroker()) {
            return; // This broker satisfies this goal for the given topic
          }
        }
      }
    }

    // Try to move leader replica(s) of the interested topic from other brokers to this broker
    PriorityQueue<Broker> brokersWithExcessiveLeaderToMove =
        getBrokersWithExcessiveLeaderToMove(topicMustHaveLeaderPerBroker, clusterModel, broker);

    while (!brokersWithExcessiveLeaderToMove.isEmpty()) {
      Broker brokerWithExcessiveLeaderToMove = brokersWithExcessiveLeaderToMove.poll();
      List<Replica> leadersOfTopic = brokerWithExcessiveLeaderToMove.trackedSortedReplicas(_replicaSortName)
                                                                                  .sortedReplicas(false).stream()
                                                                                  .filter(replica ->
                                                                                              replica.isLeader()
                                                                                              && replica.topicPartition()
                                                                                                        .topic()
                                                                                                        .equals(topicMustHaveLeaderPerBroker))
                                                                                  .collect(Collectors.toList());
      boolean leaderMoved = false;
      int topicLeaderCountOnGiverBroker = leadersOfTopic.size();
      for (Replica leaderOfTopic : leadersOfTopic) {
        Broker destinationBroker = maybeApplyBalancingAction(clusterModel, leaderOfTopic, Collections.singleton(broker),
                                                             INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions);
        if (destinationBroker != null) {
          leaderMoved = true;
          break; // Successfully move one leader replica
        }
      }
      if (leaderMoved) {
        topicLeaderCountOnReceiverBroker++;
        if (topicLeaderCountOnReceiverBroker >= minTopicLeadersPerBroker()) {
          return; // This broker satisfies this goal for the given topic
        }
        topicLeaderCountOnGiverBroker--;
        if (topicLeaderCountOnGiverBroker > minTopicLeadersPerBroker()) {
          // Still have excessive topic leader to give
          brokersWithExcessiveLeaderToMove.add(brokerWithExcessiveLeaderToMove);
        }
      }
    }
    throw new OptimizationFailureException(String.format("[%s] Cannot make broker %d have at least %d leaders from topic %s.",
                                                         name(), broker.id(), minTopicLeadersPerBroker(), topicMustHaveLeaderPerBroker));
  }

  /**
   * This method creates a priority queue which has the broker with the most number of leader replicas of a given topic at the top of queue
   * @param topicName name of the given topic
   * @param clusterModel cluster model
   * @param originalBroker original broker which needs to be excluded from the priority queue
   * @return a priority queue which has the broker with the most number of leader replicas of a given topic at the top of queue
   */
  private PriorityQueue<Broker> getBrokersWithExcessiveLeaderToMove(String topicName, ClusterModel clusterModel, Broker originalBroker) {
    PriorityQueue<Broker> brokersWithExcessiveLeaderToMove = new PriorityQueue<>((broker1, broker2) -> {
      int broker1LeaderCount = broker1.numLeadersFor(topicName);
      int broker2LeaderCount = broker2.numLeadersFor(topicName);
      int leaderCountCompareResult = Integer.compare(broker2LeaderCount, broker1LeaderCount);
      return leaderCountCompareResult == 0 ? Integer.compare(broker1.id(), broker2.id()) : leaderCountCompareResult;
    });
    clusterModel.aliveBrokers()
                .stream()
                .filter(broker -> broker.numLeadersFor(topicName) > minTopicLeadersPerBroker())
                .forEach(brokersWithExcessiveLeaderToMove::add);
    return brokersWithExcessiveLeaderToMove;
  }

  private static Set<Broker> eligibleBrokersForLeadership(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    return clusterModel.aliveBrokers()
                       .stream()
                       .filter(broker -> isEligibleToHaveLeaders(broker, optimizationOptions))
                       .collect(Collectors.toSet());
  }

  private static boolean isEligibleToHaveLeaders(Broker broker, OptimizationOptions optimizationOptions) {
    return !optimizationOptions.excludedBrokersForLeadership().contains(broker.id())
           && !optimizationOptions.excludedBrokersForReplicaMove().contains(broker.id());
  }

  private void moveAwayOfflineReplicas(Broker srcBroker,
                                       ClusterModel clusterModel,
                                       Set<Goal> optimizedGoals,
                                       OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    if (srcBroker.currentOfflineReplicas().isEmpty()) {
      return;
    }
    SortedSet<Broker> eligibleBrokersToMoveOfflineReplicasTo = new TreeSet<>(
        Comparator.comparingInt((Broker broker) -> broker.replicas().size()).thenComparingInt(Broker::id));
    eligibleBrokersToMoveOfflineReplicasTo.addAll(clusterModel.aliveBrokers());
    Set<Replica> offlineReplicas = new HashSet<>(srcBroker.currentOfflineReplicas());
    for (Replica offlineReplica : offlineReplicas) {
      if (maybeApplyBalancingAction(clusterModel, offlineReplica, eligibleBrokersToMoveOfflineReplicasTo,
                                    INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions) == null) {
        ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(1).build();
        throw new OptimizationFailureException(String.format("[%s] Cannot remove %s from %s broker %d (has %d replicas).", name(),
                                                             offlineReplica, srcBroker.state(), srcBroker.id(), srcBroker.replicas().size()),
                                               recommendation);
      }
    }
  }

  @Override
  public void finish() {
    super.finish();
  }
}
