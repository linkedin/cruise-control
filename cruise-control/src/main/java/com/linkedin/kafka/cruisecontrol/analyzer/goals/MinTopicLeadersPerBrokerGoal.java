/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.Utils;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.LEADERSHIP_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.INTER_BROKER_REPLICA_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.*;


/**
 * HARD GOAL: Generate leadership movement and leader replica movement proposals to ensure that each alive broker that
 * is not excluded for replica leadership moves has at minimum number (specified by "min.topic.leaders.per.broker" config
 * property) of leader replica of each topic in a configured set of topics (specified by "topics.with.min.leaders.per.broker"
 * config property).
 */
public class MinTopicLeadersPerBrokerGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(MinTopicLeadersPerBrokerGoal.class);

  /**
   * This constructor is required for unit test
   */
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
   * requirements of the goal. Requirements(hard goal): A configured set of topics that should have a minimum number
   * of leader replicas on each alive broker that is not excluded for replica leadership moves
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   *         {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    if (!topicOfInterest(action.topic()) || !topicsWithMinLeadersPerBrokerNames(clusterModel).contains(action.topic())) {
      // Always accept balancing action on topics that are not of the interest of this goal
      return ACCEPT;
    }
    switch (action.balancingAction()) {
      case LEADERSHIP_MOVEMENT:
      case INTER_BROKER_REPLICA_MOVEMENT:
        Replica replicaToBeRemoved = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
        return doesLeaderReplicaRemoveViolateOptimizedGoal(replicaToBeRemoved) ? REPLICA_REJECT : ACCEPT;
      case INTER_BROKER_REPLICA_SWAP:
        Replica srcReplicaToSwap = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
        Replica dstReplicaToSwap = clusterModel.broker(action.destinationBrokerId()).replica(action.destinationTopicPartition());
        return acceptReplicaSwap(srcReplicaToSwap, dstReplicaToSwap);
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  private ActionAcceptance acceptReplicaSwap(Replica srcReplicaToSwap, Replica dstReplicaToSwap) {
    if (srcReplicaToSwap.isLeader() == dstReplicaToSwap.isLeader()) {
      return ACCEPT;
    }
    if (srcReplicaToSwap.isLeader()) {
      return doesLeaderReplicaRemoveViolateOptimizedGoal(srcReplicaToSwap) ? REPLICA_REJECT : ACCEPT;
    } else { // dstReplicaToSwap.isLeader()
      return doesLeaderReplicaRemoveViolateOptimizedGoal(dstReplicaToSwap) ? REPLICA_REJECT : ACCEPT;
    }
  }

  /**
   * Check whether this goal is violated after removing the given leader replica. It is violated if there is not be enough
   * leader replicas on that broker for that topic
   *
   * @param replicaToBeRemoved replica to be removed
   * @return {@code true} if the given replica move would violate the optimized goal (i.e. the move is not acceptable),
   * {@code false} otherwise.
   */
  private boolean doesLeaderReplicaRemoveViolateOptimizedGoal(Replica replicaToBeRemoved) {
    if (!replicaToBeRemoved.isLeader()) {
      // Moving a follower replica does not violate/affect this goal
      return false;
    }
    int topicLeaderReplicaCountOnSourceBroker =
        replicaToBeRemoved.broker().numReplicasOfTopicInBroker(replicaToBeRemoved.topicPartition().topic(), true);
    return topicLeaderReplicaCountOnSourceBroker <= minTopicLeadersPerBroker();
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
    if (!hasTopicsWithMinLeadersPerBrokerPattern()) {
      return;
    }
    // Sanity checks
    validateTopicsWithMinLeaderReplicaIsNotExcluded(clusterModel, optimizationOptions);
    validateEnoughLeaderReplicaToDistribute(clusterModel, optimizationOptions);
    validateBrokersAllowedReplicaMoveExist(clusterModel, optimizationOptions);

    // Sort leader replicas for each broker based on resource utilization.
    new SortedReplicasHelper().addPriorityFunc(ReplicaSortFunctionFactory.prioritizeImmigrants())
                              .trackSortedReplicasFor(replicaSortName(this, false, false), clusterModel);
  }

  private Set<String> topicsWithMinLeadersPerBrokerNames(ClusterModel clusterModel) {
    if (!hasTopicsWithMinLeadersPerBrokerPattern()) {
      return Collections.emptySet(); // Avoid running unnecessary regex pattern matching to save CPU cycles
    }
    return Utils.getTopicNamesMatchedWithPattern(_balancingConstraint.topicsWithMinLeadersPerBrokerPattern(),
                                                 clusterModel.topics());
  }

  private boolean hasTopicsWithMinLeadersPerBrokerPattern() {
    return !_balancingConstraint.topicsWithMinLeadersPerBrokerPattern().pattern().isEmpty();
  }

  private int minTopicLeadersPerBroker() {
    return _balancingConstraint.minTopicLeadersPerBroker();
  }

  private void validateTopicsWithMinLeaderReplicaIsNotExcluded(ClusterModel clusterModel,
                                                               OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    Set<String> mustHaveLeaderReplicaPerBrokerTopicNames = topicsWithMinLeadersPerBrokerNames(clusterModel);
    if (mustHaveLeaderReplicaPerBrokerTopicNames.isEmpty() || optimizationOptions.excludedTopics().isEmpty()) {
      return;
    }
    List<String> shouldNotBeExcludedTopics = new ArrayList<>();
    mustHaveLeaderReplicaPerBrokerTopicNames.forEach(topicName -> {
      if (optimizationOptions.excludedTopics().contains(topicName)) {
        shouldNotBeExcludedTopics.add(topicName);
      }
    });
    if (!shouldNotBeExcludedTopics.isEmpty()) {
      throw new OptimizationFailureException(String.format("Topics that must have a minimum number of leader replicas "
          + "per alive broker that is not excluded for leadership moves should not be excluded. This error implies"
          + "a config error. Topics should not be excluded=[%s]", String.join(", ", shouldNotBeExcludedTopics)));
    }
  }

  private void validateEnoughLeaderReplicaToDistribute(ClusterModel clusterModel,
                                                       OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    Set<String> mustHaveLeaderReplicaPerBrokerTopics = topicsWithMinLeadersPerBrokerNames(clusterModel);
    if (mustHaveLeaderReplicaPerBrokerTopics.isEmpty()) {
      return;
    }
    Map<String, List<Replica>> replicasByTopicNames = clusterModel.allReplicasOfTopics(mustHaveLeaderReplicaPerBrokerTopics);
    Set<Broker> eligibleBrokersForLeadership = eligibleToHaveLeaderReplicaBrokers(clusterModel, optimizationOptions);
    int totalMinimumLeaderReplicaCount = eligibleBrokersForLeadership.size() * minTopicLeadersPerBroker();

    for (Map.Entry<String, List<Replica>> replicasByTopicName : replicasByTopicNames.entrySet()) {
      int totalLeaderReplicaCountForTopic = countLeaderReplicas(replicasByTopicName.getValue());
      if (totalLeaderReplicaCountForTopic < totalMinimumLeaderReplicaCount) {
        throw new OptimizationFailureException(
            String.format("Cannot distribute %d leader replica(s) over %d broker(s) with total minimum required leader "
                    + "replica count %d for topic %s.",
                totalLeaderReplicaCountForTopic, eligibleBrokersForLeadership.size(),
                totalLeaderReplicaCountForTopic, replicasByTopicName.getKey()));
      }
    }
  }

  private int countLeaderReplicas(Collection<Replica> replicas) {
    int leaderReplicaCount = 0;
    for (Replica replica : replicas) {
      if (replica.isLeader()) {
        leaderReplicaCount++;
      }
    }
    return leaderReplicaCount;
  }

  private void validateBrokersAllowedReplicaMoveExist(ClusterModel clusterModel,
                                                      OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    Set<Integer> brokersAllowedReplicaMove = GoalUtils.aliveBrokersNotExcludedForReplicaMove(clusterModel, optimizationOptions);
    if (brokersAllowedReplicaMove.isEmpty()) {
      // Handle the case when all alive brokers are excluded from replica moves.
      throw new OptimizationFailureException("Cannot take any action as all alive brokers are excluded from replica moves.");
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
    if (!topicOfInterest(action.topic()) || !topicsWithMinLeadersPerBrokerNames(clusterModel).contains(action.topic())) {
      // Always self-satisfied on topics that are not of the interest of this goal
      return true;
    }
    switch (action.balancingAction()) {
      case LEADERSHIP_MOVEMENT:
      case INTER_BROKER_REPLICA_MOVEMENT:
        Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
        Replica replicaToBeMoved = sourceBroker.replica(action.topicPartition());
        if (!replicaToBeMoved.isLeader()) {
          return true; // Move a follower replica of a topic of this goal's interest is always self-satisfied since it does not matter
        }
        return isSelfSatisfiedToMoveOneLeaderReplica(sourceBroker,
                                                     clusterModel.broker(action.destinationBrokerId()),
                                                     action.topic());
      case INTER_BROKER_REPLICA_SWAP:
        Replica sourceReplica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
        Replica destinationReplica = clusterModel.broker(action.destinationBrokerId()).replica(action.topicPartition());
        if (sourceReplica.isLeader() == destinationReplica.isLeader()) {
          // Swapping either 2 leader replicas or 2 follower replicas has no effect on this goal
          return true;
        }
        // At this point, one replica must be leader and the other must be follower
        if (sourceReplica.isLeader()) {
          return isSelfSatisfiedToMoveOneLeaderReplica(clusterModel.broker(action.sourceBrokerId()),
                                                       clusterModel.broker(action.destinationBrokerId()),
                                                       action.topic());
        }
        // The replica on the destination broker is a leader replica
        return isSelfSatisfiedToMoveOneLeaderReplica(clusterModel.broker(action.destinationBrokerId()),
                                                     clusterModel.broker(action.sourceBrokerId()),
                                                     action.topic());
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  private boolean topicOfInterest(String topicName) {
    if (_balancingConstraint.topicsWithMinLeadersPerBrokerPattern().pattern().isEmpty()) {
      return false; // No topic is of this goal's interest
    }
    return _balancingConstraint.topicsWithMinLeadersPerBrokerPattern().matcher(topicName).matches();
  }

  /**
   * Check whether it is self-satisfied to move a leader replica of a topic with the given name from a the given source
   * broker to the given destination broker
   * @param sourceBroker the broker from which a leader replica is removed
   * @param destinationBroker the broker from which a leader replica is added
   * @param topicName topic name of the to-be-moved replica
   * @return {@code true} if the given replica move would violate the optimized goal (i.e. the move is not acceptable),
   * {@code false} otherwise.
   */
  private boolean isSelfSatisfiedToMoveOneLeaderReplica(Broker sourceBroker, Broker destinationBroker, String topicName) {
    // Moving a replica leadership from a not-alive broker to an alive broker is self-satisfied
    if (!sourceBroker.isAlive() && destinationBroker.isAlive()) {
      return true;
    }
    // Moving leader replica from more abundant broker could be considered as self-satisfied
    return sourceBroker.numReplicasOfTopicInBroker(topicName, true) >
           destinationBroker.numReplicasOfTopicInBroker(topicName, true);
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
    ensureBrokersAllHaveEnoughLeaderReplicaOfTopics(clusterModel, optimizationOptions);
    finish();
  }

  private void ensureBrokersAllHaveEnoughLeaderReplicaOfTopics(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    Set<String> mustHaveLeaderReplicaPerBrokerTopicNames = topicsWithMinLeadersPerBrokerNames(clusterModel);
    if (mustHaveLeaderReplicaPerBrokerTopicNames.isEmpty()) {
      return; // Early termination to avoid some unnecessary computation
    }
    for (Broker broker : clusterModel.brokers()) {
      if (!brokerIsEligibleToHaveAnyLeaderReplica(broker, optimizationOptions)) {
        continue;
      }
      for (String mustHaveLeaderReplicaPerBrokerTopicName : mustHaveLeaderReplicaPerBrokerTopicNames) {
        int leaderReplicaCount = broker.numReplicasOfTopicInBroker(mustHaveLeaderReplicaPerBrokerTopicName, true);
        if (leaderReplicaCount < minTopicLeadersPerBroker()) {
          throw new OptimizationFailureException(String.format("Broker %d does not have enough leader replica for topic %s. "
              + "Minimum required per-broker leader replica count %d. Actual broker leader replica count %d",
              broker.id(), mustHaveLeaderReplicaPerBrokerTopicName, minTopicLeadersPerBroker(), leaderReplicaCount));
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
    Set<String> mustHaveLeaderReplicaPerBrokerTopicNames = topicsWithMinLeadersPerBrokerNames(clusterModel);
    if (mustHaveLeaderReplicaPerBrokerTopicNames.isEmpty()) {
      return; // Early termination to avoid some unnecessary computation
    }
    if (!brokerIsEligibleToHaveAnyLeaderReplica(broker, optimizationOptions)) {
      return;
    }
    for (String topicMustHaveLeaderReplicaPerBroker : mustHaveLeaderReplicaPerBrokerTopicNames) {
      maybeMoveLeaderReplicaOfTopicToBroker(topicMustHaveLeaderReplicaPerBroker, broker, clusterModel, optimizedGoals, optimizationOptions);
    }
  }

  private void maybeMoveLeaderReplicaOfTopicToBroker(String topicMustHaveLeaderReplicaPerBroker,
                                                     Broker broker,
                                                     ClusterModel clusterModel,
                                                     Set<Goal> optimizedGoals,
                                                     OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    if (brokerHasSufficientLeaderReplicasOfTopic(broker, topicMustHaveLeaderReplicaPerBroker)) {
      return; // This broker has enough leader replica(s)
    }
    // Try to elect follower replica(s) of the interested topic on this broker to be leader
    Set<Replica> followerReplicas = broker.replicas().stream()
                                          .filter(
                                              replica -> !replica.isLeader()
                                                         && replica.topicPartition().topic().equals(topicMustHaveLeaderReplicaPerBroker))
                                          .collect(Collectors.toSet());

    for (Replica followerReplica : followerReplicas) {
      Replica leaderReplica = leaderReplicaOfTopicPartition(clusterModel, followerReplica.topicPartition());
      if (leaderReplica == null) {
        LOG.warn("No leader replica for {}", followerReplica.topicPartition());
      } else {
        if (leaderReplica.broker().id() != broker.id()
            && leaderReplica.broker().numReplicasOfTopicInBroker(topicMustHaveLeaderReplicaPerBroker, true) > minTopicLeadersPerBroker()) {
          maybeApplyBalancingAction(clusterModel, leaderReplica, Collections.singleton(broker),
                                    LEADERSHIP_MOVEMENT, optimizedGoals, optimizationOptions);
        }
      }
      if (brokerHasSufficientLeaderReplicasOfTopic(broker, topicMustHaveLeaderReplicaPerBroker)) {
        return; // This broker satisfies this goal
      }
    }

    // Try to move leader replica(s) of the interested topic from other brokers to this broker
    PriorityQueue<Broker> brokersWithExcessiveLeaderReplicaToMove =
        getBrokersWithExcessiveLeaderReplicaToMove(topicMustHaveLeaderReplicaPerBroker, clusterModel, broker);

    while (!brokersWithExcessiveLeaderReplicaToMove.isEmpty()) {
      Broker brokerWithExcessiveLeaderReplicaToMove = brokersWithExcessiveLeaderReplicaToMove.poll();
      List<Replica> leaderReplicasOfTopic = brokerWithExcessiveLeaderReplicaToMove.replicas().stream()
                                                                                  .filter(replica ->
                                                                                              replica.isLeader()
                                                                                              && replica.topicPartition()
                                                                                                        .topic()
                                                                                                        .equals(topicMustHaveLeaderReplicaPerBroker))
                                                                                  .collect(Collectors.toList());
      boolean leaderReplicaMoved = false;
      for (Replica leaderReplicaOfTopic : leaderReplicasOfTopic) {
        Broker destinationBroker = maybeApplyBalancingAction(clusterModel, leaderReplicaOfTopic, Collections.singleton(broker),
                                                             INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions);
        if (destinationBroker != null) {
          leaderReplicaMoved = true;
          break; // Successfully move one leader replica
        }
      }
      if (leaderReplicaMoved) {
        if (brokerHasSufficientLeaderReplicasOfTopic(broker, topicMustHaveLeaderReplicaPerBroker)) {
          return; // This broker satisfies this goal
        }
        if (brokerHasExcessiveLeaderReplicasOfTopic(topicMustHaveLeaderReplicaPerBroker, brokerWithExcessiveLeaderReplicaToMove)) {
          brokersWithExcessiveLeaderReplicaToMove.add(brokerWithExcessiveLeaderReplicaToMove);
        }
      }
    }

    if (!brokerHasSufficientLeaderReplicasOfTopic(broker, topicMustHaveLeaderReplicaPerBroker)) {
      throw new OptimizationFailureException(String.format("Cannot make broker %d have at least %d leader replica(s) of topic %s",
                                                           broker.id(),
                                                           minTopicLeadersPerBroker(),
                                                           topicMustHaveLeaderReplicaPerBroker));
    }
  }

  private boolean brokerHasSufficientLeaderReplicasOfTopic(Broker broker, String topicName) {
    return broker.numReplicasOfTopicInBroker(topicName, true) >= minTopicLeadersPerBroker();
  }

  private PriorityQueue<Broker> getBrokersWithExcessiveLeaderReplicaToMove(String topicName, ClusterModel clusterModel, Broker originalBroker) {
    PriorityQueue<Broker> brokersWithExcessiveLeaderReplicaToMove = new PriorityQueue<>((broker1, broker2) -> {
      int broker1LeaderReplicaCount = broker1.numReplicasOfTopicInBroker(topicName, true);
      int broker2LeaderReplicaCount = broker2.numReplicasOfTopicInBroker(topicName, true);
      int leaderReplicaCountCompareResult = Integer.compare(broker2LeaderReplicaCount, broker1LeaderReplicaCount);
      return leaderReplicaCountCompareResult == 0 ? Integer.compare(broker1.id(), broker2.id()) : leaderReplicaCountCompareResult;
    });
    clusterModel.brokers().forEach((broker -> {
      if (broker.isAlive() && broker.id() != originalBroker.id() && brokerHasExcessiveLeaderReplicasOfTopic(topicName, broker)) {
        brokersWithExcessiveLeaderReplicaToMove.add(broker);
      }
    }));
    return brokersWithExcessiveLeaderReplicaToMove;
  }

  private boolean brokerHasExcessiveLeaderReplicasOfTopic(String topicName, Broker broker) {
    return broker.numReplicasOfTopicInBroker(topicName, true) > minTopicLeadersPerBroker();
  }

  @Nullable
  private Replica leaderReplicaOfTopicPartition(ClusterModel clusterModel, TopicPartition topicPartition) {
    for (Replica leaderReplica : clusterModel.leaderReplicas()) {
      if (leaderReplica.topicPartition().equals(topicPartition)) {
        return leaderReplica;
      }
    }
    return null;
  }


  private Set<Broker> eligibleToHaveLeaderReplicaBrokers(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    return clusterModel.brokers()
                       .stream()
                       .filter(broker -> brokerIsEligibleToHaveAnyLeaderReplica(broker, optimizationOptions))
                       .collect(Collectors.toSet());
  }

  private boolean brokerIsEligibleToHaveAnyLeaderReplica(Broker broker, OptimizationOptions optimizationOptions) {
    return broker.isAlive()
           && !optimizationOptions.excludedBrokersForLeadership().contains(broker.id())
           && !optimizationOptions.excludedBrokersForReplicaMove().contains(broker.id());
  }

  private void moveAwayOfflineReplicas(Broker srcBroker,
                                       ClusterModel clusterModel,
                                       Set<Goal> optimizedGoals,
                                       OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    if (srcBroker.currentOfflineReplicas().isEmpty()) {
      return;
    }
    SortedSet<Broker> eligibleBrokersToMoveOfflineReplicasTo = findEligibleBrokersToMoveOfflineReplicasTo(clusterModel, optimizationOptions);
    eligibleBrokersToMoveOfflineReplicasTo.removeIf(broker -> broker.id() == srcBroker.id());
    if (eligibleBrokersToMoveOfflineReplicasTo.isEmpty()) {
      throw new OptimizationFailureException("Cannot move all replicas away from broker " + srcBroker);
    }
    Set<Replica> offlineReplicas = new HashSet<>(srcBroker.currentOfflineReplicas());
    for (Replica offlineReplica : offlineReplicas) {
      if (maybeApplyBalancingAction(clusterModel, offlineReplica, eligibleBrokersToMoveOfflineReplicasTo,
                                    INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions) == null) {
        throw new OptimizationFailureException(String.format("Cannot move offline replica %s to any broker in %s",
                                                             offlineReplica, eligibleBrokersToMoveOfflineReplicasTo));
      }
    }
  }

  private SortedSet<Broker> findEligibleBrokersToMoveOfflineReplicasTo(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    Set<Broker> candidateBrokers = clusterModel.brokers()
                                               .stream()
                                               .filter(broker -> broker.isAlive()
                                                                 && !optimizationOptions.excludedBrokersForReplicaMove().contains(broker.id()))
                                               .collect(Collectors.toSet());

    SortedSet<Broker> sortedBrokers = new TreeSet<>(
        Comparator.comparingInt((Broker broker) -> broker.replicas().size()).thenComparingInt(Broker::id));
    sortedBrokers.addAll(candidateBrokers);
    return sortedBrokers;
  }

  @Override
  public void finish() {
    super.finish();
  }
}
