/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;


/**
 * A class to represent the target for achieving the balanced distribution of replicas for the goal in terms of:
 * (1) The minimum number of replicas per broker.
 * (2) Warm broker credits: represents the number of replicas that are more than "the minimum number of replicas per
 * broker". In balanced distribution, warm broker credits are distributed evenly across cluster -- i.e. a broker has
 * either "the minimum number of replicas per broker" or one more than the minimum number of replicas per broker.
 * (3) A map for the required number of replicas to achieve the minimum number of replicas by broker id.
 * (4) A set for secondary eligible broker ids to represent broker ids that can receive one replica in case (1) there
 * are still brokers in need of offloading replicas and (2) no more cold brokers available to receive a replica.
 */
class ReplicaDistributionTarget {
  private final int _minNumReplicasPerBroker;
  private final int _warmBrokerCredits;
  // Required number of replicas by broker ids represent the destination brokers that the balancer *must* move
  // replicas to in order to achieve replica collocation balance.
  private final Map<Integer, Integer> _requiredNumReplicasByBrokerId;
  // Secondary eligible broker ids can receive one replica in case (1) there are still hot brokers in need of
  // offloading and (2) no more cold brokers available to receive a replica.
  private final Set<Integer> _secondaryEligibleBrokerIds;
  // Represents how many warm broker credits have been consumed in the balancing process.
  private int _consumedWarmBrokerCredits;

  /**
   * Constructor for replica distribution target.
   *
   * @param numReplicasToBalance The number of replicas to balance. In case replicas to be balanced are for specific
   *                             topic, this number indicates the number of replicas of this topic in the cluster.
   * @param healthyBrokers       Healthy brokers in the cluster -- i.e. brokers that are not dead.
   */
  ReplicaDistributionTarget(int numReplicasToBalance, Set<Broker> healthyBrokers) {
    _minNumReplicasPerBroker = numReplicasToBalance / healthyBrokers.size();
    _warmBrokerCredits = numReplicasToBalance % healthyBrokers.size();
    _requiredNumReplicasByBrokerId = new HashMap<>();
    _secondaryEligibleBrokerIds = new HashSet<>();
    _consumedWarmBrokerCredits = 0;
  }

  /**
   * Get the minimum number of replicas per broker in the replica distribution target.
   */
  int minNumReplicasPerBroker() {
    return _minNumReplicasPerBroker;
  }

  /**
   * Move replicas residing in the given cluster and given healthy source broker having given set of topic partitions
   * to eligible brokers. Replica movements are guaranteed not to violate the requirements of optimized goals.
   *
   * @param clusterModel           The state of the cluster.
   * @param replicasInBrokerToMove Replicas to move from the given source broker.
   * @param optimizedGoals         Goals that have already been optimized. The function ensures that their requirements won't
   *                               be violated.
   * @param excludedTopics The topics that should be excluded from the optimization action.
   */
  boolean moveReplicasInSourceBrokerToEligibleBrokers(ClusterModel clusterModel,
                                                      SortedSet<Replica> replicasInBrokerToMove,
                                                      Set<Goal> optimizedGoals,
                                                      Set<String> excludedTopics) {
    // Get number of replicas to move from the local to a remote broker to achieve the distribution target.
    int numReplicasToMove = numReplicasToMove(replicasInBrokerToMove.size());
    if (numReplicasToMove == 0) {
      return true;
    }

    for (Replica replicaToMove : replicasInBrokerToMove) {
      if (excludedTopics.contains(replicaToMove.topicPartition().topic()) && replicaToMove.originalBroker().isAlive()) {
        continue;
      }
      if (moveReplicaToEligibleBroker(clusterModel, replicaToMove, optimizedGoals)) {
        numReplicasToMove--;
        // Check if any more replicas need to move.
        if (numReplicasToMove == 0) {
          break;
        }
      }
    }

    // Update consumed warm broker credits. These credits are consumed because the broker was unable to move all
    // replicas that it was supposed to move to reach a balanced state to eligible brokers.
    _consumedWarmBrokerCredits += numReplicasToMove;
    return numReplicasToMove == 0;
  }

  /**
   * Move given self healing eligible replica residing in the given cluster in a dead or healthy broker to an eligible
   * broker. Replica movements are guaranteed not to violate the requirements of optimized goals.
   *
   * @param clusterModel     The state of the cluster.
   * @param replicaToMove    Replica to move away from its current broker.
   * @param numLocalReplicas Number of local replicas.
   * @param optimizedGoals   Goals that have already been optimized. The function ensures that their requirements won't
   *                         be violated.
   */
  void moveSelfHealingEligibleReplicaToEligibleBroker(ClusterModel clusterModel,
                                                      Replica replicaToMove,
                                                      int numLocalReplicas,
                                                      Set<Goal> optimizedGoals)
      throws OptimizationFailureException {
    // Sanity check the number of replicas to move from the local to a remote broker to achieve the distribution
    // target. If the broker is dead, the replica must move to another broker.
    if (replicaToMove.broker().isAlive() && numReplicasToMove(numLocalReplicas) == 0) {
      return;
    }

    // Attempt to move the replica to an eligible broker.
    boolean isMoveSuccessful = moveReplicaToEligibleBroker(clusterModel, replicaToMove, optimizedGoals);
    if (!replicaToMove.broker().isAlive()) {
      throw new OptimizationFailureException("Self healing failed to move the replica away from decommissioned broker.");
    }

    // Update consumed warm broker credits. Credit is consumed because the broker was unable to move replicas.
    if (isMoveSuccessful) {
      _consumedWarmBrokerCredits++;
    }
  }

  /**
   * Set broker eligibility in terms of number of replicas that it has.
   *
   * @param brokerId         Id of the broker containing given number of local replicas.
   * @param numLocalReplicas Number of local replicas in the given broker id.
   */
  void setBrokerEligibilityForReceivingReplica(int brokerId, int numLocalReplicas) {
    if (numLocalReplicas <= _minNumReplicasPerBroker) {
      _secondaryEligibleBrokerIds.add(brokerId);
      if (numLocalReplicas < _minNumReplicasPerBroker) {
        int requiredNumReplicas = _minNumReplicasPerBroker - numLocalReplicas;
        _requiredNumReplicasByBrokerId.put(brokerId, requiredNumReplicas);
      }
    }
  }

  /**
   * Get the number of replicas to move from the local broker to achieve the target for balanced replica distribution.
   * In the balanced (i.e. ideal) replica distribution, each broker has _minNumReplicasPerBroker or
   * _minNumReplicasPerBroker + 1 replicas. A broker having more than _minNumReplicasPerBroker replicas is
   * called a warm broker. A warm broker consumes numLocalReplicas - _minNumReplicasPerBroker warm broker
   * credits. If the numLocalReplicas on a broker is:
   * <p>
   * (1) Greater than (_minNumReplicasPerBroker + 1), then to reach a balanced state, it must offload:
   * ....a) (numLocalReplicas - (_minNumReplicasPerBroker + 1)) replicas (in case this broker can be a warm
   * broker and consume 1 credit).
   * ....b) (numLocalReplicas - _minNumReplicasPerBroker) replicas (in case there cannot be any more warm
   * brokers because _consumedWarmBrokerCredits >= _warmBrokerCredits).
   * <p>
   * (2) Equals (_minNumReplicasPerBroker + 1), then
   * ....a) 1 replica (in case there cannot be any more warm brokers).
   * ....b) 0 replicas (in case this broker can be a warm broker).
   * <p>
   * (3) Otherwise the broker has either less or equal to the minimum number of replicas per broker in the balanced
   * state.
   *
   * @param numLocalReplicas Number of local replicas (may have certain properties determined by the goal -- e.g.
   *                         sharing the same topic) residing at the local broker.
   * @return Number of replicas to move from the local broker to achieve the target for balanced replica distribution.
   */
  private int numReplicasToMove(int numLocalReplicas) {
    int numReplicasToMove = numLocalReplicas - _minNumReplicasPerBroker - 1;
    if (numReplicasToMove < 0) {
      return 0;
    }

    boolean isConsumedWarmBrokerCredit = false;
    if (_warmBrokerCredits > _consumedWarmBrokerCredits) {
      _consumedWarmBrokerCredits++;
      isConsumedWarmBrokerCredit = true;
    }

    return isConsumedWarmBrokerCredit ? numReplicasToMove : (numReplicasToMove + 1);
  }

  /**
   * Get a LinkedHashSet of unique broker ids containing required number of replicas by broker id before secondary
   * eligible broker ids.
   */
  private LinkedHashSet<Integer> sortedCandidateBrokerIds() {
    LinkedHashSet<Integer> sortedEligibleBrokerIds = new LinkedHashSet<>(_requiredNumReplicasByBrokerId.keySet());
    sortedEligibleBrokerIds.addAll(_secondaryEligibleBrokerIds);
    return sortedEligibleBrokerIds;
  }

  /**
   * Move given replica to an eligible broker.
   *
   * @param clusterModel   The state of the cluster.
   * @param replicaToMove  Replica to move away from its current broker.
   * @param optimizedGoals Goals that have already been optimized. The function ensures that their requirements won't
   *                       be violated.
   * @return True if replica move succeeds, false otherwise.
   */
  private boolean moveReplicaToEligibleBroker(ClusterModel clusterModel, Replica replicaToMove, Set<Goal> optimizedGoals) {
    boolean isMoveSuccessful = false;
    // Get eligible brokers to receive this replica.
    for (int brokerId : replicaToMove.broker().isAlive()
                        ? sortedCandidateBrokerIds()
                        : clusterModel.healthyBrokers().stream().map(Broker::id).collect(Collectors.toList())) {
      // filter out the broker that is not eligible.
      if (!isEligibleForReplica(clusterModel, replicaToMove, brokerId)) {
        continue;
      }
      // Check if movement is acceptable by this and optimized goals. Movement is acceptable by (1) this goal
      // if the eligible destination broker does not contain a replica in the same partition set, (2) optimized
      // goals if for each optimized goal accepts the replica movement action.
      BalancingAction optimizedGoalProposal =
          new BalancingAction(replicaToMove.topicPartition(), replicaToMove.broker().id(), brokerId,
                              ActionType.REPLICA_MOVEMENT);
      boolean canMove = (clusterModel.broker(brokerId).replica(replicaToMove.topicPartition()) == null)
                        && AnalyzerUtils.isProposalAcceptableForOptimizedGoals(
                            optimizedGoals, optimizedGoalProposal, clusterModel) == ACCEPT;
      if (canMove) {
        clusterModel.relocateReplica(replicaToMove.topicPartition(), replicaToMove.broker().id(), brokerId);
        isMoveSuccessful = true;
        // Consume an eligible broker id.
        Integer requiredNumReplicas = _requiredNumReplicasByBrokerId.get(brokerId);
        if (requiredNumReplicas != null) {
          if (requiredNumReplicas == 1) {
            _requiredNumReplicasByBrokerId.remove(brokerId);
          } else {
            requiredNumReplicas--;
            _requiredNumReplicasByBrokerId.put(brokerId, requiredNumReplicas);
          }
        } else {
          _secondaryEligibleBrokerIds.remove(brokerId);
        }
        break;
      }
    }
    return isMoveSuccessful;
  }

  private boolean isEligibleForReplica(ClusterModel clusterModel,
                                       Replica replica,
                                       int candidateBrokerId) {
    Set<Broker> newBrokers = clusterModel.newBrokers();
    Broker candidateBroker = clusterModel.broker(candidateBrokerId);
    return newBrokers.isEmpty()
        || candidateBroker.isNew()
        || replica.originalBroker() == candidateBroker;
  }
}
