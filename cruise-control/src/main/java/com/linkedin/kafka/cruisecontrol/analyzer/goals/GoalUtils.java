/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.CandidateBroker;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;


/**
 * A util class for goals.
 */
public class GoalUtils {
  private static final double DEAD_BROKER_UTILIZATION = 1.0;

  private GoalUtils() {

  }

  /**
   * Check whether the execution of a {@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#REPLICA_MOVEMENT}
   * action is eligible for the given replica in the given clusterModel to the given candidate broker.
   *
   * Invariant-1: If there are new brokers, an eligible candidate that triggers an action must be a new broker.
   *
   * @param clusterModel The state of the cluster.
   * @param replica  Replica to check for action eligibility.
   * @param candidateId Candidate broker id.
   * @param optimizationOptions Options to take into account while moving the given replica.
   * @return True if the candidate broker is eligible, false otherwise.
   */
  static boolean isEligibleForReplicaMove(ClusterModel clusterModel,
                                          Replica replica,
                                          int candidateId,
                                          OptimizationOptions optimizationOptions) {
    // Check eligibility for leadership
    if (optimizationOptions.excludedBrokersForLeadership().contains(candidateId)
        && replica.originalBroker().isAlive() && replica.isLeader()) {
      return false;
    }

    // Check eligibility for replica move
    if (optimizationOptions.excludedBrokersForReplicaMove().contains(candidateId)
        && replica.originalBroker().isAlive()) {
      return false;
    }

    if (clusterModel.newBrokers().isEmpty()) {
      return true;
    }

    Broker candidateBroker = clusterModel.broker(candidateId);
    return candidateBroker.isNew() || candidateBroker == replica.originalBroker();
  }

  /**
   * Filter out the given excluded brokers from the original brokers (if needed). If the action is:
   * <ul>
   * <li>{@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#LEADERSHIP_MOVEMENT}, then brokers excluded for
   * leadership are not eligible.</li>
   * <li>{@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#REPLICA_MOVEMENT} for a leader replica, then unless
   * the source leader replica is dead, brokers excluded for leadership are not eligible.</li>
   * </ul>
   *
   * Note that this function supports only the above actions.
   *
   * @param originalBrokers Original list of brokers to be filtered.
   * @param excludedBrokers Brokers to be excluded from.
   * @param replica Replica affected from the action.
   * @param action Action that affects the given replica.
   */
  private static void filterOutBrokersExcludedForLeadership(List<Broker> originalBrokers,
                                                            Set<Integer> excludedBrokers,
                                                            Replica replica,
                                                            ActionType action) {
    if (!excludedBrokers.isEmpty()
        && (action == ActionType.LEADERSHIP_MOVEMENT || (replica.originalBroker().isAlive() && replica.isLeader()))) {
      originalBrokers.removeIf(broker -> excludedBrokers.contains(broker.id()));
    }
  }

  /**
   * Filter out the given excluded brokers from the original brokers (if needed). If the action is:
   * <ul>
   * <li>{@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#REPLICA_MOVEMENT}, then unless the source replica
   * is dead, brokers excluded for replica move are not eligible.</li>
   * </ul>
   *
   * @param originalBrokers Original list of brokers to be filtered.
   * @param excludedBrokers Brokers to be excluded from.
   * @param replica Replica affected from the action.
   * @param action Action that affects the given replica.
   */
  private static void filterOutBrokersExcludedForReplicaMove(List<Broker> originalBrokers,
                                                             Set<Integer> excludedBrokers,
                                                             Replica replica,
                                                             ActionType action) {
    if (!excludedBrokers.isEmpty() && action == ActionType.REPLICA_MOVEMENT && replica.originalBroker().isAlive()) {
      originalBrokers.removeIf(broker -> excludedBrokers.contains(broker.id()));
    }
  }

  /**
   * Filter the given candidate brokers in the given clusterModel to retrieve the eligible ones for execution of a
   * {@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#REPLICA_MOVEMENT} or
   * {@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#LEADERSHIP_MOVEMENT} action for the given replica.
   *
   * Invariant-1: If there are new brokers, an eligible candidate that triggers an action must be a new broker.
   * Invariant-2: Brokers excluded for leadership if exclusion applies to the given replica for the given action.
   *
   * @param clusterModel The state of the cluster.
   * @param replica  Replica to check for action eligibility.
   * @param candidates Candidate brokers among which the eligible ones will be selected.
   * @param action Action that affects the given replica.
   * @param optimizationOptions Options to take into account while applying the given action.
   * @return List of eligible brokers with a fixed order.
   */
  static List<Broker> eligibleBrokers(ClusterModel clusterModel,
                                      Replica replica,
                                      Collection<Broker> candidates,
                                      ActionType action,
                                      OptimizationOptions optimizationOptions) {

    List<Broker> eligibleBrokers = new ArrayList<>(candidates);
    filterOutBrokersExcludedForLeadership(eligibleBrokers, optimizationOptions.excludedBrokersForLeadership(), replica, action);
    filterOutBrokersExcludedForReplicaMove(eligibleBrokers, optimizationOptions.excludedBrokersForReplicaMove(), replica, action);

    if (clusterModel.newBrokers().isEmpty()) {
      return eligibleBrokers;
    }

    // When there are new brokers, we should only allow the replicas/leadership to be moved to the new brokers.
    return eligibleBrokers.stream().filter(b -> b.isNew() || b == replica.originalBroker()).collect(Collectors.toList());
  }

  /**
   * Check whether the proposed action is legit. An action is legit if it is:
   * (1) a replica movement and the destination does not have a replica of the same partition, or
   * (2) a leadership movement, the replica is a leader and the destination broker has a follower of the same partition.
   *
   * @param replica Replica that is affected from the given action type.
   * @param destinationBroker Destination broker.
   * @param actionType Action type.
   * @return True if the move is legit, false otherwise.
   */
  static boolean legitMove(Replica replica, Broker destinationBroker, ActionType actionType) {
    if (actionType == ActionType.REPLICA_MOVEMENT && destinationBroker.replica(replica.topicPartition()) == null) {
      return true;
    }

    return actionType == ActionType.LEADERSHIP_MOVEMENT && replica.isLeader()
           && destinationBroker.replica(replica.topicPartition()) != null;
  }

  /**
   * Get eligible replicas among the given candidate replicas for the proposed swap operation of the source replica.
   * Invariant-1: No replica is eligible if the candidate broker is excluded for leadership and the source replica is the leader.
   * Invariant-2: No replica is eligible if the candidate broker is excluded for replica move.
   *
   * @param clusterModel The state of the cluster.
   * @param sourceReplica Source replica for intended swap operation.
   * @param cb Candidate broker containing candidate replicas to swap with the source replica in the order of attempts to swap.
   * @return Eligible replicas for swap.
   */
  static SortedSet<Replica> eligibleReplicasForSwap(ClusterModel clusterModel, Replica sourceReplica, CandidateBroker cb) {
    if (cb.shouldExcludeForLeadership(sourceReplica) || cb.shouldExcludeForReplicaMove(sourceReplica)) {
      return Collections.emptySortedSet();
    }

    // Candidate replicas from the same destination broker to swap in the order of attempts to swap.
    SortedSet<Replica> candidateReplicasToSwapWith = cb.replicas();

    // CASE#1: All candidate replicas are eligible if any of the following is true:
    // (1) there are no new brokers in the cluster,
    // (2) the given candidate set contains no replicas,
    // (3) the intended swap is between replicas of new brokers,
    // (4) the intended swap is between a replica on a new broker, which originally was in the destination broker, and
    // any replica in the destination broker.
    Broker sourceBroker = sourceReplica.broker();
    Broker destinationBroker = candidateReplicasToSwapWith.isEmpty() ? null : candidateReplicasToSwapWith.first().broker();

    if (clusterModel.newBrokers().isEmpty()
        || destinationBroker == null
        || (sourceBroker.isNew() && (destinationBroker.isNew() || sourceReplica.originalBroker() == destinationBroker))) {
      return candidateReplicasToSwapWith;
    }

    // CASE#2: A subset of candidate replicas might be eligible if only the destination broker is a new broker and it
    // contains replicas that were originally in the source broker.
    if (destinationBroker.isNew()) {
      candidateReplicasToSwapWith.removeIf(replica -> replica.originalBroker() != sourceBroker);
      return candidateReplicasToSwapWith;
    }

    // CASE#3: No swap is possible between old brokers when there are new brokers in the cluster.
    return Collections.emptySortedSet();
  }

  /**
   * Get the utilization percentage of the broker for the given resource, or {@link #DEAD_BROKER_UTILIZATION} if the
   * broker is dead.
   *
   * @param broker Broker for which the resource utilization percentage has been queried.
   * @param resource Resource for the utilization percentage.
   * @return Utilization percentage of the broker for the given resource.
   */
  public static double utilizationPercentage(Broker broker, Resource resource) {
    double brokerCapacity = broker.capacityFor(resource);
    return brokerCapacity > 0 ? broker.load().expectedUtilizationFor(resource) / brokerCapacity : DEAD_BROKER_UTILIZATION;
  }
}
