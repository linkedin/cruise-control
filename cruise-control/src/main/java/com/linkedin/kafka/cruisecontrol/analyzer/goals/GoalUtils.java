/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * A util class for goals.
 */
public class GoalUtils {

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
    if (optimizationOptions.excludedBrokersForLeadership().contains(candidateId)
        && replica.originalBroker().isAlive() && replica.isLeader()) {
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
}
