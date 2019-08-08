/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.CandidateBroker;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;


/**
 * A util class for goals.
 */
public class GoalUtils {
  public static final int MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING = 1;
  private static final double DEAD_BROKER_UTILIZATION = 1.0;
  private static final double DEAD_DISK_UTILIZATION = 1.0;

  private GoalUtils() {

  }

  /**
   * Check whether the execution of a {@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#INTER_BROKER_REPLICA_MOVEMENT}
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
   * Filter out the given excluded brokers from the original brokers (if needed). If the user explicitly specified the
   * eligible destination brokers, and the action is not leadership movement, then retain only the brokers in the
   * requested destination brokers. Otherwise, if the action is:
   * <ul>
   * <li>{@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#LEADERSHIP_MOVEMENT}, then brokers excluded for
   * leadership are not eligible.</li>
   * <li>{@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#INTER_BROKER_REPLICA_MOVEMENT} for a leader replica,
   * then unless the source leader replica is dead, brokers excluded for leadership are not eligible.</li>
   * </ul>
   *
   * Note that this function supports only the above actions.
   *
   * @param originalBrokers Original list of brokers to be filtered.
   * @param optimizationOptions Options to take into account while filtering out brokers.
   * @param replica Replica affected from the action.
   * @param action Action that affects the given replica.
   */
  private static void filterOutBrokersExcludedForLeadership(List<Broker> originalBrokers,
                                                            OptimizationOptions optimizationOptions,
                                                            Replica replica,
                                                            ActionType action) {
    Set<Integer> requestedDestinationBrokerIds = optimizationOptions.requestedDestinationBrokerIds();
    if (requestedDestinationBrokerIds.isEmpty() || (action == ActionType.LEADERSHIP_MOVEMENT)) {
      Set<Integer> excludedBrokers = optimizationOptions.excludedBrokersForLeadership();
      if (!excludedBrokers.isEmpty()
          && (action == ActionType.LEADERSHIP_MOVEMENT || (replica.originalBroker().isAlive() && replica.isLeader()))) {
        originalBrokers.removeIf(broker -> excludedBrokers.contains(broker.id()));
      }
    } else {
      // Retain only the brokers that are in the requested destination brokers.
      originalBrokers.removeIf(b -> !requestedDestinationBrokerIds.contains(b.id()));
    }
  }

  /**
   * Filter out the given excluded brokers from the original brokers (if needed). If the user explicitly specified the
   * eligible destination brokers, and the action is not leadership movement, then retain only the brokers in the
   * requested destination brokers. Otherwise, if the action is:
   * <ul>
   * <li>{@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#INTER_BROKER_REPLICA_MOVEMENT}, then unless the source replica
   * is dead, brokers excluded for replica move are not eligible.</li>
   * </ul>
   *
   * @param originalBrokers Original list of brokers to be filtered.
   * @param optimizationOptions Options to take into account while filtering out brokers.
   * @param replica Replica affected from the action.
   * @param action Action that affects the given replica.
   */
  private static void filterOutBrokersExcludedForReplicaMove(List<Broker> originalBrokers,
                                                             OptimizationOptions optimizationOptions,
                                                             Replica replica,
                                                             ActionType action) {
    Set<Integer> requestedDestinationBrokerIds = optimizationOptions.requestedDestinationBrokerIds();
    if (requestedDestinationBrokerIds.isEmpty()) {
      Set<Integer> excludedBrokers = optimizationOptions.excludedBrokersForReplicaMove();
      if (!excludedBrokers.isEmpty() && action == ActionType.INTER_BROKER_REPLICA_MOVEMENT
          && replica.originalBroker().isAlive()) {
        originalBrokers.removeIf(broker -> excludedBrokers.contains(broker.id()));
      }
    } else if (action != ActionType.LEADERSHIP_MOVEMENT) {
      // Retain only the brokers that are in the requested destination brokers.
      originalBrokers.removeIf(b -> !requestedDestinationBrokerIds.contains(b.id()));
    }
  }

  /**
   * Filter the given candidate brokers in the given clusterModel to retrieve the eligible ones for execution of a
   * {@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#INTER_BROKER_REPLICA_MOVEMENT} or
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
    filterOutBrokersExcludedForLeadership(eligibleBrokers, optimizationOptions, replica, action);
    filterOutBrokersExcludedForReplicaMove(eligibleBrokers, optimizationOptions, replica, action);
    if (!optimizationOptions.requestedDestinationBrokerIds().isEmpty()) {
      return eligibleBrokers;
    }

    if (clusterModel.newBrokers().isEmpty()) {
      return eligibleBrokers;
    }

    // When there are new brokers, we should only allow the replicas/leadership to be moved to the new brokers.
    return eligibleBrokers.stream().filter(b -> b.isNew() || b == replica.originalBroker()).collect(Collectors.toList());
  }

  /**
   * Check whether the proposed inter-broker action is legit. An action is legit if it is:
   * (1) a replica movement across brokers, the destination broker does not have a replica of the same partition and is
   * allowed to have a replica from the partition
   * (2) a leadership movement, the replica is a leader and the destination broker has a follower of the same partition.
   *
   * @param replica Replica that is affected from the given action type.
   * @param destinationBroker Destination broker.
   * @param clusterModel Cluster model.
   * @param actionType Action type.
   * @return True if the move is legit, false otherwise.
   */
  static boolean legitMove(Replica replica,
                           Broker destinationBroker,
                           ClusterModel clusterModel,
                           ActionType actionType) {
    switch (actionType) {
      case INTER_BROKER_REPLICA_MOVEMENT:
        return clusterModel.partition(replica.topicPartition()).canAssignReplicaToBroker(destinationBroker)
               && destinationBroker.replica(replica.topicPartition()) == null;
      case LEADERSHIP_MOVEMENT:
        return replica.isLeader() && destinationBroker.replica(replica.topicPartition()) != null;
      default:
        return false;
    }
  }

  /**
   * Check whether the proposed intra-broker action is legit. An action is legit if it is a replica movement across the
   * disks of the same broker, and the destination disk is alive.
   *
   * @param replica Replica that is affected from the given action type.
   * @param destinationDisk Destination disk.
   * @param actionType Action type.
   * @return True if the move is legit, false otherwise.
   */
  static boolean legitMoveBetweenDisks(Replica replica,
                                       Disk destinationDisk,
                                       ActionType actionType) {
    return actionType == ActionType.INTRA_BROKER_REPLICA_MOVEMENT
           && destinationDisk != null
           && destinationDisk.broker() == replica.broker()
           && destinationDisk.isAlive();
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
   * Checks the replicas that are supposed to be moved away from the dead brokers or broken disks have been moved.
   * If there are still replicas on the dead brokers or broken disks, throws an exception.
   * @param clusterModel the cluster model to check.
   * @param goalName Goal name for which the sanity check is executed.
   * @throws OptimizationFailureException when there are still replicas on the dead brokers or on broken disks.
   */
  public static void ensureNoOfflineReplicas(ClusterModel clusterModel, String goalName)
      throws OptimizationFailureException {
    // Sanity check: No self-healing eligible replica should remain at a decommissioned broker or on broken disk.
    for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
      if (replica.isCurrentOffline()) {
        throw new OptimizationFailureException(String.format(
            "[%s] Self healing failed to move the replica %s from %s broker %d (contains %d replicas).",
            goalName, replica, replica.broker().state(), replica.broker().id(), replica.broker().replicas().size()));
      }
    }
  }

  /**
   * Checks for the broker with broken disk, the partitions of the replicas used to be on its broken disk does not have
   * any replica on this broker.
   * @param clusterModel the cluster model to check.
   * @param goalName Goal name for which the sanity check is executed.
   * @throws OptimizationFailureException when there are replicas hosted by broker with broken disk which belongs to the
   * same partition as the replica used to be hosted on broken disks
   */
  public static void ensureReplicasMoveOffBrokersWithBadDisks(ClusterModel clusterModel, String goalName)
      throws OptimizationFailureException {
    for (Broker broker : clusterModel.brokersWithBadDisks()) {
      for (Replica replica : broker.replicas()) {
        if (!clusterModel.partition(replica.topicPartition()).canAssignReplicaToBroker(broker)) {
          throw new OptimizationFailureException(String.format(
              "[%s] A replica of partition %s has been moved back to broker %d, where it was originally hosted on a "
              + "broken disk.", goalName, clusterModel.partition(replica.topicPartition()), replica.broker().id()));
        }
      }
    }
  }

  /**
   * Get a filtered set of leaders from the given broker based on given filtering requirements.
   *
   * @param broker Broker whose replicas will be filters.
   * @param immigrantsOnly True if replicas should be filtered to ensure that they contain only the immigrants.
   * @return A filtered set of leaders from the given broker based on given filtering requirements.
   */
  private static Set<Replica> filterLeaders(Broker broker, boolean immigrantsOnly) {
    Set<Replica> filteredLeaders;
    if (immigrantsOnly) {
      filteredLeaders = new HashSet<>(broker.immigrantReplicas());
      filteredLeaders.removeIf(replica -> !replica.isLeader());
    } else {
      filteredLeaders = broker.leaderReplicas();
    }
    return filteredLeaders;
  }

  /**
   * Get a filtered set of replicas from the given broker based on given filtering requirements.
   *
   * @param broker Broker whose replicas will be filters.
   * @param followersOnly True if replicas should be filtered to ensure that they contain only the followers.
   * @param leadersOnly True if replicas should be filtered to ensure that they contain only the leaders.
   * @param immigrantsOnly True if replicas should be filtered to ensure that they contain only the immigrants.
   * @return A filtered set of replicas from the given broker.
   */
  public static Set<Replica> filterReplicas(Broker broker, boolean followersOnly, boolean leadersOnly, boolean immigrantsOnly) {
    if (leadersOnly) {
      // Get filtered leaders on the given broker.
      return followersOnly ? Collections.emptySet() : filterLeaders(broker, immigrantsOnly);
    }

    Set<Replica> filteredReplicas;
    if (followersOnly) {
      filteredReplicas = new HashSet<>(immigrantsOnly ? broker.immigrantReplicas() : broker.replicas());
      filteredReplicas.removeAll(broker.leaderReplicas());
    } else {
      filteredReplicas = immigrantsOnly ? broker.immigrantReplicas() : broker.replicas();
    }
    return filteredReplicas;
  }

  /**
   * Get the utilization percentage of the broker for the given resource, or {@link #DEAD_BROKER_UTILIZATION} if the
   * broker is dead. The utilization percentage for resources is calculated from broker capacity and
   * {@link com.linkedin.kafka.cruisecontrol.model.Load#expectedUtilizationFor(Resource)} .
   *
   * @param broker Broker for which the resource utilization percentage has been queried.
   * @param resource Resource for the utilization percentage.
   * @return Utilization percentage of the broker for the given resource.
   */
  public static double utilizationPercentage(Broker broker, Resource resource) {
    double brokerCapacity = broker.capacityFor(resource);
    return brokerCapacity > 0 ? broker.load().expectedUtilizationFor(resource) / brokerCapacity : DEAD_BROKER_UTILIZATION;
  }

  /**
   * Get the latest average utilization percentage of all the alive disks on the broker.
   *
   * @param broker Broker for which the average disk utilization percentage has been queried.
   * @return Latest average utilization percentage of all the alive disks on the broker.
   */
  public static double averageDiskUtilizationPercentage(Broker broker) {
    double totalAliveDiskCapacity = 0;
    double totalAliveDiskUtilization = 0;
    for (Disk disk : broker.disks()) {
      if (disk.isAlive()) {
        totalAliveDiskCapacity += disk.capacity();
        totalAliveDiskUtilization += disk.utilization();
      }
    }
    return totalAliveDiskCapacity > 0 ? totalAliveDiskUtilization / totalAliveDiskCapacity : DEAD_BROKER_UTILIZATION;
  }

  /**
   * Get the latest utilization percentage of the disk, or {@link #DEAD_DISK_UTILIZATION} if the disk is dead.
   *
   * @param disk Disk to query.
   * @return Latest utilization percentage of the disk.
   */
  public static double diskUtilizationPercentage(Disk disk) {
    double diskCapacity = disk.capacity();
    return diskCapacity > 0 ? disk.utilization() / diskCapacity : DEAD_DISK_UTILIZATION;
  }

  /**
   * Sort replicas in ascending order of resource quantity present in the broker that they reside in terms of the
   * requested resource.
   *
   * @param replicas A list of replicas to be sorted by the amount of resources that their broker contains.
   * @param resource Resource for which the given replicas will be sorted.
   */
  static void sortReplicasInAscendingOrderByBrokerResourceUtilization(List<Replica> replicas, Resource resource) {
    replicas.sort((r1, r2) -> {
      double expectedBrokerLoad1 = r1.broker().load().expectedUtilizationFor(resource);
      double expectedBrokerLoad2 = r2.broker().load().expectedUtilizationFor(resource);
      int result = Double.compare(expectedBrokerLoad1, expectedBrokerLoad2);
      return result == 0 ? Integer.compare(r1.broker().id(), r2.broker().id()) : result;
    });
  }
}
