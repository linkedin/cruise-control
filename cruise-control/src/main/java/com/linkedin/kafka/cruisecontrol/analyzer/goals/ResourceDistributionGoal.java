/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.CandidateBroker;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Load;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.INTER_BROKER_REPLICA_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.LEADERSHIP_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.utilizationPercentage;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.filterReplicas;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal.ChangeType.ADD;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal.ChangeType.REMOVE;


/**
 * SOFT GOAL: Balance {@link Resource} distribution over brokers.
 */
public abstract class ResourceDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  private static final long PER_BROKER_SWAP_TIMEOUT_MS = 1000L;
  // Flag to indicate whether the self healing failed to relocate all offline replicas away from dead brokers or broken
  // disks in its initial attempt and currently omitting the resource balance limit to relocate remaining replicas.
  private boolean _fixOfflineReplicasOnly;
  private double _balanceUpperThreshold;
  private double _balanceLowerThreshold;

  /**
   * Constructor for Resource Distribution Goal.
   */
  public ResourceDistributionGoal() {

  }

  /**
   * Package private for unit test.
   */
  ResourceDistributionGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
  }

  protected abstract Resource resource();

  /**
   * Check whether given action is acceptable by this goal. An action is acceptable by this goal if it satisfies the
   * following: (1) if both source and destination brokers were within the limit before the action, the corresponding
   * limits cannot be violated after the action, (2) otherwise, the action cannot increase the utilization difference
   * between brokers.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    Replica sourceReplica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());

    switch (action.balancingAction()) {
      case INTER_BROKER_REPLICA_SWAP:
        Replica destinationReplica = destinationBroker.replica(action.destinationTopicPartition());
        double sourceUtilizationDelta = destinationReplica.load().expectedUtilizationFor(resource())
                                        - sourceReplica.load().expectedUtilizationFor(resource());

        if (sourceUtilizationDelta == 0) {
          // No change in terms of load.
          return ACCEPT;
        }
        // Check if both the source and the destination broker are within the balance limit before applying a swap that
        // could potentially make a balanced broker unbalanced -- i.e. never make a balanced broker unbalanced.
        // Note that (1) if both source and destination brokers were within the limit before the swap, the corresponding
        // limits cannot be violated after the swap, (2) otherwise, the swap is guaranteed not to increase the utilization
        // difference between brokers.
        boolean bothBrokersCurrentlyWithinLimit = sourceUtilizationDelta > 0
                                                  ? (isLoadAboveBalanceLowerLimit(destinationBroker)
                                                     && isLoadUnderBalanceUpperLimit(sourceReplica.broker()))
                                                  : (isLoadAboveBalanceLowerLimit(sourceReplica.broker())
                                                     && isLoadUnderBalanceUpperLimit(destinationBroker));

        if (bothBrokersCurrentlyWithinLimit) {
          // Ensure that the resource utilization on balanced broker(s) do not go out of limits after the swap.
          return isSwapViolatingLimit(sourceReplica, destinationReplica) ? REPLICA_REJECT : ACCEPT;
        }
        // Ensure that the swap does not increase the utilization difference between brokers.
        return isSelfSatisfiedAfterSwap(sourceReplica, destinationReplica) ? ACCEPT : REPLICA_REJECT;
      case INTER_BROKER_REPLICA_MOVEMENT:
      case LEADERSHIP_MOVEMENT:
        if (isLoadAboveBalanceLowerLimit(sourceReplica.broker())
            && isLoadUnderBalanceUpperLimit(destinationBroker)) {
          // Already satisfied balance limits cannot be violated due to balancing action.
          return (isLoadUnderBalanceUpperLimitAfterChange(sourceReplica.load(), destinationBroker, ADD) &&
                  isLoadAboveBalanceLowerLimitAfterChange(sourceReplica.load(), sourceReplica.broker(), REMOVE))
                 ? ACCEPT : REPLICA_REJECT;
        }

        // Check that current destination would not become more unbalanced.
        return isAcceptableAfterReplicaMove(sourceReplica, destinationBroker) ? ACCEPT : REPLICA_REJECT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new ResourceDistributionGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(Math.max(1, _numWindows / 2), _minMonitoredPartitionPercentage, false);
  }

  /**
   * Get the name of this goal. Name of a goal provides an identification for the goal in human readable format.
   */
  @Override
  public abstract String name();

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
    return clusterModel.newBrokers().isEmpty() ? clusterModel.brokers() : clusterModel.newBrokers();
  }

  /**
   * Check if requirements of this goal are not violated if this action is applied to the given cluster state,
   * false otherwise. An action is acceptable if: (1) destination broker utilization for the given resource is less
   * than the source broker utilization. (2) movement is acceptable (i.e. under the broker balance limit for balanced
   * resources) for already balanced resources. Already balanced resources are the ones that have gone through the
   * "resource distribution" process specified in this goal.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model.
   * @return True if requirements of this goal are not violated if this action is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    Replica sourceReplica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
    // The action must be executed if currently fixing offline replicas only and the offline source replica is proposed
    // to be moved to another broker.
    if (_fixOfflineReplicasOnly && sourceReplica.broker().replica(action.topicPartition()).isCurrentOffline()) {
      return action.balancingAction() == ActionType.INTER_BROKER_REPLICA_MOVEMENT;
    }

    switch (action.balancingAction()) {
      case INTER_BROKER_REPLICA_SWAP:
        Replica destinationReplica = destinationBroker.replica(action.destinationTopicPartition());
        double sourceUtilizationDelta = destinationReplica.load().expectedUtilizationFor(resource())
                                        - sourceReplica.load().expectedUtilizationFor(resource());

        return sourceUtilizationDelta != 0 && !isSwapViolatingLimit(sourceReplica, destinationReplica);
      case INTER_BROKER_REPLICA_MOVEMENT:
      case LEADERSHIP_MOVEMENT:
        //Check that current destination would not become more unbalanced.
        return isLoadUnderBalanceUpperLimitAfterChange(sourceReplica.load(), destinationBroker, ADD) &&
               isLoadAboveBalanceLowerLimitAfterChange(sourceReplica.load(), sourceReplica.broker(), REMOVE);
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  /**
   * (1) Initialize the current resource to be balanced or self healed.
   * (2) Set the flag which indicates whether the self healing failed to relocate all replicas away from dead brokers
   * in its initial attempt. Since self healing has not been executed yet, this flag is false.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    _fixOfflineReplicasOnly = false;
    _balanceUpperThreshold = computeBalanceUpperThreshold(clusterModel, optimizationOptions);
    _balanceLowerThreshold = computeBalanceLowerThreshold(clusterModel, optimizationOptions);
    clusterModel.trackSortedReplicas(sortName(),
                                     optimizationOptions.onlyMoveImmigrantReplicas() ? ReplicaSortFunctionFactory.selectImmigrants()
                                                                                     : null,
                                     ReplicaSortFunctionFactory.deprioritizeOfflineReplicasThenImmigrants(),
                                     ReplicaSortFunctionFactory.sortByMetricGroupValue(resource().name()));
  }

  protected double balanceUpperThreshold() {
    return _balanceUpperThreshold;
  }

  protected double balanceLowerThreshold() {
    return _balanceLowerThreshold;
  }

  /**
   * Update the current resource that is being balanced if there are still resources to be balanced, finish otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization action.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws OptimizationFailureException {
    Set<Integer> brokerIdsAboveBalanceUpperLimit = new HashSet<>();
    Set<Integer> brokerIdsUnderBalanceLowerLimit = new HashSet<>();
    // Log broker Ids over balancing limit.
    // While proposals exclude the excludedTopics, the balance still considers utilization of the excludedTopic replicas.
    for (Broker broker : clusterModel.aliveBrokers()) {
      if (!isLoadUnderBalanceUpperLimit(broker)) {
        brokerIdsAboveBalanceUpperLimit.add(broker.id());
      }
      if (!isLoadAboveBalanceLowerLimit(broker)) {
        brokerIdsUnderBalanceLowerLimit.add(broker.id());
      }
    }
    if (!brokerIdsAboveBalanceUpperLimit.isEmpty()) {
      LOG.debug("Utilization for broker ids:{} {} above the balance limit for:{} after {}.",
                brokerIdsAboveBalanceUpperLimit, (brokerIdsAboveBalanceUpperLimit.size() > 1) ? "are" : "is", resource(),
                (clusterModel.selfHealingEligibleReplicas().isEmpty()) ? "rebalance" : "self-healing");
      _succeeded = false;
    }
    if (!brokerIdsUnderBalanceLowerLimit.isEmpty()) {
      LOG.debug("Utilization for broker ids:{} {} under the balance limit for:{} after {}.",
                brokerIdsUnderBalanceLowerLimit, (brokerIdsUnderBalanceLowerLimit.size() > 1) ? "are" : "is", resource(),
                (clusterModel.selfHealingEligibleReplicas().isEmpty()) ? "rebalance" : "self-healing");
      _succeeded = false;
    }
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    try {
      GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    } catch (OptimizationFailureException ofe) {
      if (_fixOfflineReplicasOnly) {
        clusterModel.untrackSortedReplicas(sortName());
        throw ofe;
      }
      _fixOfflineReplicasOnly = true;
      LOG.info("Ignoring resource balance limit to move replicas from dead brokers/disks.");
      return;
    }
    // Sanity check: No replica should be moved to a broker, which used to host any replica of the same partition on its broken disk.
    GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
    finish();
    clusterModel.untrackSortedReplicas(sortName());
  }

  @Override
  public void finish() {
    _finished = true;
  }

  /**
   * (1) REBALANCE BY LEADERSHIP MOVEMENT:
   * Perform leadership movement to ensure that the load on brokers for the outbound network and CPU load is under the
   * balance limit.
   * <p>
   * (2) REBALANCE BY REPLICA MOVEMENT:
   * Perform optimization via replica movement for the given resource (without breaking the balance for already
   * balanced resources) to ensure rebalance: The load on brokers for the given resource is under the balance limit.
   * <p>
   * (3) REBALANCE BY REPLICA SWAP:
   * Swap replicas to ensure balance without violating optimized goal requirements.
   *
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param optimizationOptions Options to take into account during optimization -- e.g. excluded topics.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) {
    int numOfflineReplicas = broker.currentOfflineReplicas().size();

    boolean requireLessLoad = numOfflineReplicas > 0 || !isLoadUnderBalanceUpperLimit(broker);
    boolean requireMoreLoad = !isLoadAboveBalanceLowerLimit(broker);
    // Cluster has offline replicas, but this broker does not -- move immigrant replicas only (relevant to replica move).
    boolean moveImmigrantsOnly = false;
    if (broker.currentOfflineReplicas().isEmpty()) {
      if (!requireMoreLoad && !requireLessLoad) {
        // return if the broker is already within limits.
        return;
      }
      moveImmigrantsOnly = !clusterModel.selfHealingEligibleReplicas().isEmpty() || optimizationOptions.onlyMoveImmigrantReplicas();
      if (moveImmigrantsOnly && requireLessLoad && broker.immigrantReplicas().isEmpty()) {
        // return if the cluster is in self-healing mode and the broker requires less load but does not have any
        // immigrant replicas.
        return;
      }
    }

    // First try leadership movement
    if ((resource() == Resource.NW_OUT || resource() == Resource.CPU)
        && !(_fixOfflineReplicasOnly && !broker.currentOfflineReplicas().isEmpty())) {
      if (requireLessLoad && !rebalanceByMovingLoadOut(broker, clusterModel, optimizedGoals,
                                                       LEADERSHIP_MOVEMENT, optimizationOptions)) {
        LOG.debug("Successfully balanced {} for broker {} by moving out leaders.", resource(), broker.id());
        requireLessLoad = false;
      }
      if (requireMoreLoad && !rebalanceByMovingLoadIn(broker, clusterModel, optimizedGoals,
                                                      LEADERSHIP_MOVEMENT, optimizationOptions, false)) {
        LOG.debug("Successfully balanced {} for broker {} by moving in leaders.", resource(), broker.id());
        requireMoreLoad = false;
      }
    }

    // Update broker ids over the balance limit for logging purposes.
    boolean unbalanced = false;
    if (requireLessLoad) {
      if (rebalanceByMovingLoadOut(broker, clusterModel, optimizedGoals, INTER_BROKER_REPLICA_MOVEMENT, optimizationOptions)) {
        unbalanced = rebalanceBySwappingLoadOut(broker, clusterModel, optimizedGoals, optimizationOptions, moveImmigrantsOnly);
      }
    }
    if (requireMoreLoad) {
      if (rebalanceByMovingLoadIn(broker, clusterModel, optimizedGoals, INTER_BROKER_REPLICA_MOVEMENT, optimizationOptions, moveImmigrantsOnly)) {
        unbalanced = unbalanced || rebalanceBySwappingLoadIn(broker, clusterModel, optimizedGoals, optimizationOptions, moveImmigrantsOnly);
      }
    }

    if (!unbalanced) {
      LOG.debug("Successfully balanced {} for broker {} by moving leaders and replicas.", resource(), broker.id());
    }
  }

  private boolean rebalanceByMovingLoadIn(Broker broker,
                                          ClusterModel clusterModel,
                                          Set<Goal> optimizedGoals,
                                          ActionType actionType,
                                          OptimizationOptions optimizationOptions,
                                          boolean moveImmigrantsOnly) {

    if (!clusterModel.newBrokers().isEmpty() && !broker.isNew()) {
      // We have new brokers and the current broker is not a new broker.
      return true;
    }

    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    Set<Integer> excludedBrokersForLeadership = optimizationOptions.excludedBrokersForLeadership();
    Set<Integer> excludedBrokersForReplicaMove = optimizationOptions.excludedBrokersForReplicaMove();
    // If this broker is excluded for leadership, then it can move in only followers.
    boolean moveFollowersOnly = excludedBrokersForLeadership.contains(broker.id());
    PriorityQueue<CandidateBroker> candidateBrokerPQ = new PriorityQueue<>();
    // Sort the replicas initially to avoid sorting it every time.

    double clusterUtilization = clusterModel.load().expectedUtilizationFor(resource()) / clusterModel.capacityFor(resource());
    for (Broker candidate : clusterModel.aliveBrokers()) {
      // Get candidate replicas on candidate broker to try moving load from -- sorted in the order of trial (descending load).
      if (utilizationPercentage(candidate, resource()) > clusterUtilization) {
        SortedSet<Replica> replicasToMoveIn = sortedCandidateReplicas(candidate,
                                                                      excludedTopics,
                                                                      0,
                                                                      false,
                                                                      moveFollowersOnly,
                                                                      moveImmigrantsOnly);
        CandidateBroker candidateBroker = new CandidateBroker(candidate,
                                                              resource(),
                                                              replicasToMoveIn,
                                                              false,
                                                              excludedBrokersForLeadership,
                                                              excludedBrokersForReplicaMove);
        candidateBrokerPQ.add(candidateBroker);
      }
    }

    // Stop when all the replicas are leaders for leader movement or there is no replicas can be moved in anymore
    // for replica movement.
    while (!candidateBrokerPQ.isEmpty() && (actionType == INTER_BROKER_REPLICA_MOVEMENT ||
        (actionType == LEADERSHIP_MOVEMENT && broker.leaderReplicas().size() != broker.replicas().size()))) {
      CandidateBroker cb = candidateBrokerPQ.poll();
      SortedSet<Replica> candidateReplicasToReceive = cb.replicas();

      for (Iterator<Replica> iterator = candidateReplicasToReceive.iterator(); iterator.hasNext(); ) {
        Replica replica = iterator.next();
        Broker b = maybeApplyBalancingAction(clusterModel, replica, Collections.singletonList(broker), actionType,
                                             optimizedGoals, optimizationOptions);

        // Only need to check status if the action is taken. This will also handle the case that the source broker
        // has nothing to move in. In that case we will never reenqueue that source broker.
        if (b != null) {
          if (isLoadAboveBalanceLowerLimit(broker)) {
            return false;
          }
          // Remove the replica from its source broker if it was a replica movement.
          if (actionType == INTER_BROKER_REPLICA_MOVEMENT) {
            iterator.remove();
          }

          // If the source broker has a lower utilization than the next broker in the eligible broker in the queue,
          // we reenqueue the source broker and switch to the next broker.
          if (!candidateBrokerPQ.isEmpty()
              && utilizationPercentage(cb.broker(), resource()) < utilizationPercentage(candidateBrokerPQ.peek().broker(), resource())) {
            candidateBrokerPQ.add(cb);
            break;
          }
        }
      }
    }
    return true;
  }

  /**
   * Get the sorted replicas in the given broker whose (1) topic is not an excluded topic AND (2) do not violate the given load
   * limit in ascending or descending order. Load limit requires the replica load to be (1) above the given limit in
   * descending order, (2) below the given limit in ascending order. Offline replicas have priority over online replicas.
   *
   * @param broker Broker whose replicas will be considered.
   * @param excludedTopics Excluded topics for which the replicas will be remove from the returned candidate replicas.
   * @param loadLimit Load limit determining the lower cutoff in descending order, upper cutoff in ascending order.
   * @param isAscending True if sort requested in ascending order, false otherwise.
   * @param followersOnly Candidate replicas contain only the followers.
   * @param immigrantsOnly Candidate replicas contain only the immigrants.
   * @return Sorted replicas in the given broker whose (1) topic is not an excluded topic AND (2) do not violate the
   * given load limit in ascending or descending order.
   */
  private SortedSet<Replica> sortedCandidateReplicas(Broker broker,
                                                     Set<String> excludedTopics,
                                                     double loadLimit,
                                                     boolean isAscending,
                                                     boolean followersOnly,
                                                     boolean immigrantsOnly) {
    SortedSet<Replica> candidateReplicas = new TreeSet<>((r1, r2) -> {
      // Primary sort: by offline / online status
      boolean isR1Offline = r1.isCurrentOffline();
      boolean isR2Offline = r2.isCurrentOffline();

      if (isR1Offline && !isR2Offline) {
        return -1;
      } else if (!isR1Offline && isR2Offline) {
        return 1;
      }
      int result = isAscending
                   ? Double.compare(r1.load().expectedUtilizationFor(resource()), r2.load().expectedUtilizationFor(resource()))
                   : Double.compare(r2.load().expectedUtilizationFor(resource()), r1.load().expectedUtilizationFor(resource()));
      return result == 0 ? r1.topicPartition().toString().compareTo(r2.topicPartition().toString()) : result;
    });

    Set<Replica> filteredReplicas = filterReplicas(broker, followersOnly, resource() == Resource.NW_OUT, immigrantsOnly);
    // The given load limit determines the lower cutoff in descending order, upper cutoff in ascending order.
    if (isAscending) {
      candidateReplicas.addAll(filteredReplicas.stream()
          .filter(r -> !shouldExclude(r, excludedTopics) && r.load().expectedUtilizationFor(resource()) < loadLimit)
          .collect(Collectors.toSet()));
    } else {
      candidateReplicas.addAll(filteredReplicas.stream()
          .filter(r -> !shouldExclude(r, excludedTopics) && r.load().expectedUtilizationFor(resource()) > loadLimit)
          .collect(Collectors.toSet()));
    }

    return candidateReplicas;
  }

  private double getMaxReplicaLoad(SortedSet<Replica> sortedReplicas) {
    // Compare the load of the first replica with the first online replica.
    double maxReplicaLoad = sortedReplicas.first().load().expectedUtilizationFor(resource());
    for (Replica replica : sortedReplicas) {
      if (replica.isCurrentOffline()) {
        continue;
      } else if (replica.load().expectedUtilizationFor(resource()) > maxReplicaLoad) {
        maxReplicaLoad = replica.load().expectedUtilizationFor(resource());
      }
      break;
    }
    return maxReplicaLoad;
  }

  private boolean rebalanceBySwappingLoadOut(Broker broker,
                                             ClusterModel clusterModel,
                                             Set<Goal> optimizedGoals,
                                             OptimizationOptions optimizationOptions,
                                             boolean moveImmigrantsOnly) {
    long swapStartTimeMs = System.currentTimeMillis();
    if (!broker.isAlive() || optimizationOptions.excludedBrokersForReplicaMove().contains(broker.id())) {
      // If the source broker is (1) dead, or (2) excluded for replica move, then swap operation is not possible.
      return true;
    }

    // Get the replicas to rebalance.
    SortedSet<Replica> sourceReplicas = new TreeSet<>((r1, r2) -> {
      // Primary sort: by offline / online status
      boolean isR1Offline = r1.isCurrentOffline();
      boolean isR2Offline = r2.isCurrentOffline();

      if (isR1Offline && !isR2Offline) {
        return -1;
      } else if (!isR1Offline && isR2Offline) {
        return 1;
      }
      int result = Double.compare(r2.load().expectedUtilizationFor(resource()), r1.load().expectedUtilizationFor(resource()));
      return result == 0 ? r1.topicPartition().toString().compareTo(r2.topicPartition().toString()) : result;
    });

    sourceReplicas.addAll(filterReplicas(broker, false, resource() == Resource.NW_OUT, moveImmigrantsOnly));
    if (sourceReplicas.isEmpty()) {
      // Source broker has no filtered replica to swap.
      return true;
    }
    double maxSourceReplicaLoad = getMaxReplicaLoad(sourceReplicas);

    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    Set<Integer> excludedBrokersForLeadership = optimizationOptions.excludedBrokersForLeadership();
    Set<Integer> excludedBrokersForReplicaMove = optimizationOptions.excludedBrokersForReplicaMove();
    // Sort the replicas initially to avoid sorting it every time.
    // If this broker is excluded for leadership, then it can swapped with only followers.
    boolean swapWithFollowersOnly = excludedBrokersForLeadership.contains(broker.id());
    PriorityQueue<CandidateBroker> candidateBrokerPQ = new PriorityQueue<>();
    for (Broker candidate : clusterModel.aliveBrokersUnderThreshold(resource(), _balanceUpperThreshold)
                                        .stream().filter(b -> !b.replicas().isEmpty()).collect(Collectors.toSet())) {
      // Get candidate replicas on candidate broker to try swapping with -- sorted in the order of trial (ascending load).
      SortedSet<Replica> replicasToSwapWith = sortedCandidateReplicas(candidate,
                                                                      excludedTopics,
                                                                      maxSourceReplicaLoad,
                                                                      true,
                                                                      swapWithFollowersOnly,
                                                                      moveImmigrantsOnly);
      CandidateBroker candidateBroker = new CandidateBroker(candidate,
                                                            resource(),
                                                            replicasToSwapWith,
                                                            true,
                                                            excludedBrokersForLeadership,
                                                            excludedBrokersForReplicaMove);
      candidateBrokerPQ.add(candidateBroker);
    }

    while (!candidateBrokerPQ.isEmpty()) {
      if (remainingPerBrokerSwapTimeMs(swapStartTimeMs) <= 0) {
        LOG.debug("Swap load out timeout for broker {}.", broker.id());
        break;
      }

      CandidateBroker cb = candidateBrokerPQ.poll();
      SortedSet<Replica> candidateReplicasToSwapWith = cb.replicas();

      Replica swappedInReplica = null;
      Replica swappedOutReplica = null;
      for (Replica sourceReplica : sourceReplicas) {
        if (shouldExclude(sourceReplica, excludedTopics)) {
          continue;
        }

        // Try swapping the source with the candidate replicas. Get the swapped in replica if successful, null otherwise.
        Replica swappedIn = maybeApplySwapAction(clusterModel, sourceReplica, cb, optimizedGoals);
        if (swappedIn != null) {
          if (isLoadUnderBalanceUpperLimit(broker)) {
            // Successfully balanced this broker by swapping in.
            return false;
          }
          // Add swapped in/out replica for updating the list of replicas in source broker.
          swappedInReplica = swappedIn;
          swappedOutReplica = sourceReplica;
          break;
        } else if (remainingPerBrokerSwapTimeMs(swapStartTimeMs) <= 0) {
          LOG.debug("Swap load out timeout for source replica {}.", sourceReplica);
          return true;
        }
      }

      swapUpdate(swappedInReplica, swappedOutReplica, sourceReplicas, candidateReplicasToSwapWith, candidateBrokerPQ, cb);
    }

    return true;
  }

  /**
   * Get the remaining per broker swap time in milliseconds based on the given swap start time.
   *
   * @param swapStartTimeMs Per broker swap start time in milliseconds.
   * @return Remaining per broker swap time in milliseconds.
   */
  private long remainingPerBrokerSwapTimeMs(long swapStartTimeMs) {
    return PER_BROKER_SWAP_TIMEOUT_MS - (System.currentTimeMillis() - swapStartTimeMs);
  }

  /**
   * If the swap was successful, update the (1) replicas of the source broker (2) replicas of the candidate broker, and
   * (3) the priority queue of candidate brokers to swap replicas with.
   *
   * @param swappedInReplica the replica swapped in to the source broker from the candidate broker, null otherwise.
   * @param swappedOutReplica the replica swapped out from the source broker to candidate broker, null otherwise.
   * @param sourceReplicas replicas of the source broker before the swap.
   * @param candidateReplicasToSwapWith replicas of the candidate broker before the swap.
   * @param candidateBrokerPQ a priority queue of candidate brokers to swap replicas with (priority: by broker utilization)
   * @param cb the candidate broker to swap with.
   */
  private void swapUpdate(Replica swappedInReplica,
                          Replica swappedOutReplica,
                          SortedSet<Replica> sourceReplicas,
                          SortedSet<Replica> candidateReplicasToSwapWith,
                          PriorityQueue<CandidateBroker> candidateBrokerPQ,
                          CandidateBroker cb) {
    if (swappedInReplica != null) {
      // Update the list of replicas in source broker after the swap operations.
      sourceReplicas.remove(swappedOutReplica);
      sourceReplicas.add(swappedInReplica);
      candidateReplicasToSwapWith.remove(swappedInReplica);
      candidateReplicasToSwapWith.add(swappedOutReplica);

      // The broker is still considered as an eligible candidate replica, because the swap was successful -- i.e. there
      // might be other potential candidate replicas on it to swap with.
      candidateBrokerPQ.add(cb);
    }
  }

  private boolean rebalanceBySwappingLoadIn(Broker broker,
                                            ClusterModel clusterModel,
                                            Set<Goal> optimizedGoals,
                                            OptimizationOptions optimizationOptions,
                                            boolean moveImmigrantsOnly) {
    long swapStartTimeMs = System.currentTimeMillis();
    if (!broker.isAlive() || optimizationOptions.excludedBrokersForReplicaMove().contains(broker.id())) {
      // If the source broker is (1) dead, or (2) excluded for replica move, then swap operation is not possible.
      return true;
    }

    // Get the replicas to rebalance.
    SortedSet<Replica> sourceReplicas = new TreeSet<>((r1, r2) -> {
      // Primary sort: by offline / online status
      boolean isR1Offline = r1.isCurrentOffline();
      boolean isR2Offline = r2.isCurrentOffline();

      if (isR1Offline && !isR2Offline) {
        return -1;
      } else if (!isR1Offline && isR2Offline) {
        return 1;
      }
      int result = Double.compare(r1.load().expectedUtilizationFor(resource()), r2.load().expectedUtilizationFor(resource()));
      return result == 0 ? r1.topicPartition().toString().compareTo(r2.topicPartition().toString()) : result;
    });

    sourceReplicas.addAll(filterReplicas(broker, false, false, moveImmigrantsOnly));
    if (sourceReplicas.isEmpty()) {
      // Source broker has no filtered replica to swap.
      return true;
    }

    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    Set<Integer> excludedBrokersForLeadership = optimizationOptions.excludedBrokersForLeadership();
    Set<Integer> excludedBrokersForReplicaMove = optimizationOptions.excludedBrokersForReplicaMove();
    // Sort the replicas initially to avoid sorting it every time.
    // If this broker is excluded for leadership, then it can swapped with only followers.
    boolean swapWithFollowersOnly = excludedBrokersForLeadership.contains(broker.id());
    PriorityQueue<CandidateBroker> candidateBrokerPQ = new PriorityQueue<>();
    for (Broker candidate : clusterModel.aliveBrokersOverThreshold(resource(), _balanceLowerThreshold)) {
      // Get candidate replicas on candidate broker to try swapping with -- sorted in the order of trial (descending load).
      double minSourceReplicaLoad = sourceReplicas.first().load().expectedUtilizationFor(resource());
      SortedSet<Replica> replicasToSwapWith = sortedCandidateReplicas(candidate,
                                                                      excludedTopics,
                                                                      minSourceReplicaLoad,
                                                                      false,
                                                                      swapWithFollowersOnly,
                                                                      moveImmigrantsOnly);
      CandidateBroker candidateBroker = new CandidateBroker(candidate,
                                                            resource(),
                                                            replicasToSwapWith,
                                                            false,
                                                            excludedBrokersForLeadership,
                                                            excludedBrokersForReplicaMove);
      candidateBrokerPQ.add(candidateBroker);
    }

    while (!candidateBrokerPQ.isEmpty()) {
      if (remainingPerBrokerSwapTimeMs(swapStartTimeMs) <= 0) {
        LOG.debug("Swap load in timeout for broker {}.", broker.id());
        break;
      }
      CandidateBroker cb = candidateBrokerPQ.poll();
      SortedSet<Replica> candidateReplicasToSwapWith = cb.replicas();

      Replica swappedInReplica = null;
      Replica swappedOutReplica = null;
      for (Replica sourceReplica : sourceReplicas) {
        if (shouldExclude(sourceReplica, excludedTopics)) {
          continue;
        }
        // It does not make sense to swap replicas without utilization from a live broker.
        double sourceReplicaUtilization = sourceReplica.load().expectedUtilizationFor(resource());
        if (sourceReplicaUtilization == 0.0) {
          break;
        }
        // Try swapping the source with the candidate replicas. Get the swapped in replica if successful, null otherwise.
        Replica swappedIn = maybeApplySwapAction(clusterModel, sourceReplica, cb, optimizedGoals);
        if (swappedIn != null) {
          if (isLoadAboveBalanceLowerLimit(broker)) {
            // Successfully balanced this broker by swapping in.
            return false;
          }
          // Add swapped in/out replica for updating the list of replicas in source broker.
          swappedInReplica = swappedIn;
          swappedOutReplica = sourceReplica;
          break;
        } else if (remainingPerBrokerSwapTimeMs(swapStartTimeMs) <= 0) {
          LOG.debug("Swap load in timeout for source replica {}.", sourceReplica);
          return true;
        }
      }

      swapUpdate(swappedInReplica, swappedOutReplica, sourceReplicas, candidateReplicasToSwapWith, candidateBrokerPQ, cb);
    }

    return true;
  }

  private boolean rebalanceByMovingLoadOut(Broker broker,
                                           ClusterModel clusterModel,
                                           Set<Goal> optimizedGoals,
                                           ActionType actionType,
                                           OptimizationOptions optimizationOptions) {
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    // Get the eligible brokers.
    SortedSet<Broker> candidateBrokers = new TreeSet<>(
        Comparator.comparingDouble((Broker b) -> utilizationPercentage(b, resource())).thenComparingInt(Broker::id));
    if (_fixOfflineReplicasOnly) {
      candidateBrokers.addAll(clusterModel.aliveBrokers());
    } else {
      candidateBrokers.addAll(clusterModel.aliveBrokersUnderThreshold(resource(), _balanceUpperThreshold));
    }

    // Get the replicas to rebalance.
    List<Replica> replicasToMove;
    if (actionType == LEADERSHIP_MOVEMENT) {
      // Only take leader replicas to move leaders.
      replicasToMove = new ArrayList<>(broker.leaderReplicas());
      replicasToMove.sort((r1, r2) -> Double.compare(r2.load().expectedUtilizationFor(resource()),
                                                     r1.load().expectedUtilizationFor(resource())));
    } else {
      // Take all replicas for replica movements.
      replicasToMove = broker.trackedSortedReplicas(sortName()).reverselySortedReplicas();
    }

    // Now let's move things around.
    for (Replica replica : replicasToMove) {
      if (shouldExclude(replica, excludedTopics)) {
        continue;
      }
      // It does not make sense to move an online replica without utilization from a live broker.
      if (replica.load().expectedUtilizationFor(resource()) == 0.0 && !replica.isCurrentOffline()) {
        break;
      }

      // An optimization for leader movements.
      SortedSet<Broker> eligibleBrokers;
      if (actionType == LEADERSHIP_MOVEMENT) {
        eligibleBrokers = new TreeSet<>(Comparator.comparingDouble((Broker b) -> utilizationPercentage(b, resource()))
                                                  .thenComparingInt(Broker::id));
        clusterModel.partition(replica.topicPartition()).onlineFollowerBrokers().forEach(b -> {
          if (candidateBrokers.contains(b)) {
            eligibleBrokers.add(b);
          }
        });
      } else {
        eligibleBrokers = candidateBrokers;
      }

      Broker b = maybeApplyBalancingAction(clusterModel, replica, eligibleBrokers, actionType, optimizedGoals, optimizationOptions);
      // Only check if we successfully moved something.
      if (b != null) {
        if (isLoadUnderBalanceUpperLimit(broker) && !(_fixOfflineReplicasOnly && !broker.currentOfflineReplicas().isEmpty())) {
          return false;
        }
        // Remove and reinsert the broker so the order is correct.
        candidateBrokers.remove(b);
        if (utilizationPercentage(b, resource()) < _balanceUpperThreshold) {
          candidateBrokers.add(b);
        }
      }
    }
    // If all the replicas has been moved away from the broker and we still reach here, that means the broker
    // capacity is negative, i.e. the broker is dead. So as long as there is no replicas on the broker anymore
    // we consider it as not over limit.
    return !broker.replicas().isEmpty();
  }

  private boolean isLoadAboveBalanceLowerLimit(Broker broker) {
    // The action does not matter here because the load is null.
    return isLoadAboveBalanceLowerLimitAfterChange(null, broker, ADD);
  }

  private boolean isLoadUnderBalanceUpperLimit(Broker broker) {
    // The action does not matter here because the load is null.
    return isLoadUnderBalanceUpperLimitAfterChange(null, broker, REMOVE);
  }

  private boolean isLoadAboveBalanceLowerLimitAfterChange(Load load,
                                                          Broker broker,
                                                          ChangeType changeType) {
    double utilizationDelta = load == null ? 0 : load.expectedUtilizationFor(resource());

    double brokerBalanceLowerLimit = broker.capacityFor(resource()) * _balanceLowerThreshold;
    double brokerUtilization = broker.load().expectedUtilizationFor(resource());
    boolean isBrokerAboveLowerLimit = changeType == ADD ? brokerUtilization + utilizationDelta >= brokerBalanceLowerLimit :
                                      brokerUtilization - utilizationDelta >= brokerBalanceLowerLimit;

    if (resource().isHostResource()) {
      double hostBalanceLowerLimit = broker.host().capacityFor(resource()) * _balanceLowerThreshold;
      double hostUtilization = broker.host().load().expectedUtilizationFor(resource());
      boolean isHostAboveLowerLimit = changeType == ADD ? hostUtilization + utilizationDelta >= hostBalanceLowerLimit :
                                      hostUtilization - utilizationDelta >= hostBalanceLowerLimit;
      // As long as either the host or the broker is above the limit, we claim the host resource utilization is
      // above the limit. If the host is below limit, there must be at least one broker below limit. We should just
      // bring more load to that broker.
      return isHostAboveLowerLimit || isBrokerAboveLowerLimit;
    } else {
      return isBrokerAboveLowerLimit;
    }
  }

  private boolean isLoadUnderBalanceUpperLimitAfterChange(Load load,
                                                          Broker broker,
                                                          ChangeType changeType) {
    double utilizationDelta = load == null ? 0 : load.expectedUtilizationFor(resource());

    double brokerBalanceUpperLimit = broker.capacityFor(resource()) * _balanceUpperThreshold;
    double brokerUtilization = broker.load().expectedUtilizationFor(resource());
    boolean isBrokerUnderUpperLimit = changeType == ADD ? brokerUtilization + utilizationDelta <= brokerBalanceUpperLimit :
                                      brokerUtilization - utilizationDelta <= brokerBalanceUpperLimit;

    if (resource().isHostResource()) {
      double hostBalanceUpperLimit = broker.host().capacityFor(resource()) * _balanceUpperThreshold;
      double hostUtilization = broker.host().load().expectedUtilizationFor(resource());
      boolean isHostUnderUpperLimit = changeType == ADD ? hostUtilization + utilizationDelta <= hostBalanceUpperLimit :
                                      hostUtilization - utilizationDelta <= hostBalanceUpperLimit;
      // As long as either the host or the broker is under the limit, we claim the host resource utilization is
      // under the limit. If the host is above limit, there must be at least one broker above limit. We should just
      // move load off that broker.
      return isHostUnderUpperLimit || isBrokerUnderUpperLimit;
    } else {
      return isBrokerUnderUpperLimit;
    }
  }

  /**
   * Check if moving the source replica to destination broker would decrease the utilization difference between source
   * and destination brokers.
   *
   * @param sourceReplica Source replica to be moved.
   * @param destinationBroker Destination broker to receive the source replica utilization.
   * @return True if the change would lead to a better balance, false otherwise.
   */
  private boolean isAcceptableAfterReplicaMove(Replica sourceReplica, Broker destinationBroker) {
    double sourceUtilizationDelta = - sourceReplica.load().expectedUtilizationFor(resource());

    double destinationBrokerUtilization = destinationBroker.load().expectedUtilizationFor(resource());
    return isGettingMoreBalanced(sourceReplica, sourceUtilizationDelta, destinationBrokerUtilization);
  }

  /**
   * Check if the swap would decrease the utilization difference between source and destination brokers.
   *
   * @param sourceReplica Source replica to be swapped.
   * @param destinationReplica Destination replica to be swapped.
   * @return True if the change would lead to a better balance, false otherwise.
   */
  private boolean isSelfSatisfiedAfterSwap(Replica sourceReplica, Replica destinationReplica) {
    double sourceUtilizationDelta = destinationReplica.load().expectedUtilizationFor(resource())
                                    - sourceReplica.load().expectedUtilizationFor(resource());

    double destinationBrokerUtilization = destinationReplica.broker().load().expectedUtilizationFor(resource());
    return isGettingMoreBalanced(sourceReplica, sourceUtilizationDelta, destinationBrokerUtilization);
  }

  /**
   * Check if the utilization difference between source and destination brokers would decrease after the source
   * utilization delta is removed from the destination and added to source broker.
   *
   * @param sourceReplica Source replica.
   * @param sourceUtilizationDelta Utilization that would be removed from the destination and added to source broker.
   * @param destinationBrokerUtilization The utilization of destination broker.
   * @return True if the change would lead to a better balance, false otherwise.
   */
  private boolean isGettingMoreBalanced(Replica sourceReplica, double sourceUtilizationDelta, double destinationBrokerUtilization) {
    double sourceBrokerUtilization = sourceReplica.broker().load().expectedUtilizationFor(resource());

    double prevDiff = sourceBrokerUtilization - destinationBrokerUtilization;
    double nextDiff = prevDiff + (2 * sourceUtilizationDelta);

    return Math.abs(nextDiff) < Math.abs(prevDiff);
  }

  private boolean isSwapViolatingLimit(Replica sourceReplica, Replica destinationReplica) {
    double sourceUtilizationDelta = destinationReplica.load().expectedUtilizationFor(resource())
                                    - sourceReplica.load().expectedUtilizationFor(resource());
    // Check: Broker resource load within balance limit check.
    boolean swapViolatingBrokerLimit = isSwapViolatingContainerLimit(sourceUtilizationDelta,
                                                                     sourceReplica,
                                                                     destinationReplica,
                                                                     r -> r.broker().load(),
                                                                     r -> r.broker().capacityFor(resource()));

    if (!swapViolatingBrokerLimit || !resource().isHostResource()) {
      return swapViolatingBrokerLimit;
    }

    // Check: Host resource load within balance limit check.
    return isSwapViolatingContainerLimit(sourceUtilizationDelta,
                                         sourceReplica,
                                         destinationReplica,
                                         r -> r.broker().host().load(),
                                         r -> r.broker().host().capacityFor(resource()));
  }

  private boolean isSwapViolatingContainerLimit(double sourceUtilizationDelta,
                                                Replica sourceReplica,
                                                Replica destinationReplica,
                                                Function<Replica, Load> loadFunction,
                                                Function<Replica, Double> capacityFunction) {
    // Container could be host or broker.
    double sourceContainerUtilization = loadFunction.apply(sourceReplica).expectedUtilizationFor(resource());
    double destinationContainerUtilization = loadFunction.apply(destinationReplica).expectedUtilizationFor(resource());

    // 1. Container under balance upper limit check.
    boolean isContainerUnderUpperLimit;
    if (sourceUtilizationDelta > 0) {
      double sourceContainerBalanceUpperLimit = capacityFunction.apply(sourceReplica) * _balanceUpperThreshold;
      isContainerUnderUpperLimit = sourceContainerUtilization + sourceUtilizationDelta <= sourceContainerBalanceUpperLimit;
    } else {
      double destinationContainerBalanceUpperLimit = capacityFunction.apply(destinationReplica) * _balanceUpperThreshold;
      isContainerUnderUpperLimit = destinationContainerUtilization - sourceUtilizationDelta <= destinationContainerBalanceUpperLimit;
    }

    if (!isContainerUnderUpperLimit) {
      return true;
    }
    // 2. Container above balance lower limit check.
    boolean isContainerAboveLowerLimit;
    if (sourceUtilizationDelta < 0) {
      double sourceContainerBalanceLowerLimit = capacityFunction.apply(sourceReplica) * _balanceLowerThreshold;
      isContainerAboveLowerLimit = sourceContainerUtilization + sourceUtilizationDelta >= sourceContainerBalanceLowerLimit;
    } else {
      double destinationContainerBalanceLowerLimit = capacityFunction.apply(destinationReplica) * _balanceLowerThreshold;
      isContainerAboveLowerLimit = destinationContainerUtilization - sourceUtilizationDelta >= destinationContainerBalanceLowerLimit;
    }

    return !isContainerAboveLowerLimit;
  }

  /**
   * @param clusterModel the cluster topology and load.
   * @param optimizationOptions Options to adjust balance upper limit in case goal optimization is triggered by goal
   * violation detector.
   * @return the utilization upper threshold in percent for the {@link #resource()}
   */
  private double computeBalanceUpperThreshold(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    return (clusterModel.load().expectedUtilizationFor(resource()) / clusterModel.capacityFor(resource()))
           * (1 + balancePercentageWithMargin(optimizationOptions));
  }

  /**
   * @param clusterModel the cluster topology and load.
   * @param optimizationOptions Options to adjust balance lower limit in case goal optimization is triggered by goal
   * violation detector.
   * @return the utilization lower threshold in percent for the {@link #resource()}
   */
  private double computeBalanceLowerThreshold(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    return (clusterModel.load().expectedUtilizationFor(resource()) / clusterModel.capacityFor(resource()))
           * Math.max(0, (1 - balancePercentageWithMargin(optimizationOptions)));
  }

  /**
   * To avoid churns, we add a balance margin to the user specified rebalance threshold. e.g. when user sets the
   * threshold to be resourceBalancePercentage, we use (resourceBalancePercentage-1)*balanceMargin instead.
   *
   * @param optimizationOptions Options to adjust balance percentage with margin in case goal optimization is triggered
   * by goal violation detector.
   * @return the rebalance threshold with a margin.
   */
  private double balancePercentageWithMargin(OptimizationOptions optimizationOptions) {
    double balancePercentage = optimizationOptions.isTriggeredByGoalViolation()
                               ? _balancingConstraint.resourceBalancePercentage(resource())
                                 * _balancingConstraint.goalViolationDistributionThresholdMultiplier()
                               : _balancingConstraint.resourceBalancePercentage(resource());

    return (balancePercentage - 1) * BALANCE_MARGIN;
  }

  private String sortName() {
    return name() + "-" + resource().name() + "-ALL";
  }

  private class ResourceDistributionGoalStatsComparator implements ClusterModelStatsComparator {
    private String _reasonForLastNegativeResult;

    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // Number of balanced brokers in the highest priority resource cannot be more than the pre-optimized
      // stats. This constraint is applicable for the rest of the resources, if their higher priority resources
      // have the same number of balanced brokers in their corresponding pre- and post-optimized stats.
        int numBalancedBroker1 = stats1.numBalancedBrokersByResource().get(resource());
        int numBalancedBroker2 = stats2.numBalancedBrokersByResource().get(resource());
        // First compare the number of balanced brokers
        if (numBalancedBroker2 > numBalancedBroker1) {
          // If the number of balanced brokers has increased, the standard deviation of utilization for the resource
          // must decrease. Otherwise, the goal is producing a worse cluster state.
          double afterUtilizationStd = stats1.resourceUtilizationStats().get(Statistic.ST_DEV).get(resource());
          double beforeUtilizationStd = stats2.resourceUtilizationStats().get(Statistic.ST_DEV).get(resource());
          if (Double.compare(beforeUtilizationStd, afterUtilizationStd) < 0) {
            _reasonForLastNegativeResult = String.format(
                "Violated %s. [Number of Balanced Brokers] for resource %s. post-optimization:%d pre-optimization:%d "
                + "without improving the standard dev. of utilization. post-optimization:%.2f pre-optimization:%.2f",
                name(), resource(), numBalancedBroker1, numBalancedBroker2, afterUtilizationStd, beforeUtilizationStd);
            return -1;
          }
        }
      return 1;
    }

    @Override
    public String explainLastComparison() {
      return _reasonForLastNegativeResult;
    }
  }

  /**
   * Whether bring load in or bring load out.
   */
  protected enum ChangeType {
    ADD, REMOVE
  }
}
