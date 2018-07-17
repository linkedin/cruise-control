/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

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
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.REPLICA_SWAP;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.REPLICA_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.LEADERSHIP_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal.ChangeType.ADD;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal.ChangeType.REMOVE;


/**
 * Class for achieving the following soft goal:
 * <p>
 * SOFT GOAL#3: Balance resource distribution over brokers (e.g. cpu, disk, inbound / outbound network traffic).
 */
public abstract class ResourceDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  // Flag to indicate whether the self healing failed to relocate all replicas away from dead brokers in its initial
  // attempt and currently omitting the resource balance limit to relocate remaining replicas.
  private boolean _selfHealingDeadBrokersOnly;
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
   * @deprecated
   * Please use {@link #actionAcceptance(BalancingAction, ClusterModel)} instead.
   */
  @Override
  public boolean isActionAcceptable(BalancingAction action, ClusterModel clusterModel) {
    return actionAcceptance(action, clusterModel) == ACCEPT;
  }

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
      case REPLICA_SWAP:
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
      case REPLICA_MOVEMENT:
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
    return new ModelCompletenessRequirements(_numWindows, _minMonitoredPartitionPercentage, false);
  }

  /**
   * Get the name of this goal. Name of a goal provides an identification for the goal in human readable format.
   */
  @Override
  public abstract String name();

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
    // If the source broker is dead and currently self healing dead brokers only, unless it is replica swap, the action
    // must be executed.
    if (!sourceReplica.broker().isAlive() && _selfHealingDeadBrokersOnly) {
      return action.balancingAction() != REPLICA_SWAP;
    }

    switch (action.balancingAction()) {
      case REPLICA_SWAP:
        Replica destinationReplica = destinationBroker.replica(action.destinationTopicPartition());
        double sourceUtilizationDelta = destinationReplica.load().expectedUtilizationFor(resource())
                                        - sourceReplica.load().expectedUtilizationFor(resource());

        return sourceUtilizationDelta != 0 && !isSwapViolatingLimit(sourceReplica, destinationReplica);
      case REPLICA_MOVEMENT:
      case LEADERSHIP_MOVEMENT:
        //Check that current destination would not become more unbalanced.
        return isLoadUnderBalanceUpperLimitAfterChange(sourceReplica.load(), destinationBroker, ADD) &&
               isLoadAboveBalanceLowerLimitAfterChange(sourceReplica.load(), sourceReplica.broker(), REMOVE);
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  /**
   * (1) Initialize the current resource to be balanced or self healed. resource selection is based on the order of
   * priority set in the _balancingConstraint.
   * (2) Set the flag which indicates whether the self healing failed to relocate all replicas away from dead brokers
   * in its initial attempt. Since self healing has not been executed yet, this flag is false.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposals.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, Set<String> excludedTopics) {
    _selfHealingDeadBrokersOnly = false;
    _balanceUpperThreshold = computeBalanceUpperThreshold(clusterModel);
    _balanceLowerThreshold = computeBalanceLowerThreshold(clusterModel);
    clusterModel.trackSortedReplicas(sortName(),
                                     ReplicaSortFunctionFactory.deprioritizeImmigrants(),
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
    for (Broker broker : clusterModel.healthyBrokers()) {
      if (!isLoadUnderBalanceUpperLimit(broker)) {
        brokerIdsAboveBalanceUpperLimit.add(broker.id());
      }
      if (!isLoadAboveBalanceLowerLimit(broker)) {
        brokerIdsUnderBalanceLowerLimit.add(broker.id());
      }
    }
    if (!brokerIdsAboveBalanceUpperLimit.isEmpty()) {
      LOG.warn("Utilization for broker ids:{} {} above the balance limit for:{} after {}.",
               brokerIdsAboveBalanceUpperLimit, (brokerIdsAboveBalanceUpperLimit.size() > 1) ? "are" : "is", resource(),
               (clusterModel.selfHealingEligibleReplicas().isEmpty()) ? "rebalance" : "self-healing");
      _succeeded = false;
    }
    if (!brokerIdsUnderBalanceLowerLimit.isEmpty()) {
      LOG.warn("Utilization for broker ids:{} {} under the balance limit for:{} after {}.",
               brokerIdsUnderBalanceLowerLimit, (brokerIdsUnderBalanceLowerLimit.size() > 1) ? "are" : "is", resource(),
               (clusterModel.selfHealingEligibleReplicas().isEmpty()) ? "rebalance" : "self-healing");
      _succeeded = false;
    }
    // Sanity check: No self-healing eligible replica should remain at a decommissioned broker.
    for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
      if (replica.broker().isAlive()) {
        continue;
      }
      if (_selfHealingDeadBrokersOnly) {
        throw new OptimizationFailureException(
            "Self healing failed to move the replica away from decommissioned brokers.");
      }
      _selfHealingDeadBrokersOnly = true;
      LOG.warn("Omitting resource balance limit to relocate remaining replicas from dead brokers to healthy ones.");
      return;
    }
    // No dead broker contains replica.
    _selfHealingDeadBrokersOnly = false;

    // Sanity check: No self-healing eligible replica should remain at a decommissioned broker.
    for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
      if (!replica.broker().isAlive()) {
        throw new OptimizationFailureException("Self healing failed to move the replica away from decommissioned broker.");
      }
    }
    finish();
    clusterModel.untrackSortedReplicas(sortName());
  }

  /**
   * (1) REBALANCE BY LEADERSHIP MOVEMENT:
   * Perform leadership movement to ensure that the load on brokers for the outbound network load is under the balance
   * limit.
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
   * @param excludedTopics The topics that should be excluded from the optimization action.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    Set<String> excludedTopics) {
    boolean requireLessLoad = !isLoadUnderBalanceUpperLimit(broker);
    boolean requireMoreLoad = !isLoadAboveBalanceLowerLimit(broker);
    if (broker.isAlive() && !requireMoreLoad && !requireLessLoad) {
      // return if the broker is already under limit.
      return;
    } else if (!clusterModel.deadBrokers().isEmpty() && requireLessLoad && broker.isAlive()
        && broker.immigrantReplicas().isEmpty()) {
      // return if the cluster is in self-healing mode and the broker requires less load but does not have any
      // immigrant replicas.
      return;
    }

    // First try leadership movement
    if (resource() == Resource.NW_OUT || resource() == Resource.CPU) {
      if (requireLessLoad && !rebalanceByMovingLoadOut(broker, clusterModel, optimizedGoals,
                                                       LEADERSHIP_MOVEMENT, excludedTopics)) {
        LOG.debug("Successfully balanced {} for broker {} by moving out leaders.", resource(), broker.id());
        return;
      } else if (requireMoreLoad && !rebalanceByMovingLoadIn(broker, clusterModel, optimizedGoals,
                                                             LEADERSHIP_MOVEMENT, excludedTopics)) {
        LOG.debug("Successfully balanced {} for broker {} by moving in leaders.", resource(), broker.id());
        return;
      }
    }

    // Update broker ids over the balance limit for logging purposes.
    boolean unbalanced = false;
    if (requireLessLoad) {
      if (rebalanceByMovingLoadOut(broker, clusterModel, optimizedGoals, REPLICA_MOVEMENT, excludedTopics)) {
        unbalanced = rebalanceBySwappingLoadOut(broker, clusterModel, optimizedGoals, excludedTopics);
      }
    } else if (requireMoreLoad) {
      if (rebalanceByMovingLoadIn(broker, clusterModel, optimizedGoals, REPLICA_MOVEMENT, excludedTopics)) {
        unbalanced = rebalanceBySwappingLoadIn(broker, clusterModel, optimizedGoals, excludedTopics);
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
                                          Set<String> excludedTopics) {

    if (!clusterModel.newBrokers().isEmpty() && !broker.isNew()) {
      // We have new brokers and the current broker is not a new broker.
      return true;
    }

    PriorityQueue<CandidateBroker> candidateBrokerPQ = new PriorityQueue<>();
    // Sort the replicas initially to avoid sorting it every time.

    double clusterUtilization = clusterModel.load().expectedUtilizationFor(resource()) / clusterModel.capacityFor(resource());
    for (Broker candidate : clusterModel.healthyBrokers()) {
      // Get candidate replicas on candidate broker to try moving load from -- sorted in the order of trial (descending load).
      if (utilizationPercentage(candidate) > clusterUtilization) {
        SortedSet<Replica> replicasToMoveIn = sortedCandidateReplicas(candidate, excludedTopics, 0, false);
        CandidateBroker candidateBroker = new CandidateBroker(candidate, replicasToMoveIn, false);
        candidateBrokerPQ.add(candidateBroker);
      }
    }

    // Stop when all the replicas are leaders for leader movement or there is no replicas can be moved in anymore
    // for replica movement.
    while (!candidateBrokerPQ.isEmpty() && (actionType == REPLICA_MOVEMENT ||
        (actionType == LEADERSHIP_MOVEMENT && broker.leaderReplicas().size() != broker.replicas().size()))) {
      CandidateBroker cb = candidateBrokerPQ.poll();
      SortedSet<Replica> candidateReplicasToReceive = cb.replicas();

      for (Iterator<Replica> iterator = candidateReplicasToReceive.iterator(); iterator.hasNext(); ) {
        Replica replica = iterator.next();
        Broker b = maybeApplyBalancingAction(clusterModel, replica, Collections.singletonList(broker), actionType, optimizedGoals);

        // Only need to check status if the action is taken. This will also handle the case that the source broker
        // has nothing to move in. In that case we will never reenqueue that source broker.
        if (b != null) {
          if (isLoadAboveBalanceLowerLimit(broker)) {
            return false;
          }
          // Remove the replica from its source broker if it was a replica movement.
          if (actionType == REPLICA_MOVEMENT) {
            iterator.remove();
          }

          // If the source broker has a lower utilization than the next broker in the eligible broker in the queue,
          // we reenqueue the source broker and switch to the next broker.
          if (!candidateBrokerPQ.isEmpty()
              && utilizationPercentage(cb.broker()) < utilizationPercentage(candidateBrokerPQ.peek().broker())) {
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
   * descending order, (2) below the given limit in ascending order.
   *
   * @param broker Broker whose replicas will be considered.
   * @param excludedTopics Excluded topics for which the replicas will be remove from the returned candidate replicas.
   * @param loadLimit Load limit determining the lower cutoff in descending order, upper cutoff in ascending order.
   * @param isAscending True if sort requested in ascending order, false otherwise.
   * @return Sorted replicas in the given broker whose (1) topic is not an excluded topic AND (2) do not violate the
   * given load limit in ascending or descending order.
   */
  private SortedSet<Replica> sortedCandidateReplicas(Broker broker,
                                                     Set<String> excludedTopics,
                                                     double loadLimit,
                                                     boolean isAscending) {
    SortedSet<Replica> candidateReplicas = new TreeSet<>((r1, r2) -> {
      int result = isAscending
                   ? Double.compare(r1.load().expectedUtilizationFor(resource()), r2.load().expectedUtilizationFor(resource()))
                   : Double.compare(r2.load().expectedUtilizationFor(resource()), r1.load().expectedUtilizationFor(resource()));
      return result == 0 ? r1.topicPartition().toString().compareTo(r2.topicPartition().toString()) : result;
    });

    // If the resource is NW_OUT, candidate replicas consider only the leaders -- i.e. only the leaders have NW_OUT load,
    // otherwise all replicas on broker are considered.
    Set<Replica> coveredReplicas = resource() == Resource.NW_OUT ? broker.leaderReplicas() : broker.replicas();

    // The given load limit determines the lower cutoff in descending order, upper cutoff in ascending order.
    if (isAscending) {
      candidateReplicas.addAll(coveredReplicas.stream()
          .filter(r -> !shouldExclude(r, excludedTopics) && r.load().expectedUtilizationFor(resource()) < loadLimit)
          .collect(Collectors.toSet()));
    } else {
      candidateReplicas.addAll(coveredReplicas.stream()
          .filter(r -> !shouldExclude(r, excludedTopics) && r.load().expectedUtilizationFor(resource()) > loadLimit)
          .collect(Collectors.toSet()));
    }

    return candidateReplicas;
  }

  private boolean rebalanceBySwappingLoadOut(Broker broker,
                                             ClusterModel clusterModel,
                                             Set<Goal> optimizedGoals,
                                             Set<String> excludedTopics) {
    if (!broker.isAlive()) {
      return true;
    }
    // Get the replicas to rebalance.
    SortedSet<Replica> sourceReplicas = new TreeSet<>((r1, r2) -> {
      int result = Double.compare(r2.load().expectedUtilizationFor(resource()), r1.load().expectedUtilizationFor(resource()));
      return result == 0 ? r1.topicPartition().toString().compareTo(r2.topicPartition().toString()) : result;
    });

    sourceReplicas.addAll(resource() == Resource.NW_OUT ? broker.leaderReplicas() : broker.replicas());

    // Sort the replicas initially to avoid sorting it every time.
    PriorityQueue<CandidateBroker> candidateBrokerPQ = new PriorityQueue<>();
    for (Broker candidate : clusterModel.healthyBrokersUnderThreshold(resource(), _balanceUpperThreshold)
                                        .stream().filter(b -> !b.replicas().isEmpty()).collect(Collectors.toSet())) {
      // Get candidate replicas on candidate broker to try swapping with -- sorted in the order of trial (ascending load).
      double maxSourceReplicaLoad = sourceReplicas.first().load().expectedUtilizationFor(resource());
      SortedSet<Replica> replicasToSwapWith = sortedCandidateReplicas(candidate, excludedTopics, maxSourceReplicaLoad, true);
      CandidateBroker candidateBroker = new CandidateBroker(candidate, replicasToSwapWith, true);
      candidateBrokerPQ.add(candidateBroker);
    }

    while (!candidateBrokerPQ.isEmpty()) {
      CandidateBroker cb = candidateBrokerPQ.poll();
      SortedSet<Replica> candidateReplicasToSwapWith = cb.replicas();

      Replica swappedInReplica = null;
      Replica swappedOutReplica = null;
      for (Replica sourceReplica : sourceReplicas) {
        if (shouldExclude(sourceReplica, excludedTopics)) {
          continue;
        }

        // Try swapping the source with the candidate replicas. Get the swapped in replica if successful, null otherwise.
        Replica swappedIn = maybeApplySwapAction(clusterModel, sourceReplica, candidateReplicasToSwapWith, optimizedGoals);
        if (swappedIn != null) {
          if (isLoadUnderBalanceUpperLimit(broker)) {
            // Successfully balanced this broker by swapping in.
            return false;
          }
          // Add swapped in/out replica for updating the list of replicas in source broker.
          swappedInReplica = swappedIn;
          swappedOutReplica = sourceReplica;
          break;
        }
      }

      swapUpdate(swappedInReplica, swappedOutReplica, sourceReplicas, candidateReplicasToSwapWith, candidateBrokerPQ, cb);
    }

    return true;
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
                                            Set<String> excludedTopics) {
    if (!broker.isAlive() || broker.replicas().isEmpty()) {
      // Source broker is dead or has no replicas to swap.
      return true;
    }

    // Get the replicas to rebalance.
    SortedSet<Replica> sourceReplicas = new TreeSet<>(
        Comparator.comparingDouble((Replica r) -> r.load().expectedUtilizationFor(resource()))
                  .thenComparing(r -> r.topicPartition().toString()));

    sourceReplicas.addAll(broker.replicas());

    // Sort the replicas initially to avoid sorting it every time.
    PriorityQueue<CandidateBroker> candidateBrokerPQ = new PriorityQueue<>();
    for (Broker candidate : clusterModel.healthyBrokersOverThreshold(resource(), _balanceLowerThreshold)) {
      // Get candidate replicas on candidate broker to try swapping with -- sorted in the order of trial (descending load).
      double minSourceReplicaLoad = sourceReplicas.first().load().expectedUtilizationFor(resource());
      SortedSet<Replica> replicasToSwapWith = sortedCandidateReplicas(candidate, excludedTopics, minSourceReplicaLoad, false);
      CandidateBroker candidateBroker = new CandidateBroker(candidate, replicasToSwapWith, false);
      candidateBrokerPQ.add(candidateBroker);
    }

    while (!candidateBrokerPQ.isEmpty()) {
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
        Replica swappedIn = maybeApplySwapAction(clusterModel, sourceReplica, candidateReplicasToSwapWith, optimizedGoals);
        if (swappedIn != null) {
          if (isLoadAboveBalanceLowerLimit(broker)) {
            // Successfully balanced this broker by swapping in.
            return false;
          }
          // Add swapped in/out replica for updating the list of replicas in source broker.
          swappedInReplica = swappedIn;
          swappedOutReplica = sourceReplica;
          break;
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
                                           Set<String> excludedTopics) {
    // Get the eligible brokers.
    SortedSet<Broker> candidateBrokers = new TreeSet<>(
        Comparator.comparingDouble(this::utilizationPercentage).thenComparingInt(Broker::id));
    if (_selfHealingDeadBrokersOnly) {
      candidateBrokers.addAll(clusterModel.healthyBrokers());
    } else {
      candidateBrokers.addAll(clusterModel.healthyBrokersUnderThreshold(resource(), _balanceUpperThreshold));
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
      // It does not make sense to move a replica without utilization from a live broker.
      if (replica.load().expectedUtilizationFor(resource()) == 0.0 && broker.isAlive()) {
        break;
      }

      // An optimization for leader movements.
      SortedSet<Broker> eligibleBrokers;
      if (actionType == LEADERSHIP_MOVEMENT) {
        eligibleBrokers = new TreeSet<>(Comparator.comparingDouble(this::utilizationPercentage)
                                                  .thenComparingInt(Broker::id));
        clusterModel.partition(replica.topicPartition()).followerBrokers().forEach(b -> {
          if (candidateBrokers.contains(b)) {
            eligibleBrokers.add(b);
          }
        });
      } else {
        eligibleBrokers = candidateBrokers;
      }

      Broker b = maybeApplyBalancingAction(clusterModel, replica, eligibleBrokers, actionType, optimizedGoals);
      // Only check if we successfully moved something.
      if (b != null) {
        if (isLoadUnderBalanceUpperLimit(broker)) {
          return false;
        }
        // Remove and reinsert the broker so the order is correct.
        candidateBrokers.remove(b);
        if (utilizationPercentage(b) < _balanceUpperThreshold) {
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
   * @return the utilization upper threshold in percent for the {@link #resource()}
   */
  private double computeBalanceUpperThreshold(ClusterModel clusterModel) {
    return (clusterModel.load().expectedUtilizationFor(resource()) / clusterModel.capacityFor(resource()))
        * (1 + balancePercentageWithMargin(resource()));
  }

  /**
   * @param clusterModel the cluster topology and load.
   * @return the utilization lower threshold in percent for the {@link #resource()}
   */
  private double computeBalanceLowerThreshold(ClusterModel clusterModel) {
    return (clusterModel.load().expectedUtilizationFor(resource()) / clusterModel.capacityFor(resource()))
        * Math.max(0, (1 - balancePercentageWithMargin(resource())));
  }

  private double utilizationPercentage(Broker broker) {
    return broker.isAlive() ? broker.load().expectedUtilizationFor(resource()) / broker.capacityFor(resource()) : 1;
  }

  /**
   * To avoid churns, we add a balance margin to the user specified rebalance threshold. e.g. when user sets the
   * threshold to be resourceBalancePercentage, we use (resourceBalancePercentage-1)*balanceMargin instead.
   * @return the rebalance threshold with a margin.
   */
  private double balancePercentageWithMargin(Resource resource) {
    return (_balancingConstraint.resourceBalancePercentage(resource) - 1) * BALANCE_MARGIN;
  }

  private String sortName() {
    return name() + "-" + resource().name() + "-ALL";
  }

  /**
   * A helper class for this goal to keep track of the candidate brokers and its sorted replicas.
   */
  private class CandidateBroker implements Comparable<CandidateBroker> {
    private final Broker _broker;
    private final SortedSet<Replica> _replicas;
    private final boolean _isAscending;

    CandidateBroker(Broker broker, SortedSet<Replica> replicas, boolean isAscending) {
      _broker = broker;
      _replicas = replicas;
      _isAscending = isAscending;
    }

    public Broker broker() {
      return _broker;
    }

    public SortedSet<Replica> replicas() {
      return _replicas;
    }

    @Override
    public int compareTo(CandidateBroker o) {
      int result = _isAscending
                   ? Double.compare(utilizationPercentage(_broker), utilizationPercentage(o._broker))
                   : Double.compare(utilizationPercentage(o._broker), utilizationPercentage(_broker));
      return result != 0 ? result : Integer.compare(_broker.id(), o._broker.id());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CandidateBroker that = (CandidateBroker) o;
      int result = Double.compare(utilizationPercentage(that._broker), utilizationPercentage(_broker));
      return result == 0 && Integer.compare(_broker.id(), that._broker.id()) == 0;
    }

    @Override
    public int hashCode() {
      return _broker.id();
    }

    @Override
    public String toString() {
      return "CandidateBroker{" + _broker + " util: " + utilizationPercentage(_broker) + "}";
    }
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
