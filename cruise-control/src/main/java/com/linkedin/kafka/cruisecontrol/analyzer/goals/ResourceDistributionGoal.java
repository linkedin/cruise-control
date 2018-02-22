/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Load;
import com.linkedin.kafka.cruisecontrol.model.Replica;

import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

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
   * @deprecated Please use {@link this#actionAcceptance(BalancingAction, ClusterModel)} instead.
   */
  @Override
  public boolean isActionAcceptable(BalancingAction action, ClusterModel clusterModel) {
    return actionAcceptance(action, clusterModel) == ACCEPT;
  }

  /**
   * Check whether given action is acceptable by this goal. An action is acceptable by this goal if the movement
   * specified by the given action does not lead to a utilization in destination that is more than the broker
   * balance limit (in terms of utilization) broker utilization achieved after the balancing process of this goal.
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

    if (action.balancingAction() == REPLICA_SWAP) {
      Replica destinationReplica = destinationBroker.replica(action.destinationTopicPartition());
      double sourceUtilizationDelta = destinationReplica.load().expectedUtilizationFor(resource())
                                      - sourceReplica.load().expectedUtilizationFor(resource());

      if (sourceUtilizationDelta == 0) {
        // No change in terms of load.
        return ACCEPT;
      }
      // Check if the source or destination broker is within the balance limit before applying a swap that could
      // potentially make them unbalanced -- i.e. never make a balanced broker unbalanced.
      boolean isCurrentlyWithinLimit = sourceUtilizationDelta > 0
                                       ? (isLoadAboveBalanceLowerLimit(clusterModel, destinationBroker)
                                          || isLoadUnderBalanceUpperLimit(clusterModel, sourceReplica.broker()))
                                       : (isLoadAboveBalanceLowerLimit(clusterModel, sourceReplica.broker())
                                          || isLoadUnderBalanceUpperLimit(clusterModel, destinationBroker));

      if (isCurrentlyWithinLimit) {
        // Ensure that the resource utilization on brokers do not go out of limits after the swap.
        return isLoadStillWithinLimitsAfterSwap(clusterModel, sourceReplica, destinationReplica) ? ACCEPT : REPLICA_REJECT;
      }
      // Ensure that the swap does not increase the utilization difference between brokers.
      return isSelfSatisfiedAfterSwap(sourceReplica, destinationReplica) ? ACCEPT : REPLICA_REJECT;
    }

    if (isLoadAboveBalanceLowerLimit(clusterModel, sourceReplica.broker())
        || isLoadUnderBalanceUpperLimit(clusterModel, destinationBroker)) {
      // Already satisfied balance limits cannot be violated due to balancing action.
      return (isLoadUnderBalanceUpperLimitAfterChange(clusterModel, sourceReplica.load(), destinationBroker, ADD) &&
             isLoadAboveBalanceLowerLimitAfterChange(clusterModel, sourceReplica.load(), sourceReplica.broker(), REMOVE))
             ? ACCEPT : REPLICA_REJECT;
    }

    // Check that current destination would not become more unbalanced (none of them were balanced before).
    return isAcceptableAfterReplicaMove(sourceReplica, destinationBroker) ? ACCEPT : REPLICA_REJECT;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new ResourceDistributionGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(_numSnapshots, _minMonitoredPartitionPercentage, false);
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

    if (action.balancingAction() == REPLICA_SWAP) {
      Replica destinationReplica = destinationBroker.replica(action.destinationTopicPartition());
      double sourceUtilizationDelta = destinationReplica.load().expectedUtilizationFor(resource())
                                      - sourceReplica.load().expectedUtilizationFor(resource());

      return !(sourceUtilizationDelta == 0) && isLoadStillWithinLimitsAfterSwap(clusterModel, sourceReplica,
                                                                                destinationReplica);
    }

    //Check that current destination would not become more unbalanced.
    return isLoadUnderBalanceUpperLimitAfterChange(clusterModel, sourceReplica.load(), destinationBroker, ADD) &&
        isLoadAboveBalanceLowerLimitAfterChange(clusterModel, sourceReplica.load(), sourceReplica.broker(), REMOVE);
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
      if (!isLoadUnderBalanceUpperLimit(clusterModel, broker)) {
        brokerIdsAboveBalanceUpperLimit.add(broker.id());
      }
      if (!isLoadAboveBalanceLowerLimit(clusterModel, broker)) {
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
                                    Set<String> excludedTopics)
      throws ModelInputException {
    boolean requireLessLoad = !isLoadUnderBalanceUpperLimit(clusterModel, broker);
    boolean requireMoreLoad = !isLoadAboveBalanceLowerLimit(clusterModel, broker);
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
    if ((resource() == Resource.NW_OUT || resource() == Resource.CPU)) {
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
                                          Set<String> excludedTopics)
      throws ModelInputException {

    PriorityQueue<CandidateBroker> candidateBrokerPQ = new PriorityQueue<>();
    // Sort the replicas initially to avoid sorting it every time.

    double clusterUtilization = clusterModel.load().expectedUtilizationFor(resource()) / clusterModel.capacityFor(resource());
    for (Broker candidate : clusterModel.healthyBrokers()) {
      if (utilizationPercentage(candidate) > clusterUtilization) {
        SortedSet<Replica> candidateReplicasToSwapWith = replicasToSwapWith(candidate, excludedTopics, 0, true);
        CandidateBroker candidateBroker = new CandidateBroker(candidate, candidateReplicasToSwapWith, false);
        candidateBrokerPQ.add(candidateBroker);
      }
    }

    // Stop when all the replicas are leaders for leader movement or there is no replicas can be moved in anymore
    // for replica movement.
    while (!candidateBrokerPQ.isEmpty() && (actionType == REPLICA_MOVEMENT ||
        (actionType == LEADERSHIP_MOVEMENT && broker.leaderReplicas().size() != broker.replicas().size()))) {
      CandidateBroker cb = candidateBrokerPQ.poll();
      SortedSet<Replica> candidateReplicasToReceive = cb.replicas();

      Set<Replica> receivedReplicas = new HashSet<>();
      for (Replica replica : candidateReplicasToReceive) {
        if (shouldExclude(replica, excludedTopics)) {
          continue;
        }

        Broker b = maybeApplyBalancingAction(clusterModel, replica, Collections.singletonList(broker), actionType, optimizedGoals);
        // Only need to check status if the action is taken. This will also handle the case that the source broker
        // has nothing to move in. In that case we will never reenqueue that source broker.
        if (b != null) {
          if (isLoadAboveBalanceLowerLimit(clusterModel, broker)) {
            return false;
          }
          // Identify the replica that the source broker has received.
          receivedReplicas.add(replica);

          // If the source broker has a lower utilization than the next broker in the eligible broker in the queue,
          // we reenqueue the source broker and switch to the next broker.
          if (!candidateBrokerPQ.isEmpty() &&
              utilizationPercentage(cb.broker()) < utilizationPercentage(candidateBrokerPQ.peek().broker())) {
            if (actionType == REPLICA_MOVEMENT) {
              candidateReplicasToReceive.removeAll(receivedReplicas);
            }
            candidateBrokerPQ.add(cb);
            break;
          }
        }
      }
    }
    return true;
  }

  private SortedSet<Replica> replicasToSwapWith(Broker broker,
                                                Set<String> excludedTopics,
                                                double swapLoadLimit,
                                                boolean isSwapIn) {
    SortedSet<Replica> candidateReplicasToSwapWith = new TreeSet<>((r1, r2) -> {
      int result = isSwapIn ? Double.compare(r2.load().expectedUtilizationFor(resource()), r1.load().expectedUtilizationFor(resource()))
                            : Double.compare(r1.load().expectedUtilizationFor(resource()), r2.load().expectedUtilizationFor(resource()));
      return result == 0 ? r1.topicPartition().toString().compareTo(r2.topicPartition().toString()) : result;
    });
    candidateReplicasToSwapWith.addAll(
        resource() == Resource.NW_OUT
        ? broker.leaderReplicas().stream().filter(l -> (!excludedTopics.contains(l.topicPartition().topic())) &&
                                                       (isSwapIn ? l.load().expectedUtilizationFor(resource()) > swapLoadLimit
                                                                 : l.load().expectedUtilizationFor(resource()) < swapLoadLimit))
                .collect(Collectors.toSet())
        : broker.replicas().stream().filter(r -> (!excludedTopics.contains(r.topicPartition().topic())) &&
                                                 (isSwapIn ? r.load().expectedUtilizationFor(resource()) > swapLoadLimit
                                                           : r.load().expectedUtilizationFor(resource()) < swapLoadLimit))
                .collect(Collectors.toSet()));

    return candidateReplicasToSwapWith;
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
    for (Broker candidate : clusterModel.healthyBrokersUnderThreshold(resource(), balanceUpperThreshold(clusterModel))
                                        .stream().filter(b -> !b.replicas().isEmpty()).collect(Collectors.toSet())) {
      // Get candidate replicas on candidate broker to try swapping with -- sorted in the order of trial.
      double maxSourceReplicaLoad = sourceReplicas.first().load().expectedUtilizationFor(resource());
      SortedSet<Replica> candidateReplicasToSwapWith = replicasToSwapWith(candidate, excludedTopics, maxSourceReplicaLoad, false);

      CandidateBroker candidateBroker = new CandidateBroker(candidate, candidateReplicasToSwapWith, true);
      candidateBrokerPQ.add(candidateBroker);
    }

    while (!candidateBrokerPQ.isEmpty()) {
      CandidateBroker cb = candidateBrokerPQ.poll();
      SortedSet<Replica> candidateReplicasToSwapWith = cb.replicas();

      Replica swappedInReplica = null;
      Replica swappedOutReplica = null;
      for (Replica sourceReplica : sourceReplicas) {
        if (excludedTopics.contains(sourceReplica.topicPartition().topic())) {
          continue;
        }

        // Try swapping the source with the candidate replicas. Get the swapped in replica if successful, null otherwise.
        Replica swappedIn = maybeApplySwapAction(clusterModel, sourceReplica, candidateReplicasToSwapWith, optimizedGoals);
        if (swappedIn != null) {
          if (isLoadUnderBalanceUpperLimit(clusterModel, broker)) {
            // Successfully balanced this broker by swapping in.
            return false;
          }
          // Add swapped in/out replica for updating the list of replicas in source broker.
          swappedInReplica = swappedIn;
          swappedOutReplica = sourceReplica;
          break;
        }
      }

      if (swappedInReplica != null) {
        // Update the list of replicas in source broker after the swap operations.
        sourceReplicas.remove(swappedOutReplica);
        sourceReplicas.add(swappedInReplica);
        candidateReplicasToSwapWith.remove(swappedInReplica);
        candidateReplicasToSwapWith.add(swappedOutReplica);

        // The broker is still eligible to be a candidate replica.
        candidateBrokerPQ.add(cb);
      }
    }

    return true;
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
    for (Broker candidate : clusterModel.healthyBrokersOverThreshold(resource(), balanceLowerThreshold(clusterModel))) {
      // Get candidate replicas on candidate broker to try swapping with -- sorted in the order of trial.
      double minSourceReplicaLoad = sourceReplicas.first().load().expectedUtilizationFor(resource());
      SortedSet<Replica> candidateReplicasToSwapWith = replicasToSwapWith(candidate, excludedTopics, minSourceReplicaLoad, true);
      CandidateBroker candidateBroker = new CandidateBroker(candidate, candidateReplicasToSwapWith, false);
      candidateBrokerPQ.add(candidateBroker);
    }

    while (!candidateBrokerPQ.isEmpty()) {
      CandidateBroker cb = candidateBrokerPQ.poll();
      SortedSet<Replica> candidateReplicasToSwapWith = cb.replicas();

      Replica swappedInReplica = null;
      Replica swappedOutReplica = null;
      for (Replica sourceReplica : sourceReplicas) {
        if (excludedTopics.contains(sourceReplica.topicPartition().topic())) {
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
          if (isLoadAboveBalanceLowerLimit(clusterModel, broker)) {
            // Successfully balanced this broker by swapping in.
            return false;
          }
          // Add swapped in/out replica for updating the list of replicas in source broker.
          swappedInReplica = swappedIn;
          swappedOutReplica = sourceReplica;
          break;
        }
      }

      if (swappedInReplica != null) {
        // Update the list of replicas in source broker after the swap operations.
        sourceReplicas.remove(swappedOutReplica);
        sourceReplicas.add(swappedInReplica);
        candidateReplicasToSwapWith.remove(swappedInReplica);
        candidateReplicasToSwapWith.add(swappedOutReplica);

        // The broker is still eligible to be a candidate replica.
        candidateBrokerPQ.add(cb);
      }
    }

    return true;
  }

  private boolean rebalanceByMovingLoadOut(Broker broker,
                                           ClusterModel clusterModel,
                                           Set<Goal> optimizedGoals,
                                           ActionType actionType,
                                           Set<String> excludedTopics)
      throws ModelInputException {
    // Get the eligible brokers.
    SortedSet<Broker> candidateBrokers = new TreeSet<>(
        Comparator.comparingDouble(this::utilizationPercentage).thenComparingInt(Broker::id));
    double balancingUpperThreshold = balanceUpperThreshold(clusterModel);
    if (_selfHealingDeadBrokersOnly) {
      candidateBrokers.addAll(clusterModel.healthyBrokers());
    } else {
      candidateBrokers.addAll(clusterModel.healthyBrokersUnderThreshold(resource(), balancingUpperThreshold));
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
      replicasToMove = broker.sortedReplicas(resource());
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
        if (isLoadUnderBalanceUpperLimit(clusterModel, broker)) {
          return false;
        }
        // Remove and reinsert the broker so the order is correct.
        candidateBrokers.remove(b);
        if (utilizationPercentage(b) < balancingUpperThreshold) {
          candidateBrokers.add(b);
        }
      }
    }
    // If all the replicas has been moved away from the broker and we still reach here, that means the broker
    // capacity is negative, i.e. the broker is dead. So as long as there is no replicas on the broker anymore
    // we consider it as not over limit.
    return !broker.replicas().isEmpty();
  }

  private boolean isLoadAboveBalanceLowerLimit(ClusterModel clusterModel, Broker broker) {
    // The action does not matter here because the load is null.
    return isLoadAboveBalanceLowerLimitAfterChange(clusterModel, null, broker, ADD);
  }

  private boolean isLoadUnderBalanceUpperLimit(ClusterModel clusterModel, Broker broker) {
    // The action does not matter here because the load is null.
    return isLoadUnderBalanceUpperLimitAfterChange(clusterModel, null, broker, REMOVE);
  }

  private boolean isLoadAboveBalanceLowerLimitAfterChange(ClusterModel clusterModel,
                                                          Load load,
                                                          Broker broker,
                                                          ChangeType changeType) {
    double utilizationDelta = load == null ? 0 : load.expectedUtilizationFor(resource());

    double balanceLowerThreshold = balanceLowerThreshold(clusterModel);
    double brokerCapacity = broker.capacityFor(resource());
    double brokerBalanceLowerLimit = brokerCapacity * balanceLowerThreshold;
    double brokerUtilization = broker.load().expectedUtilizationFor(resource());
    boolean isBrokerAboveLowerLimit = changeType == ADD ? brokerUtilization + utilizationDelta >= brokerBalanceLowerLimit :
        brokerUtilization - utilizationDelta >= brokerBalanceLowerLimit;

    if (resource().isHostResource()) {
      double hostCapacity = broker.host().capacityFor(resource());
      double hostBalanceLowerLimit = hostCapacity * balanceLowerThreshold;
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

  /**
   * Check if moving the source replica to destination broker would decrease the utilization difference between source
   * and destination brokers.
   *
   * @param sourceReplica Source replica to be moved.
   * @param destinationBroker Destination broker to receive the source replica utilization.
   * @return True if the change would lead to a better balance, false otherwise.
   */
  private boolean isAcceptableAfterReplicaMove(Replica sourceReplica, Broker destinationBroker) {
    double sourceUtilizationDelta = 0 - sourceReplica.load().expectedUtilizationFor(resource());

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

    prevDiff = (prevDiff <= 0.0F) ? 0.0F - prevDiff : prevDiff;
    nextDiff = (nextDiff <= 0.0F) ? 0.0F - nextDiff : nextDiff;

    return nextDiff < prevDiff;
  }

  private boolean isLoadStillWithinLimitsAfterSwap(ClusterModel clusterModel, Replica sourceReplica, Replica destinationReplica) {
    double sourceUtilizationDelta = destinationReplica.load().expectedUtilizationFor(resource())
                                    - sourceReplica.load().expectedUtilizationFor(resource());
    double upperThreshold = balanceUpperThreshold(clusterModel);
    double lowerThreshold = balanceLowerThreshold(clusterModel);

    // Check: Broker resource load within balance limit check.
    boolean brokerLoadStillInLimits =
        isBrokerLoadStillWithinLimitsAfterSwap(sourceUtilizationDelta, upperThreshold, lowerThreshold, sourceReplica, destinationReplica);

    if (brokerLoadStillInLimits || !resource().isHostResource()) {
      return brokerLoadStillInLimits;
    }

    // Check: Host resource load within balance limit check.
    return isHostLoadStillWithinLimitsAfterSwap(sourceUtilizationDelta, upperThreshold, lowerThreshold, sourceReplica, destinationReplica);
  }

  private boolean isHostLoadStillWithinLimitsAfterSwap(double sourceUtilizationDelta,
                                                       double upperThreshold,
                                                       double lowerThreshold,
                                                       Replica sourceReplica,
                                                       Replica destinationReplica) {
    double sourceHostUtilization = sourceReplica.broker().host().load().expectedUtilizationFor(resource());
    double destinationHostUtilization = destinationReplica.broker().host().load().expectedUtilizationFor(resource());

    // 1. Host under balance upper limit check.
    boolean isHostUnderUpperLimit;
    if (sourceUtilizationDelta > 0) {
      double sourceHostBalanceUpperLimit = sourceReplica.broker().host().capacityFor(resource()) * upperThreshold;
      isHostUnderUpperLimit = sourceHostUtilization + sourceUtilizationDelta <= sourceHostBalanceUpperLimit;
    } else {
      double destinationHostBalanceUpperLimit = destinationReplica.broker().host().capacityFor(resource()) * upperThreshold;
      isHostUnderUpperLimit = destinationHostUtilization - sourceUtilizationDelta <= destinationHostBalanceUpperLimit;
    }

    if (!isHostUnderUpperLimit) {
      return false;
    }
    // 2. Host above balance lower limit check.
    boolean isHostAboveLowerLimit;
    if (sourceUtilizationDelta < 0) {
      double sourceHostBalanceLowerLimit = sourceReplica.broker().host().capacityFor(resource()) * lowerThreshold;
      isHostAboveLowerLimit = sourceHostUtilization + sourceUtilizationDelta >= sourceHostBalanceLowerLimit;
    } else {
      double destinationHostBalanceLowerLimit = destinationReplica.broker().host().capacityFor(resource()) * lowerThreshold;
      isHostAboveLowerLimit = destinationHostBalanceLowerLimit - sourceUtilizationDelta >= destinationHostBalanceLowerLimit;
    }

    return isHostAboveLowerLimit;
  }

  private boolean isBrokerLoadStillWithinLimitsAfterSwap(double sourceUtilizationDelta,
                                                         double upperThreshold,
                                                         double lowerThreshold,
                                                         Replica sourceReplica,
                                                         Replica destinationReplica) {
    double sourceBrokerUtilization = sourceReplica.broker().load().expectedUtilizationFor(resource());
    double destinationBrokerUtilization = destinationReplica.broker().load().expectedUtilizationFor(resource());

    // 1. Broker under balance upper limit check.
    boolean isBrokerUnderUpperLimit;
    if (sourceUtilizationDelta > 0) {
      double sourceBrokerBalanceUpperLimit = sourceReplica.broker().capacityFor(resource()) * upperThreshold;
      isBrokerUnderUpperLimit = sourceBrokerUtilization + sourceUtilizationDelta <= sourceBrokerBalanceUpperLimit;
    } else {
      double destinationBrokerBalanceUpperLimit = destinationReplica.broker().capacityFor(resource()) * upperThreshold;
      isBrokerUnderUpperLimit = destinationBrokerUtilization - sourceUtilizationDelta <= destinationBrokerBalanceUpperLimit;
    }

    if (!isBrokerUnderUpperLimit) {
      return false;
    }

    // 2. Broker above balance lower limit check.
    boolean isBrokerAboveLowerLimit;
    if (sourceUtilizationDelta < 0) {
      double sourceBrokerBalanceLowerLimit = sourceReplica.broker().capacityFor(resource()) * lowerThreshold;
      isBrokerAboveLowerLimit = sourceBrokerUtilization + sourceUtilizationDelta >= sourceBrokerBalanceLowerLimit;
    } else {
      double destinationBrokerBalanceLowerLimit = destinationReplica.broker().capacityFor(resource()) * lowerThreshold;
      isBrokerAboveLowerLimit = destinationBrokerUtilization - sourceUtilizationDelta >= destinationBrokerBalanceLowerLimit;
    }

    return isBrokerAboveLowerLimit;
  }

  private boolean isLoadUnderBalanceUpperLimitAfterChange(ClusterModel clusterModel,
                                                          Load load,
                                                          Broker broker,
                                                          ChangeType changeType) {
    double utilizationDelta = load == null ? 0 : load.expectedUtilizationFor(resource());

    double balanceUpperThreshold = balanceUpperThreshold(clusterModel);
    double brokerCapacity = broker.capacityFor(resource());
    double brokerBalanceUpperLimit = brokerCapacity * balanceUpperThreshold;
    double brokerUtilization = broker.load().expectedUtilizationFor(resource());
    boolean isBrokerUnderUpperLimit = changeType == ADD ? brokerUtilization + utilizationDelta <= brokerBalanceUpperLimit :
        brokerUtilization - utilizationDelta <= brokerBalanceUpperLimit;

    if (resource().isHostResource()) {
      double hostCapacity = broker.host().capacityFor(resource());
      double hostBalanceUpperLimit = hostCapacity * balanceUpperThreshold;
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
   * @param clusterModel the cluster topology and load.
   * @return the utilization upper threshold in percent for the {@link #resource()}
   */
  private double balanceUpperThreshold(ClusterModel clusterModel) {
    return (clusterModel.load().expectedUtilizationFor(resource()) / clusterModel.capacityFor(resource()))
        * (1 + balancePercentageWithMargin(resource()));
  }

  /**
   * @param clusterModel the cluster topology and load.
   * @return the utilization lower threshold in percent for the {@link #resource()}
   */
  private double balanceLowerThreshold(ClusterModel clusterModel) {
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
        // First compare the
        if (numBalancedBroker2 > numBalancedBroker1) {
            _reasonForLastNegativeResult = String.format(
                "Violated %s. [Number of Balanced Brokers] for resource %s. post-optimization:%d pre-optimization:%d",
                name(), resource(), numBalancedBroker1, numBalancedBroker2);
            return -1;
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
