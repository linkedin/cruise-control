/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
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
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
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
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.remainingTimeMs;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.utilization;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.DENOMINATOR_FOR_MIN_VALID_WINDOWS_FOR_SELF_HEALING;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal.ChangeType.ADD;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal.ChangeType.REMOVE;


/**
 * SOFT GOAL: Balance {@link Resource} distribution over alive brokers not excluded for replica moves.
 */
public abstract class ResourceDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceDistributionGoal.class);
  public static final double BALANCE_MARGIN = 0.9;
  // Flag to indicate whether the self healing failed to relocate all offline replicas away from dead brokers or broken
  // disks in its initial attempt and currently omitting the resource balance limit to relocate remaining replicas.
  private boolean _fixOfflineReplicasOnly;
  private double _balanceUpperThreshold;
  private double _balanceLowerThreshold;
  private final Comparator<Broker> _brokerComparator;
  // This is used to identify brokers not excluded for replica moves.
  private Set<Integer> _brokersAllowedReplicaMove;
  // This is used to identify the overprovisioned cluster status
  private boolean _isLowUtilization;
  // The recommendation to be used in case the cluster is overprovisioned
  private ProvisionRecommendation _overProvisionedRecommendation;

  /**
   * Constructor for Resource Distribution Goal.
   */
  public ResourceDistributionGoal() {
    _brokerComparator = Comparator.comparingDouble((Broker b) -> utilization(b, resource())).thenComparing(b -> b);
  }

  /**
   * Package private for unit test.
   */
  ResourceDistributionGoal(BalancingConstraint constraint) {
    this();
    _balancingConstraint = constraint;
  }

  protected abstract Resource resource();

  /**
   * Check whether given action is acceptable by this goal. An action is acceptable by this goal if it satisfies the
   * following: (1) if both source and destination brokers were within the limit before the action, the corresponding
   * limits cannot be violated after the action, (2) otherwise, the action cannot increase the utilization difference
   * between brokers. If the source broker is excluded for replica moves, then action cannot cause destination broker to
   * be unbalanced -- i.e. either by making it move from under the balance upper limit to over it or if it is already
   * above the upper limit, by moving any replica load with a positive utilization.
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
        // It is guaranteed that neither source nor destination brokers are excluded for replica moves.
        boolean bothBrokersCurrentlyWithinLimit = sourceUtilizationDelta > 0
                                                  ? (isLoadAboveBalanceLowerLimit(destinationBroker)
                                                     && isLoadUnderBalanceUpperLimit(sourceBroker))
                                                  : (isLoadAboveBalanceLowerLimit(sourceBroker)
                                                     && isLoadUnderBalanceUpperLimit(destinationBroker));

        if (bothBrokersCurrentlyWithinLimit) {
          // Ensure that the resource utilization on balanced broker(s) do not go out of limits after the swap.
          return isSwapViolatingLimit(sourceReplica, destinationReplica) ? REPLICA_REJECT : ACCEPT;
        }
        // Ensure that the swap does not increase the utilization difference between brokers.
        return isSelfSatisfiedAfterSwap(sourceReplica, destinationReplica) ? ACCEPT : REPLICA_REJECT;
      case INTER_BROKER_REPLICA_MOVEMENT:
      case LEADERSHIP_MOVEMENT:
        boolean sourceBrokerExcludedForReplicaMove = isExcludedForReplicaMove(sourceBroker);
        if ((sourceBrokerExcludedForReplicaMove || isLoadAboveBalanceLowerLimit(sourceBroker))
            && isLoadUnderBalanceUpperLimit(destinationBroker)) {
          // Already satisfied balance limits cannot be violated due to balancing action.
          return (isLoadUnderBalanceUpperLimitAfterChange(sourceReplica.load(), destinationBroker, ADD)
                  && (sourceBrokerExcludedForReplicaMove
                      || isLoadAboveBalanceLowerLimitAfterChange(sourceReplica.load(), sourceBroker, REMOVE)))
                 ? ACCEPT : REPLICA_REJECT;
        } else if (sourceBrokerExcludedForReplicaMove) {
          // We know that the destination broker load is not under balance upper limit. Hence moving load to it from a
          // source broker that is excluded for replica moves is acceptable only if the replica load is 0.
          return sourceReplica.load().expectedUtilizationFor(resource()) == 0.0 ? ACCEPT : REPLICA_REJECT;
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
    return new ModelCompletenessRequirements(Math.max(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING,
                                                      _numWindows / DENOMINATOR_FOR_MIN_VALID_WINDOWS_FOR_SELF_HEALING),
                                             _minMonitoredPartitionPercentage, false);
  }

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
   * {@code false} otherwise. An action is acceptable if: (1) destination broker utilization for the given resource is less
   * than the source broker utilization. (2) movement is acceptable (i.e. under the broker balance limit for balanced
   * resources) for already balanced resources. Already balanced resources are the ones that have gone through the
   * "resource distribution" process specified in this goal.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model.
   * @return {@code true} if requirements of this goal are not violated if this action is applied to the given cluster state,
   * {@code false} otherwise.
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
        return isLoadUnderBalanceUpperLimitAfterChange(sourceReplica.load(), destinationBroker, ADD)
               && isLoadAboveBalanceLowerLimitAfterChange(sourceReplica.load(), sourceReplica.broker(), REMOVE);
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
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    _brokersAllowedReplicaMove = GoalUtils.aliveBrokersNotExcludedForReplicaMove(clusterModel, optimizationOptions);
    if (_brokersAllowedReplicaMove.isEmpty()) {
      // Handle the case when all alive brokers are excluded from replica moves.
      ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
          .numBrokers(clusterModel.maxReplicationFactor()).build();
      throw new OptimizationFailureException(String.format("[%s] All alive brokers are excluded from replica moves.", name()), recommendation);
    }
    _fixOfflineReplicasOnly = false;
    double resourceUtilization = clusterModel.load().expectedUtilizationFor(resource());
    double capacity = clusterModel.capacityWithAllowedReplicaMovesFor(resource(), optimizationOptions);
    // Cluster utilization excludes the capacity of brokers excluded for replica moves.
    double avgUtilizationPercentage = resourceUtilization / capacity;
    _balanceUpperThreshold = GoalUtils.computeResourceUtilizationBalanceThreshold(avgUtilizationPercentage,
                                                                                  resource(),
                                                                                  _balancingConstraint,
                                                                                  optimizationOptions.isTriggeredByGoalViolation(),
                                                                                  BALANCE_MARGIN,
                                                                                  false);

    _balanceLowerThreshold = GoalUtils.computeResourceUtilizationBalanceThreshold(avgUtilizationPercentage,
                                                                                  resource(),
                                                                                  _balancingConstraint,
                                                                                  optimizationOptions.isTriggeredByGoalViolation(),
                                                                                  BALANCE_MARGIN,
                                                                                  true);
    // Identify whether the cluster is in a low utilization state.
    _isLowUtilization = avgUtilizationPercentage <= _balancingConstraint.lowUtilizationThreshold(resource());
    if (_isLowUtilization) {
      // Identify a typical broker capacity to be used in recommendations in case the cluster is over-provisioned.
      // To make sure that the recommended brokers to drop in an heterogeneous cluster is a positive number, the typical broker must have
      // the max capacity.
      int typicalBrokerId = brokerIdWithMaxCapacity(clusterModel);
      double typicalCapacity = clusterModel.broker(typicalBrokerId).capacityFor(resource());

      // Any capacity greater than the allowed capacity may yield over-provisioning.
      double allowedCapacity = resourceUtilization / _balancingConstraint.lowUtilizationThreshold(resource());
      int allowedNumBrokers = (int) (allowedCapacity / typicalCapacity);
      int numBrokersToDrop = Math.max(_brokersAllowedReplicaMove.size() - allowedNumBrokers, 1);
      _overProvisionedRecommendation = new ProvisionRecommendation.Builder(ProvisionStatus.OVER_PROVISIONED)
          .numBrokers(numBrokersToDrop).typicalBrokerCapacity(typicalCapacity).typicalBrokerId(typicalBrokerId).resource(resource()).build();
    }
  }

  private int brokerIdWithMaxCapacity(ClusterModel clusterModel) {
    int brokerIdWithMaxCapacity = -1;
    double maxCapacity = 0.0;

    for (int brokerId : _brokersAllowedReplicaMove) {
      double brokerCapacity = clusterModel.broker(brokerId).capacityFor(resource());
      if (brokerCapacity > maxCapacity) {
        brokerIdWithMaxCapacity = brokerId;
        maxCapacity = brokerCapacity;
      }
    }

    return brokerIdWithMaxCapacity;
  }

  /**
   * Update the current resource that is being balanced if there are still resources to be balanced, finish otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    Set<Integer> brokerIdsAboveBalanceUpperLimit = new HashSet<>();
    Set<Integer> brokerIdsUnderBalanceLowerLimit = new HashSet<>();
    // Log broker Ids over balancing limit.
    // While proposals exclude the excludedTopics, the balance still considers utilization of the excludedTopic replicas.
    for (Broker broker : clusterModel.aliveBrokers()) {
      if (!isLoadUnderBalanceUpperLimit(broker)) {
        brokerIdsAboveBalanceUpperLimit.add(broker.id());
      }
      if (!isExcludedForReplicaMove(broker) && !isLoadAboveBalanceLowerLimit(broker)) {
        // A broker that is excluded for replica moves cannot be under the balance lower limit.
        brokerIdsUnderBalanceLowerLimit.add(broker.id());
      }
    }
    if (!brokerIdsAboveBalanceUpperLimit.isEmpty()) {
      LOG.debug("Utilization for broker ids:{} {} above the balance limit for:{} after {}.",
                brokerIdsAboveBalanceUpperLimit, (brokerIdsAboveBalanceUpperLimit.size() > 1) ? "are" : "is", resource(),
                (clusterModel.selfHealingEligibleReplicas().isEmpty()) ? "rebalance" : "self-healing");
      _succeeded = false;
    } else if (_isLowUtilization) {
      // Cluster is under a low utilization state and all brokers are under the corresponding balance upper limit.
      _provisionResponse = new ProvisionResponse(ProvisionStatus.OVER_PROVISIONED, _overProvisionedRecommendation, name());
    }
    if (!brokerIdsUnderBalanceLowerLimit.isEmpty()) {
      LOG.debug("Utilization for broker ids:{} {} under the balance limit for:{} after {}.",
                brokerIdsUnderBalanceLowerLimit, (brokerIdsUnderBalanceLowerLimit.size() > 1) ? "are" : "is", resource(),
                (clusterModel.selfHealingEligibleReplicas().isEmpty()) ? "rebalance" : "self-healing");
      _succeeded = false;
    } else if (brokerIdsAboveBalanceUpperLimit.isEmpty() && !_isLowUtilization) {
      // All brokers are within the upper and lower balance limits and the cluster is not under a low utilization state.
      _provisionResponse = new ProvisionResponse(ProvisionStatus.RIGHT_SIZED);
    }
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    try {
      GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    } catch (OptimizationFailureException ofe) {
      if (_fixOfflineReplicasOnly) {
        throw ofe;
      }
      _fixOfflineReplicasOnly = true;
      LOG.info("Ignoring resource balance limit to move replicas from dead brokers/disks.");
      return;
    }
    // Sanity check: No replica should be moved to a broker, which used to host any replica of the same partition on its broken disk.
    GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
    finish();
  }

  /**
   * Check whether the given broker is excluded for replica moves.
   * Such a broker cannot receive replicas, but can give them away.
   *
   * @param broker Broker to check for exclusion from replica moves.
   * @return {@code true} if the given broker is excluded for replica moves, {@code false} otherwise.
   */
  private boolean isExcludedForReplicaMove(Broker broker) {
    return !_brokersAllowedReplicaMove.contains(broker.id());
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
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) {
    int numOfflineReplicas = broker.currentOfflineReplicas().size();

    boolean isExcludedForReplicaMove = isExcludedForReplicaMove(broker);
    boolean requireLessLoad = numOfflineReplicas > 0 || isExcludedForReplicaMove || !isLoadUnderBalanceUpperLimit(broker);
    boolean requireMoreLoad = !isExcludedForReplicaMove && !isLoadAboveBalanceLowerLimit(broker);
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
    long moveStartTimeMs = System.currentTimeMillis();
    if (!clusterModel.newBrokers().isEmpty() && !broker.isNew()) {
      // We have new brokers and the current broker is not a new broker.
      return true;
    }

    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    // If this broker is excluded for leadership, then it can move in only followers.
    boolean moveFollowersOnly = optimizationOptions.excludedBrokersForLeadership().contains(broker.id());
    PriorityQueue<Broker> candidateBrokerPQ = new PriorityQueue<>(_brokerComparator.reversed());

    String replicaSortName = null;
    for (Broker candidate : clusterModel.aliveBrokers()) {
      // Get candidate replicas on candidate broker to try moving load from -- sorted in the order of trial (descending load).
      if (utilization(candidate, resource()) > (isExcludedForReplicaMove(candidate) ? 0.0 : _balanceLowerThreshold)) {
        replicaSortName = sortedCandidateReplicas(candidate,
                                                  clusterModel,
                                                  excludedTopics,
                                                  0.0,
                                                  false,
                                                  moveFollowersOnly,
                                                  resource() == Resource.NW_OUT,
                                                  moveImmigrantsOnly);
        candidateBrokerPQ.add(candidate);
      }
    }

    boolean fastMode = optimizationOptions.fastMode();
    // Stop when all the replicas are leaders for leader movement or there is no replicas can be moved in anymore
    // for replica movement.
    while (!candidateBrokerPQ.isEmpty() && (actionType == INTER_BROKER_REPLICA_MOVEMENT
                                            || (actionType == LEADERSHIP_MOVEMENT && broker.leaderReplicas().size() != broker.replicas().size()))) {
      if (fastMode && remainingTimeMs(_balancingConstraint.fastModePerBrokerMoveTimeoutMs(), moveStartTimeMs) <= 0) {
        LOG.debug("Move load in timeout in fast mode for broker {}.", broker.id());
        break;
      }
      Broker cb = candidateBrokerPQ.poll();
      SortedSet<Replica> candidateReplicasToReceive = cb.trackedSortedReplicas(replicaSortName).sortedReplicas(false);

      // Track (1) whether the iteration is ready to poll the next cb and (2) indices to skip in candidate replicas to receive.
      boolean readyToPollNextCb = false;
      int indicesToSkip = 0;

      while (!readyToPollNextCb) {
        // Track indices that have already been iterated in candidate replicas to receive.
        int iteratedIndices = indicesToSkip;
        int maxIndicesToIterate = candidateReplicasToReceive.size();
        for (Replica replica : candidateReplicasToReceive) {
          // Skip indices of candidate replicas that have already been checked before.
          if (indicesToSkip > 0) {
            indicesToSkip--;
            continue;
          }

          Broker b = maybeApplyBalancingAction(clusterModel, replica, Collections.singletonList(broker), actionType,
                                               optimizedGoals, optimizationOptions);

          // Only need to check status if the action is taken. This will also handle the case that the source broker
          // has nothing to move in. In that case we will never reenqueue that source broker.
          if (b != null) {
            if (isLoadAboveBalanceLowerLimit(broker)) {
              clusterModel.untrackSortedReplicas(replicaSortName);
              return false;
            }
            indicesToSkip = iteratedIndices;

            // If the source broker has a lower utilization than the next broker in the eligible broker in the queue,
            // we reenqueue the source broker and switch to the next broker.
            if (!candidateBrokerPQ.isEmpty() && utilization(cb, resource()) < utilization(candidateBrokerPQ.peek(), resource())) {
              candidateBrokerPQ.add(cb);
              readyToPollNextCb = true;
            }
            break;
          }
          iteratedIndices++;
        }
        if (iteratedIndices == maxIndicesToIterate) {
          readyToPollNextCb = true;
        }
      }
    }
    clusterModel.untrackSortedReplicas(replicaSortName);
    return true;
  }

  /**
   * Get the sorted replicas in the given broker whose (1) topic is not an excluded topic AND (2) do not violate the given load
   * limit in ascending or descending order. Load limit requires the replica load to be (1) above the given limit in
   * descending order, (2) below the given limit in ascending order. Offline replicas have priority over online replicas.
   *
   * @param broker Broker whose replicas will be considered.
   * @param clusterModel The state of the cluster.
   * @param excludedTopics Excluded topics for which the replicas will be remove from the returned candidate replicas.
   * @param loadLimit Load limit determining the lower cutoff in descending order, upper cutoff in ascending order.
   * @param isAscending {@code true} if sort requested in ascending order, {@code false} otherwise.
   * @param followersOnly Candidate replicas contain only the followers.
   * @param leadersOnly Candidate replicas contain only the leaders.
   * @param immigrantsOnly Candidate replicas contain only the immigrant replicas.
   * @return The name of tracked sorted replicas.
   */
  private String sortedCandidateReplicas(Broker broker,
                                         ClusterModel clusterModel,
                                         Set<String> excludedTopics,
                                         double loadLimit,
                                         boolean isAscending,
                                         boolean followersOnly,
                                         boolean leadersOnly,
                                         boolean immigrantsOnly) {
    SortedReplicasHelper helper = new SortedReplicasHelper();
    helper.maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectFollowers(), followersOnly)
          .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectLeaders(), leadersOnly)
          .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(), immigrantsOnly)
          .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                 !excludedTopics.isEmpty())
          .maybeAddPriorityFunc(ReplicaSortFunctionFactory.prioritizeOfflineReplicas(),
                                !clusterModel.selfHealingEligibleReplicas().isEmpty());
    if (isAscending) {
      helper.maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBelowLimit(resource(), loadLimit), loadLimit < Double.MAX_VALUE)
            .setScoreFunc(ReplicaSortFunctionFactory.sortByMetricGroupValue(resource().name()));
    } else {
      helper.addSelectionFunc(ReplicaSortFunctionFactory.selectReplicasAboveLimit(resource(), loadLimit))
            .setScoreFunc(ReplicaSortFunctionFactory.reverseSortByMetricGroupValue(resource().name()));
    }
    String replicaSortName = replicaSortName(this, !isAscending, leadersOnly);
    helper.trackSortedReplicasFor(replicaSortName, broker);
    return replicaSortName;
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

  private double getMinReplicaLoad(SortedSet<Replica> sortedReplicas) {
    // Compare the load of the first replica with the first online replica.
    double minReplicaLoad = sortedReplicas.first().load().expectedUtilizationFor(resource());
    for (Replica replica : sortedReplicas) {
      if (replica.isCurrentOffline()) {
        continue;
      } else if (replica.load().expectedUtilizationFor(resource()) < minReplicaLoad) {
        minReplicaLoad = replica.load().expectedUtilizationFor(resource());
      }
      break;
    }
    return minReplicaLoad;
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

    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    // Get the replicas to swap.
    String sourceReplicaSortName = sortedCandidateReplicas(broker,
                                                           clusterModel,
                                                           excludedTopics,
                                                           0.0,
                                                           false,
                                                           false,
                                                           resource() == Resource.NW_OUT,
                                                           moveImmigrantsOnly);
    SortedSet<Replica> sourceReplicas = broker.trackedSortedReplicas(sourceReplicaSortName).sortedReplicas(false);
    if (sourceReplicas.isEmpty()) {
      // Source broker has no filtered replica to swap.
      broker.untrackSortedReplicas(sourceReplicaSortName);
      return true;
    }

    // If this broker is excluded for leadership, then it can swapped with only followers.
    double maxSourceReplicaLoad = getMaxReplicaLoad(sourceReplicas);
    boolean swapWithFollowersOnly = optimizationOptions.excludedBrokersForLeadership().contains(broker.id());
    PriorityQueue<Broker> candidateBrokerPQ = new PriorityQueue<>(_brokerComparator);
    String candidateReplicaSortName = null;
    for (Broker candidate : clusterModel.aliveBrokersUnderThreshold(resource(), _balanceUpperThreshold)
                                        .stream().filter(b -> !b.replicas().isEmpty()).collect(Collectors.toSet())) {
      // Get candidate replicas on candidate broker to try swapping with -- sorted in the order of trial (ascending load).
      candidateReplicaSortName = sortedCandidateReplicas(candidate,
                                                         clusterModel,
                                                         excludedTopics,
                                                         maxSourceReplicaLoad,
                                                         true,
                                                         swapWithFollowersOnly,
                                                         false,
                                                         moveImmigrantsOnly);
      candidateBrokerPQ.add(candidate);
    }

    long perBrokerSwapTimeoutMs = 2 * _balancingConstraint.fastModePerBrokerMoveTimeoutMs();
    while (!candidateBrokerPQ.isEmpty()) {
      if (remainingTimeMs(perBrokerSwapTimeoutMs, swapStartTimeMs) <= 0) {
        LOG.debug("Swap load out timeout for broker {}.", broker.id());
        break;
      }

      Broker cb = candidateBrokerPQ.poll();
      Replica swappedInReplica = null;
      for (Replica sourceReplica : sourceReplicas) {
        // Try swapping the source with the candidate replicas. Get the swapped in replica if successful, null otherwise.
        Replica swappedIn = maybeApplySwapAction(clusterModel,
                                                 sourceReplica,
                                                 cb.trackedSortedReplicas(candidateReplicaSortName).sortedReplicas(false),
                                                 optimizedGoals,
                                                 optimizationOptions);
        if (swappedIn != null) {
          if (isLoadUnderBalanceUpperLimit(broker)) {
            // Successfully balanced this broker by swapping in.
            clusterModel.clearSortedReplicas();
            return false;
          }
          // Add swapped in/out replica for updating the list of replicas in source broker.
          swappedInReplica = swappedIn;
          break;
        } else if (remainingTimeMs(perBrokerSwapTimeoutMs, swapStartTimeMs) <= 0) {
          LOG.debug("Swap load out timeout for source replica {}.", sourceReplica);
          clusterModel.clearSortedReplicas();
          return true;
        }
      }

      if (swappedInReplica != null) {
        sourceReplicas = broker.trackedSortedReplicas(sourceReplicaSortName).sortedReplicas(false);
        // The broker is still considered as an eligible candidate replica, because the swap was successful -- i.e. there
        // might be other potential candidate replicas on it to swap with.
        candidateBrokerPQ.add(cb);
      }
    }
    clusterModel.clearSortedReplicas();
    return true;
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

    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    // Get the replicas to swap.
    String sourceReplicaSortName = sortedCandidateReplicas(broker,
                                                           clusterModel,
                                                           excludedTopics,
                                                           Double.MAX_VALUE,
                                                           true,
                                                           false,
                                                           false,
                                                           moveImmigrantsOnly);
    SortedSet<Replica> sourceReplicas = broker.trackedSortedReplicas(sourceReplicaSortName).sortedReplicas(false);
    if (sourceReplicas.isEmpty()) {
      // Source broker has no filtered replica to swap.
      broker.untrackSortedReplicas(sourceReplicaSortName);
      return true;
    }

    // If this broker is excluded for leadership, then it can swapped with only followers.
    double minSourceReplicaLoad = getMinReplicaLoad(sourceReplicas);
    boolean swapWithFollowersOnly = optimizationOptions.excludedBrokersForLeadership().contains(broker.id());
    PriorityQueue<Broker> candidateBrokerPQ = new PriorityQueue<>(_brokerComparator.reversed());
    String candidateReplicaSortName = null;
    for (Broker candidate : clusterModel.aliveBrokersOverThreshold(resource(), _balanceLowerThreshold)) {
      // Get candidate replicas on candidate broker to try swapping with -- sorted in the order of trial (descending load).
      candidateReplicaSortName = sortedCandidateReplicas(candidate,
                                                         clusterModel,
                                                         excludedTopics,
                                                         minSourceReplicaLoad,
                                                         false,
                                                         swapWithFollowersOnly,
                                                         resource() == Resource.NW_OUT,
                                                         moveImmigrantsOnly);
      candidateBrokerPQ.add(candidate);
    }

    long perBrokerSwapTimeoutMs = 2 * _balancingConstraint.fastModePerBrokerMoveTimeoutMs();
    while (!candidateBrokerPQ.isEmpty()) {
      if (remainingTimeMs(perBrokerSwapTimeoutMs, swapStartTimeMs) <= 0) {
        LOG.debug("Swap load in timeout for broker {}.", broker.id());
        break;
      }
      Broker cb = candidateBrokerPQ.poll();

      Replica swappedInReplica = null;
      for (Replica sourceReplica : sourceReplicas) {
        // Try swapping the source with the candidate replicas. Get the swapped in replica if successful, null otherwise.
        SortedSet<Replica> candidateReplicas = cb.trackedSortedReplicas(candidateReplicaSortName).sortedReplicas(false);
        Replica swappedIn = maybeApplySwapAction(clusterModel,
                                                 sourceReplica,
                                                 candidateReplicas,
                                                 optimizedGoals,
                                                 optimizationOptions);
        if (swappedIn != null) {
          if (isLoadAboveBalanceLowerLimit(broker)) {
            // Successfully balanced this broker by swapping in.
            clusterModel.clearSortedReplicas();
            return false;
          }
          // Add swapped in/out replica for updating the list of replicas in source broker.
          swappedInReplica = swappedIn;
          break;
        } else if (remainingTimeMs(perBrokerSwapTimeoutMs, swapStartTimeMs) <= 0) {
          LOG.debug("Swap load in timeout for source replica {}.", sourceReplica);
          clusterModel.clearSortedReplicas();
          return true;
        }
      }

      if (swappedInReplica != null) {
        sourceReplicas = broker.trackedSortedReplicas(sourceReplicaSortName).sortedReplicas(false);
        // The broker is still considered as an eligible candidate replica, because the swap was successful -- i.e. there
        // might be other potential candidate replicas on it to swap with.
        candidateBrokerPQ.add(cb);
      }
    }
    clusterModel.clearSortedReplicas();
    return true;
  }

  private boolean rebalanceByMovingLoadOut(Broker broker,
                                           ClusterModel clusterModel,
                                           Set<Goal> optimizedGoals,
                                           ActionType actionType,
                                           OptimizationOptions optimizationOptions) {
    long moveStartTimeMs = System.currentTimeMillis();
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    // Get the eligible brokers.
    SortedSet<Broker> candidateBrokers = new TreeSet<>(
        Comparator.comparingDouble((Broker b) -> utilization(b, resource())).thenComparingInt(Broker::id));
    if (_fixOfflineReplicasOnly) {
      candidateBrokers.addAll(clusterModel.aliveBrokers());
    } else {
      candidateBrokers.addAll(clusterModel.aliveBrokersUnderThreshold(resource(), _balanceUpperThreshold));
    }

    // Get the replicas to rebalance.
    new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectLeaders(),
                                                     actionType == LEADERSHIP_MOVEMENT)
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(),
                                                     optimizationOptions.onlyMoveImmigrantReplicas())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrantOrOfflineReplicas(),
                                                     !clusterModel.selfHealingEligibleReplicas().isEmpty() && broker.isAlive())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .maybeAddPriorityFunc(ReplicaSortFunctionFactory.prioritizeOfflineReplicas(),
                                                    !clusterModel.selfHealingEligibleReplicas().isEmpty())
                              .maybeAddPriorityFunc(ReplicaSortFunctionFactory.prioritizeImmigrants(),
                                                    !optimizationOptions.onlyMoveImmigrantReplicas())
                              .setScoreFunc(ReplicaSortFunctionFactory.reverseSortByMetricGroupValue(resource().name()))
                              .trackSortedReplicasFor(replicaSortName(this, true, actionType == LEADERSHIP_MOVEMENT), broker);
    SortedSet<Replica> replicasToMove = broker.trackedSortedReplicas(replicaSortName(this, true, actionType == LEADERSHIP_MOVEMENT))
                                              .sortedReplicas(true);

    // If the source broker is excluded for replica move, set its upper limit to 0.
    double balanceUpperThresholdForSourceBroker = isExcludedForReplicaMove(broker) ? 0 : _balanceUpperThreshold;
    boolean fastMode = optimizationOptions.fastMode();
    // Now let's move things around.
    for (Replica replica : replicasToMove) {
      if (!replica.isCurrentOffline()) {
        if (fastMode && remainingTimeMs(_balancingConstraint.fastModePerBrokerMoveTimeoutMs(), moveStartTimeMs) <= 0) {
          LOG.debug("Move load out timeout in fast mode for broker {}.", broker.id());
          break;
        }
        // It does not make sense to move an online replica without utilization from a live broker.
        if (replica.load().expectedUtilizationFor(resource()) == 0.0) {
          break;
        }
      }

      // An optimization for leader movements.
      SortedSet<Broker> eligibleBrokers;
      if (actionType == LEADERSHIP_MOVEMENT) {
        eligibleBrokers = new TreeSet<>(Comparator.comparingDouble((Broker b) -> utilization(b, resource()))
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
        if (isLoadUnderBalanceUpperLimit(broker, balanceUpperThresholdForSourceBroker)
            && !(_fixOfflineReplicasOnly && !broker.currentOfflineReplicas().isEmpty())) {
          broker.clearSortedReplicas();
          return false;
        }
        // Remove and reinsert the broker so the order is correct.
        candidateBrokers.remove(b);
        if (utilization(b, resource()) < _balanceUpperThreshold) {
          candidateBrokers.add(b);
        }
      }
    }
    // If all the replicas has been moved away from the broker and we still reach here, that means the broker
    // capacity is negative, i.e. the broker is dead. So as long as there is no replicas on the broker anymore
    // we consider it as not over limit.
    broker.clearSortedReplicas();
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

  private boolean isLoadUnderBalanceUpperLimit(Broker broker, double balanceUpperThresholdForBroker) {
    // The action does not matter here because the load is null.
    return isLoadUnderBalanceUpperLimitAfterChange(null, broker, REMOVE, balanceUpperThresholdForBroker);
  }

  private boolean isLoadAboveBalanceLowerLimitAfterChange(Load load,
                                                          Broker broker,
                                                          ChangeType changeType) {
    double utilizationDelta = load == null ? 0 : load.expectedUtilizationFor(resource());

    double brokerBalanceLowerLimit = broker.capacityFor(resource()) * _balanceLowerThreshold;
    double brokerUtilization = broker.load().expectedUtilizationFor(resource());
    boolean isBrokerAboveLowerLimit = changeType == ADD ? brokerUtilization + utilizationDelta >= brokerBalanceLowerLimit
                                                        : brokerUtilization - utilizationDelta >= brokerBalanceLowerLimit;

    if (resource().isHostResource()) {
      double hostBalanceLowerLimit = broker.host().capacityFor(resource()) * _balanceLowerThreshold;
      double hostUtilization = broker.host().load().expectedUtilizationFor(resource());
      boolean isHostAboveLowerLimit = changeType == ADD ? hostUtilization + utilizationDelta >= hostBalanceLowerLimit
                                                        : hostUtilization - utilizationDelta >= hostBalanceLowerLimit;
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
                                                          ChangeType changeType,
                                                          double balanceUpperThresholdForBroker) {
    double utilizationDelta = load == null ? 0 : load.expectedUtilizationFor(resource());

    double brokerBalanceUpperLimit = broker.capacityFor(resource()) * balanceUpperThresholdForBroker;
    double brokerUtilization = broker.load().expectedUtilizationFor(resource());
    boolean isBrokerUnderUpperLimit = changeType == ADD ? brokerUtilization + utilizationDelta <= brokerBalanceUpperLimit
                                                        : brokerUtilization - utilizationDelta <= brokerBalanceUpperLimit;

    if (resource().isHostResource()) {
      double hostBalanceUpperLimit = broker.host().capacityFor(resource()) * balanceUpperThresholdForBroker;
      double hostUtilization = broker.host().load().expectedUtilizationFor(resource());
      boolean isHostUnderUpperLimit = changeType == ADD ? hostUtilization + utilizationDelta <= hostBalanceUpperLimit
                                                        : hostUtilization - utilizationDelta <= hostBalanceUpperLimit;
      // As long as either the host or the broker is under the limit, we claim the host resource utilization is
      // under the limit. If the host is above limit, there must be at least one broker above limit. We should just
      // move load off that broker.
      return isHostUnderUpperLimit || isBrokerUnderUpperLimit;
    } else {
      return isBrokerUnderUpperLimit;
    }
  }

  private boolean isLoadUnderBalanceUpperLimitAfterChange(Load load,
                                                          Broker broker,
                                                          ChangeType changeType) {
    return isLoadUnderBalanceUpperLimitAfterChange(load, broker, changeType, _balanceUpperThreshold);
  }

  /**
   * Check if moving the source replica to destination broker would decrease the utilization difference between source
   * and destination brokers.
   *
   * @param sourceReplica Source replica to be moved.
   * @param destinationBroker Destination broker to receive the source replica utilization.
   * @return {@code true} if the change would lead to a better balance, {@code false} otherwise.
   */
  private boolean isAcceptableAfterReplicaMove(Replica sourceReplica, Broker destinationBroker) {
    double sourceUtilizationDelta = - sourceReplica.load().expectedUtilizationFor(resource());
    return isGettingMoreBalanced(sourceReplica.broker(), sourceUtilizationDelta, destinationBroker);
  }

  /**
   * Check if the swap would decrease the utilization difference between source and destination brokers.
   *
   * @param sourceReplica Source replica to be swapped.
   * @param destinationReplica Destination replica to be swapped.
   * @return {@code true} if the change would lead to a better balance, {@code false} otherwise.
   */
  private boolean isSelfSatisfiedAfterSwap(Replica sourceReplica, Replica destinationReplica) {
    double sourceUtilizationDelta = destinationReplica.load().expectedUtilizationFor(resource())
                                    - sourceReplica.load().expectedUtilizationFor(resource());
    return isGettingMoreBalanced(sourceReplica.broker(), sourceUtilizationDelta, destinationReplica.broker());
  }

  /**
   * Check if the utilization difference between source and destination brokers would decrease after the source
   * utilization delta is removed from the destination and added to source broker.
   *
   * @param sourceBroker Source broker.
   * @param sourceUtilizationDelta Utilization that would be removed from the destination and added to source broker.
   * @param destinationBroker Destination broker.
   * @return {@code true} if the change would lead to a better balance, {@code false} otherwise.
   */
  private boolean isGettingMoreBalanced(Broker sourceBroker, double sourceUtilizationDelta, Broker destinationBroker) {
    double sourceBrokerUtilization = sourceBroker.load().expectedUtilizationFor(resource());
    double destinationBrokerUtilization = destinationBroker.load().expectedUtilizationFor(resource());
    double sourceBrokerCapacity = sourceBroker.capacityFor(resource());
    double destinationBrokerCapacity = destinationBroker.capacityFor(resource());

    double prevDiff = (sourceBrokerUtilization / sourceBrokerCapacity) - (destinationBrokerUtilization / destinationBrokerCapacity);
    double nextDiff = prevDiff + (sourceUtilizationDelta / sourceBrokerCapacity) + (sourceUtilizationDelta / destinationBrokerCapacity);

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
