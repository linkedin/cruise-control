/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import java.util.Collections;
import java.util.Comparator;
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
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.remainingTimeMs;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionAbstractGoal.ChangeType.*;
import static com.linkedin.kafka.cruisecontrol.common.Resource.DISK;


/**
 * Soft goal to generate replica movement proposals to ensure that the number of replicas on each broker is
 * <ul>
 * <li>Under: (the average number of replicas per broker) * (1 + replica count balance percentage)</li>
 * <li>Above: (the average number of replicas per broker) * Math.max(0, 1 - replica count balance percentage)</li>
 * </ul>
 */
public class ReplicaDistributionGoal extends ReplicaDistributionAbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaDistributionGoal.class);

  /**
   * Constructor for Replica Distribution Goal.
   */
  public ReplicaDistributionGoal() {
  }

  public ReplicaDistributionGoal(BalancingConstraint balancingConstraint) {
    this();
    _balancingConstraint = balancingConstraint;
  }

  @Override
  int numInterestedReplicas(ClusterModel clusterModel) {
    return clusterModel.numReplicas();
  }

  /**
   * The rebalance threshold for this goal is set by
   * {@link com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG}
   */
  @Override
  double balancePercentage() {
    return _balancingConstraint.replicaBalancePercentage();
  }

  /**
   * Get the number of brokers that can be dropped if the given cluster is over-provisioned, {@code null} otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @return Number of brokers that can be dropped if the given cluster is over-provisioned, {@code null} otherwise.
   */
  private Integer numBrokersToDrop(ClusterModel clusterModel) {
    int allowedNumBrokers = (int) (numInterestedReplicas(clusterModel) / _balancingConstraint.overprovisionedMaxReplicasPerBroker());
    int numBrokersToDrop = _brokersAllowedReplicaMove.size() - allowedNumBrokers;

    // Regardless of whether the cluster is balanced or not, it could be overprovisioned in terms of number of replicas per broker.
    boolean isOverprovisioned = numBrokersToDrop > 0
                                && clusterModel.aliveBrokers().stream().noneMatch(
                                    b -> b.replicas().size() > _balancingConstraint.overprovisionedMaxReplicasPerBroker());
    return isOverprovisioned ? numBrokersToDrop : null;
  }

  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    super.updateGoalState(clusterModel, optimizationOptions);
    Integer numBrokersToDrop = numBrokersToDrop(clusterModel);
    if (numBrokersToDrop != null) {
      ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.OVER_PROVISIONED)
          .numBrokers(numBrokersToDrop).build();
      _provisionResponse = new ProvisionResponse(ProvisionStatus.OVER_PROVISIONED, recommendation, name());
    } else if (_succeeded) {
      // The cluster is not overprovisioned and all brokers are within the upper and lower balance limits.
      _provisionResponse = new ProvisionResponse(ProvisionStatus.RIGHT_SIZED);
    }
  }

  /**
   * Check whether the given action is acceptable by this goal. An action is acceptable if the number of replicas at
   * (1) the source broker does not go under the allowed limit -- unless the source broker is excluded for replica moves.
   * (2) the destination broker does not go over the allowed limit.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    switch (action.balancingAction()) {
      case INTER_BROKER_REPLICA_SWAP:
        // It is guaranteed that neither source nor destination brokers are excluded for replica moves.
      case LEADERSHIP_MOVEMENT:
        return ACCEPT;
      case INTER_BROKER_REPLICA_MOVEMENT:
        Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
        Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());

        //Check that destination and source would not become unbalanced.
        return (isReplicaCountUnderBalanceUpperLimitAfterChange(destinationBroker, destinationBroker.replicas().size(), ADD)
               && (isExcludedForReplicaMove(sourceBroker)
                   || isReplicaCountAboveBalanceLowerLimitAfterChange(sourceBroker, sourceBroker.replicas().size(), REMOVE)))
               ? ACCEPT : REPLICA_REJECT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new ReplicaDistributionGoalStatsComparator();
  }

  @Override
  public String name() {
    return ReplicaDistributionGoal.class.getSimpleName();
  }

  /**
   * Initiates replica distribution goal.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    super.initGoalState(clusterModel, optimizationOptions);
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    //Sort replicas for each broker in the cluster.
    for (Broker broker : clusterModel.brokers()) {
      new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(),
                                                       optimizationOptions.onlyMoveImmigrantReplicas())
                                .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrantOrOfflineReplicas(),
                                                       !clusterModel.selfHealingEligibleReplicas().isEmpty() && broker.isAlive())
                                .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                       !excludedTopics.isEmpty())
                                .maybeAddPriorityFunc(ReplicaSortFunctionFactory.prioritizeOfflineReplicas(),
                                                      !clusterModel.selfHealingEligibleReplicas().isEmpty())
                                .maybeAddPriorityFunc(ReplicaSortFunctionFactory.prioritizeImmigrants(),
                                                      !optimizationOptions.onlyMoveImmigrantReplicas())
                                .setScoreFunc(ReplicaSortFunctionFactory.sortByMetricGroupValue(DISK.name()))
                                .trackSortedReplicasFor(replicaSortName(this, false, false), broker);
    }
  }

  /**
   * Rebalance the given broker without violating the constraints of the current goal and optimized goals.
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
    LOG.debug("Rebalancing broker {} [limits] lower: {} upper: {}.", broker.id(), _balanceLowerLimit, _balanceUpperLimit);
    int numReplicas = broker.replicas().size();
    int numOfflineReplicas = broker.currentOfflineReplicas().size();
    boolean isExcludedForReplicaMove = isExcludedForReplicaMove(broker);

    // A broker with a bad disk may take new replicas and give away offline replicas.
    boolean requireLessReplicas = numOfflineReplicas > 0 || numReplicas > _balanceUpperLimit || isExcludedForReplicaMove;
    boolean requireMoreReplicas = !isExcludedForReplicaMove && broker.isAlive() && numReplicas - numOfflineReplicas < _balanceLowerLimit;
    if (broker.isAlive() && !requireMoreReplicas && !requireLessReplicas) {
      // return if the broker is already within the limit.
      return;
    } else if (!clusterModel.newBrokers().isEmpty() && !broker.isNew() && !requireLessReplicas) {
      // return if we have new brokers and the current broker is not a new broker and does not require less replicas
      // -- i.e. hence, does not have offline replicas on it.
      return;
    } else if (((!clusterModel.selfHealingEligibleReplicas().isEmpty() && broker.currentOfflineReplicas().isEmpty())
               || optimizationOptions.onlyMoveImmigrantReplicas())
               && requireLessReplicas && broker.immigrantReplicas().isEmpty()) {
      // return if (1) cluster is in self-healing mode or (2) optimization option requires only moving immigrant replicas,
      // and the broker requires less load but does not have any immigrant replicas.
      return;
    }

    // Update broker ids over the balance limit for logging purposes.
    if (requireLessReplicas && rebalanceByMovingReplicasOut(broker, clusterModel, optimizedGoals, optimizationOptions)) {
      _brokerIdsAboveBalanceUpperLimit.add(broker.id());
      LOG.debug("Failed to sufficiently decrease replica count in broker {} with replica movements. Replicas: {}.",
                broker.id(), broker.replicas().size());
    }
    if (requireMoreReplicas && rebalanceByMovingReplicasIn(broker, clusterModel, optimizedGoals, optimizationOptions)) {
      _brokerIdsUnderBalanceLowerLimit.add(broker.id());
      LOG.debug("Failed to sufficiently increase replica count in broker {} with replica movements. Replicas: {}.",
                broker.id(), broker.replicas().size());
    }
    if (!_brokerIdsAboveBalanceUpperLimit.contains(broker.id()) && !_brokerIdsUnderBalanceLowerLimit.contains(broker.id())) {
      LOG.debug("Successfully balanced replica count for broker {} by moving replicas. Replicas: {}",
                broker.id(), broker.replicas().size());
    }
  }

  private boolean rebalanceByMovingReplicasOut(Broker broker,
                                               ClusterModel clusterModel,
                                               Set<Goal> optimizedGoals,
                                               OptimizationOptions optimizationOptions) {
    long moveStartTimeMs = System.currentTimeMillis();
    // Get the eligible brokers.
    SortedSet<Broker> candidateBrokers = new TreeSet<>(Comparator.comparingInt((Broker b) -> b.replicas().size()).thenComparingInt(Broker::id));

    candidateBrokers.addAll(_fixOfflineReplicasOnly ? clusterModel.aliveBrokers() : clusterModel
        .aliveBrokers()
        .stream()
        .filter(b -> b.replicas().size() < _balanceUpperLimit)
        .collect(Collectors.toSet()));

    // If the source broker is excluded for replica move, set its upper limit to 0.
    int balanceUpperLimitForSourceBroker = isExcludedForReplicaMove(broker) ? 0 : _balanceUpperLimit;

    // Now let's move things around.
    boolean wasUnableToMoveOfflineReplica = false;
    boolean fastMode = optimizationOptions.fastMode();
    for (Replica replica : broker.trackedSortedReplicas(replicaSortName(this, false, false)).sortedReplicas(true)) {
      if (!replica.isCurrentOffline()) {
        if (fastMode && remainingTimeMs(_balancingConstraint.fastModePerBrokerMoveTimeoutMs(), moveStartTimeMs) <= 0) {
          LOG.debug("Move replicas out timeout in fast mode for broker {}.", broker.id());
          break;
        }
        if (wasUnableToMoveOfflineReplica && broker.replicas().size() <= balanceUpperLimitForSourceBroker) {
          // Was unable to move offline replicas from the broker, and remaining replica count is under the balance limit.
          return false;
        }
      }

      Broker b = maybeApplyBalancingAction(clusterModel, replica, candidateBrokers, ActionType.INTER_BROKER_REPLICA_MOVEMENT,
                                           optimizedGoals, optimizationOptions);
      // Only check if we successfully moved something.
      if (b != null) {
        if (broker.replicas().size() <= (broker.currentOfflineReplicas().isEmpty() ? balanceUpperLimitForSourceBroker : 0)) {
          return false;
        }
        // Remove and reinsert the broker so the order is correct.
        candidateBrokers.remove(b);
        if (b.replicas().size() < _balanceUpperLimit || _fixOfflineReplicasOnly) {
          candidateBrokers.add(b);
        }
      } else if (replica.isCurrentOffline()) {
        wasUnableToMoveOfflineReplica = true;
      }
    }
    // All the replicas has been moved away from the broker.
    return !broker.replicas().isEmpty();
  }

  private boolean rebalanceByMovingReplicasIn(Broker aliveDestBroker,
                                              ClusterModel clusterModel,
                                              Set<Goal> optimizedGoals,
                                              OptimizationOptions optimizationOptions) {
    long moveStartTimeMs = System.currentTimeMillis();
    PriorityQueue<Broker> eligibleBrokers = new PriorityQueue<>((b1, b2) -> {
      // Brokers are sorted by (1) current offline replica count then (2) all replica count then (3) broker id.
      int resultByOfflineReplicas = Integer.compare(b2.currentOfflineReplicas().size(), b1.currentOfflineReplicas().size());
      if (resultByOfflineReplicas == 0) {
        int resultByAllReplicas = Integer.compare(b2.replicas().size(), b1.replicas().size());
        return resultByAllReplicas == 0 ? Integer.compare(b1.id(), b2.id()) : resultByAllReplicas;
      }
      return resultByOfflineReplicas;
    });

    // Source broker can be dead, alive, or may have bad disks.
    if (_fixOfflineReplicasOnly) {
      clusterModel.brokers().stream().filter(sourceBroker -> sourceBroker.id() != aliveDestBroker.id())
                  .forEach(eligibleBrokers::add);
    } else {
      for (Broker sourceBroker : clusterModel.brokers()) {
        if (sourceBroker.replicas().size() > _balanceLowerLimit || !sourceBroker.currentOfflineReplicas().isEmpty()
            || isExcludedForReplicaMove(sourceBroker)) {
          eligibleBrokers.add(sourceBroker);
        }
      }
    }

    List<Broker> candidateBrokers = Collections.singletonList(aliveDestBroker);
    boolean fastMode = optimizationOptions.fastMode();
    // Stop when no replicas can be moved in anymore.
    while (!eligibleBrokers.isEmpty()) {
      if (fastMode && remainingTimeMs(_balancingConstraint.fastModePerBrokerMoveTimeoutMs(), moveStartTimeMs) <= 0) {
        LOG.debug("Move replicas in timeout in fast mode for broker {}.", aliveDestBroker.id());
        break;
      }
      Broker sourceBroker = eligibleBrokers.poll();
      for (Replica replica : sourceBroker.trackedSortedReplicas(replicaSortName(this, false, false)).sortedReplicas(true)) {
        Broker b = maybeApplyBalancingAction(clusterModel, replica, candidateBrokers, ActionType.INTER_BROKER_REPLICA_MOVEMENT,
                                             optimizedGoals, optimizationOptions);
        // Only need to check status if the action is taken. This will also handle the case that the source broker
        // has nothing to move in. In that case we will never reenqueue that source broker.
        if (b != null) {
          if (aliveDestBroker.replicas().size() >= _balanceLowerLimit) {
            // Note that the broker passed to this method is always alive; hence, there is no need to check if it is dead.
            return false;
          }
          // If the source broker has a lower number of offline replicas or an equal number of offline replicas, but
          // more total replicas than the next broker in the eligible broker in the queue, we reenqueue the source broker
          // and switch to the next broker.
          if (!eligibleBrokers.isEmpty()) {
            int result = Integer.compare(sourceBroker.currentOfflineReplicas().size(),
                                         eligibleBrokers.peek().currentOfflineReplicas().size());
            if (result == -1 || (result == 0 && sourceBroker.replicas().size() < eligibleBrokers.peek().replicas().size())) {
              eligibleBrokers.add(sourceBroker);
              break;
            }
          }
        }
      }
    }
    return true;
  }

  private class ReplicaDistributionGoalStatsComparator implements ClusterModelStatsComparator {
    private String _reasonForLastNegativeResult;
    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // Standard deviation of number of replicas over brokers not excluded for replica moves must be less than the
      // pre-optimized stats.
      double stDev1 = stats1.replicaStats().get(Statistic.ST_DEV).doubleValue();
      double stDev2 = stats2.replicaStats().get(Statistic.ST_DEV).doubleValue();
      int result = AnalyzerUtils.compare(stDev2, stDev1, AnalyzerUtils.EPSILON);
      if (result < 0) {
        _reasonForLastNegativeResult = String.format("Violated %s. [Std Deviation of Replica Distribution] post-"
                                                         + "optimization:%.3f pre-optimization:%.3f", name(), stDev1, stDev2);
      }
      return result;
    }

    @Override
    public String explainLastComparison() {
      return _reasonForLastNegativeResult;
    }
  }
}
