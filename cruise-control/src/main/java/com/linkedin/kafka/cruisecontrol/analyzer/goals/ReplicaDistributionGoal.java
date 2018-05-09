/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.BrokerAndSortedReplicas;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
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
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal.ChangeType.ADD;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal.ChangeType.REMOVE;
import static com.linkedin.kafka.cruisecontrol.common.Resource.DISK;


/**
 * Class for achieving the following soft goal:
 * Generate replica movement proposals to ensure that the number of replicas on each broker is
 * <ul>
 * <li>Under: (the average number of replicas per broker) * (1 + replica count balance percentage)</li>
 * <li>Above: (the average number of replicas per broker) * Math.max(0, 1 - replica count balance percentage)</li>
 * </ul>
 * Also see: {@link com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig#REPLICA_COUNT_BALANCE_THRESHOLD_DOC}
 * and {@link #balancePercentageWithMargin()}.
 */
public class ReplicaDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  // Flag to indicate whether the self healing failed to relocate all replicas away from dead brokers in its initial
  // attempt and currently omitting the resource balance limit to relocate remaining replicas.
  private boolean _selfHealingDeadBrokersOnly;
  private final Set<Integer> _brokerIdsAboveBalanceUpperLimit;
  private final Set<Integer> _brokerIdsUnderBalanceLowerLimit;
  private Map<Integer, BrokerAndSortedReplicas> _brokerAndReplicasMap;
  private double _avgReplicasOnHealthyBroker;
  private double _balanceUpperLimit;
  private double _balanceLowerLimit;

  /**
   * Constructor for Replica Distribution Goal.
   */
  public ReplicaDistributionGoal() {
    _brokerIdsAboveBalanceUpperLimit = new HashSet<>();
    _brokerIdsUnderBalanceLowerLimit = new HashSet<>();
  }

  public ReplicaDistributionGoal(BalancingConstraint balancingConstraint) {
    _balancingConstraint = balancingConstraint;
    _brokerIdsAboveBalanceUpperLimit = new HashSet<>();
    _brokerIdsUnderBalanceLowerLimit = new HashSet<>();
  }

  /**
   * To avoid churns, we add a balance margin to the user specified rebalance threshold. e.g. when user sets the
   * threshold to be replicaBalancePercentage, we use (replicaBalancePercentage-1)*balanceMargin instead.
   * @return the rebalance threshold with a margin.
   */
  private double balancePercentageWithMargin() {
    return (_balancingConstraint.replicaBalancePercentage() - 1) * BALANCE_MARGIN;
  }

  /**
   * @return The replica balance upper threshold in percent.
   */
  private double balanceUpperLimit() {
    return _avgReplicasOnHealthyBroker * (1 + balancePercentageWithMargin());
  }

  /**
   * @return The replica balance lower threshold in percent.
   */
  private double balanceLowerLimit() {
    return _avgReplicasOnHealthyBroker * Math.max(0, (1 - balancePercentageWithMargin()));
  }

  /**
   * @deprecated
   * Please use {@link #actionAcceptance(BalancingAction, ClusterModel)} instead.
   */
  @Override
  public boolean isActionAcceptable(BalancingAction action, ClusterModel clusterModel) {
    return actionAcceptance(action, clusterModel) == ACCEPT;
  }

  /**
   * Check whether the given action is acceptable by this goal. An action is acceptable if the number of replicas at
   * (1) the source broker does not go under the allowed limit.
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
      case REPLICA_SWAP:
      case LEADERSHIP_MOVEMENT:
        return ACCEPT;
      case REPLICA_MOVEMENT:
        Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
        Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());

        //Check that destination and source would not become unbalanced.
        return (isReplicaCountUnderBalanceUpperLimitAfterChange(destinationBroker, ADD)
                && isReplicaCountAboveBalanceLowerLimitAfterChange(sourceBroker, REMOVE)) ? ACCEPT : REPLICA_REJECT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  private boolean isReplicaCountUnderBalanceUpperLimitAfterChange(Broker broker, ChangeType changeType) {
    int numReplicas = broker.replicas().size();
    double brokerBalanceUpperLimit = broker.isAlive() ? _balanceUpperLimit : 0;

    return changeType == ADD ? numReplicas + 1 <= brokerBalanceUpperLimit : numReplicas - 1 <= brokerBalanceUpperLimit;
  }

  private boolean isReplicaCountAboveBalanceLowerLimitAfterChange(Broker broker, ChangeType changeType) {
    int numReplicas = broker.replicas().size();
    double brokerBalanceLowerLimit = broker.isAlive() ? _balanceLowerLimit : 0;

    return changeType == ADD ? numReplicas + 1 >= brokerBalanceLowerLimit : numReplicas - 1 >= brokerBalanceLowerLimit;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new ReplicaDistributionGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(1, 0.0, true);
  }

  @Override
  public String name() {
    return ReplicaDistributionGoal.class.getSimpleName();
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
    return clusterModel.brokers();
  }

  /**
   * Initiates replica distribution goal.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposals.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, Set<String> excludedTopics) {
    // Initialize the average replicas on a healthy broker.
    int numReplicasInCluster = clusterModel.getReplicaDistribution().values().stream().mapToInt(List::size).sum();
    _avgReplicasOnHealthyBroker = (numReplicasInCluster / (double) clusterModel.healthyBrokers().size());

    // Log a warning if all replicas are excluded.
    if (clusterModel.topics().equals(excludedTopics)) {
      LOG.warn("All replicas are excluded from {}.", name());
    }

    _brokerAndReplicasMap = new HashMap<>();

    for (Broker broker : clusterModel.brokers()) {
      BrokerAndSortedReplicas bas = new BrokerAndSortedReplicas(broker, broker.replicaComparator(DISK));
      _brokerAndReplicasMap.put(broker.id(), bas);
    }

    _selfHealingDeadBrokersOnly = false;
    _balanceUpperLimit = balanceUpperLimit();
    _balanceLowerLimit = balanceLowerLimit();
  }

  /**
   * Check if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param action     Proposal containing information about
   * @return True if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    // If the source broker is dead and currently self healing dead brokers only, then the proposal must be executed.
    if (!sourceBroker.isAlive() && _selfHealingDeadBrokersOnly) {
      return true;
    }

    //Check that destination and source would not become unbalanced.
    return isReplicaCountUnderBalanceUpperLimitAfterChange(destinationBroker, ADD) &&
        isReplicaCountAboveBalanceLowerLimitAfterChange(sourceBroker, REMOVE);
  }

  /**
   * Update goal state after one round of self-healing / rebalance.
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposal.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws OptimizationFailureException {
    // Log broker Ids over balancing limit.
    // While proposals exclude the excludedTopics, the balance still considers utilization of the excludedTopic replicas.
    if (!_brokerIdsAboveBalanceUpperLimit.isEmpty()) {
      LOG.warn("Replicas count on broker ids:{} {} above the balance limit of {} after {}.",
          _brokerIdsAboveBalanceUpperLimit, (_brokerIdsAboveBalanceUpperLimit.size() > 1) ? "are" : "is",
               _balanceUpperLimit,
               (clusterModel.selfHealingEligibleReplicas().isEmpty()) ? "rebalance" : "self-healing");
      _brokerIdsAboveBalanceUpperLimit.clear();
      _succeeded = false;
    }
    if (!_brokerIdsUnderBalanceLowerLimit.isEmpty()) {
      LOG.warn("Replica count on broker ids:{} {} under the balance limit of {} after {}.",
          _brokerIdsUnderBalanceLowerLimit, (_brokerIdsUnderBalanceLowerLimit.size() > 1) ? "are" : "is",
               _balanceLowerLimit,
               (clusterModel.selfHealingEligibleReplicas().isEmpty()) ? "rebalance" : "self-healing");
      _brokerIdsUnderBalanceLowerLimit.clear();
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
   * Rebalance the given broker without violating the constraints of the current goal and optimized goals.
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
    LOG.debug("Rebalancing broker {} [limits] lower: {} upper: {}.", broker.id(), _balanceLowerLimit, _balanceUpperLimit);
    int numReplicas = broker.replicas().size();
    boolean requireLessReplicas = broker.isAlive() ? numReplicas > _balanceUpperLimit : numReplicas > 0;
    boolean requireMoreReplicas = broker.isAlive() && numReplicas < _balanceLowerLimit;
    if (broker.isAlive() && !requireMoreReplicas && !requireLessReplicas) {
      // return if the broker is already under limit.
      return;
    } else if (!clusterModel.newBrokers().isEmpty() && requireMoreReplicas && !broker.isNew()) {
      // return if we have new brokers and the current broker is not a new broker but require more load.
      return;
    } else if (!clusterModel.deadBrokers().isEmpty() && requireLessReplicas && broker.isAlive()
        && broker.immigrantReplicas().isEmpty()) {
      // return if the cluster is in self-healing mode and the broker requires less load, but does not have any
      // immigrant replicas.
      return;
    }

    // Update broker ids over the balance limit for logging purposes.
    if (requireLessReplicas && rebalanceByMovingReplicasOut(broker, clusterModel, optimizedGoals, excludedTopics)) {
      _brokerIdsAboveBalanceUpperLimit.add(broker.id());
      LOG.debug("Failed to sufficiently decrease replica count in broker {} with replica movements. Replicas: {}.",
                broker.id(), broker.replicas().size());
    } else if (requireMoreReplicas && rebalanceByMovingReplicasIn(broker, clusterModel, optimizedGoals, excludedTopics)) {
      _brokerIdsUnderBalanceLowerLimit.add(broker.id());
      LOG.debug("Failed to sufficiently increase replica count in broker {} with replica movements. Replicas: {}.",
                broker.id(), broker.replicas().size());
    } else {
      LOG.debug("Successfully balanced replica count for broker {} by moving replicas. Replicas: {}",
                broker.id(), broker.replicas().size());
    }
  }

  private boolean rebalanceByMovingReplicasOut(Broker broker,
                                               ClusterModel clusterModel,
                                               Set<Goal> optimizedGoals,
                                               Set<String> excludedTopics) {
    // Get the eligible brokers.
    SortedSet<Broker> candidateBrokers = new TreeSet<>(Comparator.comparingInt((Broker b) -> b.replicas().size()).thenComparingInt(Broker::id));

    candidateBrokers.addAll(_selfHealingDeadBrokersOnly ? clusterModel.healthyBrokers() : clusterModel
        .healthyBrokers()
        .stream()
        .filter(b -> b.replicas().size() < _balanceUpperLimit)
        .collect(Collectors.toSet()));

    BrokerAndSortedReplicas sourceBas = _brokerAndReplicasMap.get(broker.id());
    // Get the replicas to rebalance. Replicas are sorted from smallest to largest disk usage.
    List<Replica> replicasToMove = new ArrayList<>(sourceBas.sortedReplicas());
    // Now let's move things around.
    for (Replica replica : replicasToMove) {
      if (shouldExclude(replica, excludedTopics)) {
        continue;
      }

      Broker b = maybeApplyBalancingAction(clusterModel, replica, candidateBrokers, ActionType.REPLICA_MOVEMENT,
                                           optimizedGoals);
      // Only check if we successfully moved something.
      if (b != null) {
        // Update the global sorted broker set to reflect the replica movement.
        BrokerAndSortedReplicas destBas = _brokerAndReplicasMap.get(broker.id());
        destBas.sortedReplicas().add(replica);
        sourceBas.sortedReplicas().remove(replica);

        if (broker.replicas().size() <= (broker.isAlive() ? _balanceUpperLimit : 0)) {
          return false;
        }
        // Remove and reinsert the broker so the order is correct.
        candidateBrokers.remove(b);
        if (b.replicas().size() < _balanceUpperLimit || _selfHealingDeadBrokersOnly) {
          candidateBrokers.add(b);
        }
      }
    }
    // All the replicas has been moved away from the broker.
    return !broker.replicas().isEmpty();
  }

  private boolean rebalanceByMovingReplicasIn(Broker broker,
                                              ClusterModel clusterModel,
                                              Set<Goal> optimizedGoals,
                                              Set<String> excludedTopics) {
    PriorityQueue<Broker> eligibleBrokers = new PriorityQueue<>((b1, b2) -> {
      int result = Double.compare(b2.replicas().size(), b1.replicas().size());
      return result == 0 ? Integer.compare(b1.id(), b2.id()) : result;
    });

    for (Broker healthyBroker : clusterModel.healthyBrokers()) {
      if (healthyBroker.replicas().size() > _balanceLowerLimit) {
        eligibleBrokers.add(healthyBroker);
      }
    }

    // Remove the destination broker from the global sorted broker set.
    BrokerAndSortedReplicas destBas = _brokerAndReplicasMap.get(broker.id());

    // Stop when no replicas can be moved in anymore.
    while (!eligibleBrokers.isEmpty()) {
      Broker sourceBroker = eligibleBrokers.poll();
      // Remove the source brokerAndReplicas from the sorted broker set.
      BrokerAndSortedReplicas sourceBas = _brokerAndReplicasMap.get(sourceBroker.id());

      Iterator<Replica> sourceReplicaIter = sourceBas.sortedReplicas().iterator();
      while (sourceReplicaIter.hasNext()) {
        Replica replica = sourceReplicaIter.next();
        if (shouldExclude(replica, excludedTopics)) {
          continue;
        }

        Broker b = maybeApplyBalancingAction(clusterModel, replica, Collections.singletonList(broker), ActionType.REPLICA_MOVEMENT, optimizedGoals);
        // Only need to check status if the action is taken. This will also handle the case that the source broker
        // has nothing to move in. In that case we will never reenqueue that source broker.
        if (b != null) {
          // Update the BrokerAndSortedReplicas in the global sorted broker set to ensure consistency.
          sourceReplicaIter.remove();
          destBas.sortedReplicas().add(replica);
          if (broker.replicas().size() >= (broker.isAlive() ? _balanceLowerLimit : 0)) {
            return false;
          }
          // If the source broker has a lower number of replicas than the next broker in the eligible broker in the
          // queue, we reenqueue the source broker and switch to the next broker.
          if (!eligibleBrokers.isEmpty() && sourceBroker.replicas().size() < eligibleBrokers.peek().replicas().size()) {
            eligibleBrokers.add(sourceBroker);
            break;
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
      // Standard deviation of number of replicas over brokers in the current must be less than the pre-optimized stats.
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

  /**
   * Whether bring replica in or out.
   */
  protected enum ChangeType {
    ADD, REMOVE
  }
}
