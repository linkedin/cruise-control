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
  // Flag to indicate whether the self healing failed to relocate all offline replicas away from dead brokers or broken
  // disks in its initial attempt and currently omitting the replica balance limit to relocate remaining replicas.
  private boolean _fixOfflineReplicasOnly;
  private final Set<Integer> _brokerIdsAboveBalanceUpperLimit;
  private final Set<Integer> _brokerIdsUnderBalanceLowerLimit;
  private final Map<Integer, BrokerAndSortedReplicas> _brokerAndReplicasMap;
  private double _avgReplicasOnAliveBroker;
  private double _balanceUpperLimit;
  private double _balanceLowerLimit;

  /**
   * Constructor for Replica Distribution Goal.
   */
  public ReplicaDistributionGoal() {
    _brokerIdsAboveBalanceUpperLimit = new HashSet<>();
    _brokerIdsUnderBalanceLowerLimit = new HashSet<>();
    _brokerAndReplicasMap = new HashMap<>();
  }

  public ReplicaDistributionGoal(BalancingConstraint balancingConstraint) {
    this();
    _balancingConstraint = balancingConstraint;
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
    return _avgReplicasOnAliveBroker * (1 + balancePercentageWithMargin());
  }

  /**
   * @return The replica balance lower threshold in percent.
   */
  private double balanceLowerLimit() {
    return _avgReplicasOnAliveBroker * Math.max(0, (1 - balancePercentageWithMargin()));
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
    // Initialize the average replicas on an alive broker.
    int numReplicasInCluster = clusterModel.getReplicaDistribution().values().stream().mapToInt(List::size).sum();
    _avgReplicasOnAliveBroker = (numReplicasInCluster / (double) clusterModel.aliveBrokers().size());

    // Log a warning if all replicas are excluded.
    if (clusterModel.topics().equals(excludedTopics)) {
      LOG.warn("All replicas are excluded from {}.", name());
    }

    for (Broker broker : clusterModel.brokers()) {
      BrokerAndSortedReplicas bas = new BrokerAndSortedReplicas(broker, broker.replicaComparator(DISK));
      _brokerAndReplicasMap.put(broker.id(), bas);
    }

    _fixOfflineReplicasOnly = false;
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
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    // The action must be executed if currently fixing offline replicas only and the offline source replica is proposed
    // to be moved to another broker.
    if (_fixOfflineReplicasOnly && sourceBroker.replica(action.topicPartition()).isCurrentOffline()) {
      return action.balancingAction() == ActionType.REPLICA_MOVEMENT;
    }
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
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
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    try {
      AnalyzerUtils.ensureNoOfflineReplicas(clusterModel);
    } catch (OptimizationFailureException ofe) {
      if (_fixOfflineReplicasOnly) {
        throw ofe;
      }
      _fixOfflineReplicasOnly = true;
      LOG.warn("Omitting resource balance limit to relocate remaining replicas from dead brokers/disks.");
      return;
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
    int numOfflineReplicas = broker.currentOfflineReplicas().size();

    // A broker with a bad disk may take new replicas and give away offline replicas.
    boolean requireLessReplicas = numOfflineReplicas > 0 || (numReplicas > _balanceUpperLimit);
    boolean requireMoreReplicas = broker.isAlive() && numReplicas - numOfflineReplicas < _balanceLowerLimit;
    if (broker.isAlive() && !requireMoreReplicas && !requireLessReplicas) {
      // return if the broker is already within the limit.
      return;
    } else if (!clusterModel.newBrokers().isEmpty() && requireMoreReplicas && !broker.isNew() && !requireLessReplicas) {
      // return if we have new brokers and the current broker is not a new broker but requires: more replicas, but not
      // less replicas -- i.e. hence, does not have offline replicas on it.
      return;
    } else if (!clusterModel.selfHealingEligibleReplicas().isEmpty() && requireLessReplicas
               && broker.currentOfflineReplicas().isEmpty() && broker.immigrantReplicas().isEmpty()) {
      // return if the cluster is in self-healing mode and the broker requires less load, but does not have any
      // offline or immigrant replicas.
      return;
    }

    // Update broker ids over the balance limit for logging purposes.
    if (requireLessReplicas && rebalanceByMovingReplicasOut(broker, clusterModel, optimizedGoals, excludedTopics)) {
      _brokerIdsAboveBalanceUpperLimit.add(broker.id());
      LOG.debug("Failed to sufficiently decrease replica count in broker {} with replica movements. Replicas: {}.",
                broker.id(), broker.replicas().size());
    }
    if (requireMoreReplicas && rebalanceByMovingReplicasIn(broker, clusterModel, optimizedGoals, excludedTopics)) {
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
                                               Set<String> excludedTopics) {
    // Get the eligible brokers.
    SortedSet<Broker> candidateBrokers = new TreeSet<>(Comparator.comparingInt((Broker b) -> b.replicas().size()).thenComparingInt(Broker::id));

    candidateBrokers.addAll(_fixOfflineReplicasOnly ? clusterModel.aliveBrokers() : clusterModel
        .aliveBrokers()
        .stream()
        .filter(b -> b.replicas().size() < _balanceUpperLimit)
        .collect(Collectors.toSet()));

    BrokerAndSortedReplicas sourceBas = _brokerAndReplicasMap.get(broker.id());
    // Get the replicas to rebalance. Replicas are sorted by (1) offline replicas then (2) immigrant replicas then
    // (3) smallest to largest disk usage.
    List<Replica> replicasToMove = new ArrayList<>(sourceBas.sortedReplicas());
    // Now let's move things around.
    boolean wasUnableToMoveOfflineReplica = false;
    for (Replica replica : replicasToMove) {
      if (wasUnableToMoveOfflineReplica && !replica.isCurrentOffline() && broker.replicas().size() <= _balanceUpperLimit) {
        // Was unable to move offline replicas from the broker, and remaining replica count is under the balance limit.
        return false;
      } else if (shouldExclude(replica, excludedTopics)) {
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
        if (broker.replicas().size() <= (broker.currentOfflineReplicas().isEmpty() ? _balanceUpperLimit : 0)) {
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

  private boolean rebalanceByMovingReplicasIn(Broker broker,
                                              ClusterModel clusterModel,
                                              Set<Goal> optimizedGoals,
                                              Set<String> excludedTopics) {
    PriorityQueue<Broker> eligibleBrokers = new PriorityQueue<>((b1, b2) -> {
      // Brokers are sorted by (1) current offline replica count then (2) all replica count then (3) broker id.
      int resultByOfflineReplicas = Integer.compare(b2.currentOfflineReplicas().size(), b1.currentOfflineReplicas().size());
      if (resultByOfflineReplicas == 0) {
        int resultByAllReplicas = Integer.compare(b2.replicas().size(), b1.replicas().size());
        return resultByAllReplicas == 0 ? Integer.compare(b1.id(), b2.id()) : resultByAllReplicas;
      }
      return resultByOfflineReplicas;
    });

    for (Broker aliveBroker : clusterModel.aliveBrokers()) {
      if (aliveBroker.replicas().size() > _balanceLowerLimit || !aliveBroker.currentOfflineReplicas().isEmpty()) {
        eligibleBrokers.add(aliveBroker);
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
