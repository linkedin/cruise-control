/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;


/**
 * Class for achieving the following hard goal:
 * HARD GOAL: Generate replica movement proposals to ensure that each broker has less than the given number of replicas.
 */
public class ReplicaCapacityGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaCapacityGoal.class);
  private boolean _isSelfHealingMode;

  /**
   * Constructor for Replica Capacity Goal.
   */
  public ReplicaCapacityGoal() {
    _isSelfHealingMode = false;
  }

  /**
   * Package private for unit test.
   */
  ReplicaCapacityGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
    _isSelfHealingMode = false;
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
   * Check whether given action is acceptable by this goal. An action is acceptable by a goal if it satisfies
   * requirements of the goal. Requirements(hard goal): replica capacity goal.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    switch (action.balancingAction()) {
      case REPLICA_MOVEMENT:
      case REPLICA_ADDITION:
        Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
        return destinationBroker.replicas().size() < _balancingConstraint.maxReplicasPerBroker() ? ACCEPT : REPLICA_REJECT;
      case REPLICA_SWAP:
      case LEADERSHIP_MOVEMENT:
      case REPLICA_DELETION:
        return ACCEPT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new ReplicaCapacityGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(1, 0.0, true);
  }

  @Override
  public String name() {
    return ReplicaCapacityGoal.class.getSimpleName();
  }

  /**
   * This is a hard goal; hence, the proposals are not limited to dead broker replicas in case of self-healing.
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
   * This is a hard goal; hence, the proposals are not limited to dead broker replicas in case of self-healing.
   * Sanity Check: Each node has sufficient number of replicas that can be moved to satisfy the replica capacity goal.
   *
   *  @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposals.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, Set<String> excludedTopics) throws OptimizationFailureException {
    List<String> topicsToRebalance = new ArrayList<>(clusterModel.topics());
    topicsToRebalance.removeAll(excludedTopics);
    if (topicsToRebalance.isEmpty()) {
      LOG.warn("All topics are excluded from {}.", name());
    }

    // Sanity check: excluded topic replicas in a broker cannot exceed the max number of allowed replicas per broker.
    int totalReplicasInCluster = 0;
    for (Broker broker : brokersToBalance(clusterModel)) {
      if (!broker.isAlive()) {
        _isSelfHealingMode = true;
        continue;
      }
      int excludedReplicasInBroker = 0;
      for (String topic : excludedTopics) {
        excludedReplicasInBroker += broker.replicasOfTopicInBroker(topic).size();
      }

      if (excludedReplicasInBroker > _balancingConstraint.maxReplicasPerBroker()) {
        throw new OptimizationFailureException(String.format("Replicas of excluded topics in broker: %d exceeds the maximum "
            + "allowed number of replicas: %d.", excludedReplicasInBroker, _balancingConstraint.maxReplicasPerBroker()));
      }
      // Calculate total number of replicas for the next sanity check.
      totalReplicasInCluster += broker.replicas().size();
    }

    // Sanity check: total replicas in the cluster cannot be more than the allowed replicas in the cluster.
    long maxReplicasInCluster = _balancingConstraint.maxReplicasPerBroker() * clusterModel.healthyBrokers().size();
    if (totalReplicasInCluster > maxReplicasInCluster) {
      throw new OptimizationFailureException(String.format("Total replicas in cluster: %d exceeds the maximum allowed "
          + "replicas in cluster: %d.", totalReplicasInCluster, maxReplicasInCluster));
    }
  }

  /**
   * Check if requirements of this goal are not violated if this action is applied to the given cluster state,
   * false otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model.
   * @return True if requirements of this goal are not violated if this action is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    return destinationBroker.replicas().size() < _balancingConstraint.maxReplicasPerBroker();
  }

  /**
   * Update goal state after one round of self-healing / rebalance.
   *
   *  @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization action.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics) throws OptimizationFailureException {
    // Sanity check: No self-healing eligible replica should remain at a decommissioned broker.
    AnalyzerUtils.ensureNoReplicaOnDeadBrokers(clusterModel);

    if (!_isSelfHealingMode) {
      // One pass in non-self-healing mode is sufficient to satisfy or alert impossibility of this goal.
      // Sanity check to confirm that the final distribution has less than the allowed number of replicas per broker.
      ensureReplicaCapacitySatisfied(clusterModel);
      finish();
    }
    _isSelfHealingMode = false;
  }

  /**
   * Sanity check: Ensure that the replica capacity per broker is not exceeded.
   *
   * @param clusterModel The cluster model.
   */
  private void ensureReplicaCapacitySatisfied(ClusterModel clusterModel) {
    for (Broker broker : brokersToBalance(clusterModel)) {
      int numBrokerReplicas = broker.replicas().size();
      if (numBrokerReplicas > _balancingConstraint.maxReplicasPerBroker()) {
        throw new IllegalStateException(String.format("Replicas in broker: %d exceeds the maximum allowed number of "
            + "replicas: %d.", numBrokerReplicas, _balancingConstraint.maxReplicasPerBroker()));
      }
    }
  }

  /**
   * Rebalance the given broker without violating the constraints of the current goal and optimized goals.
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param excludedTopics The topics that should be excluded from the optimization proposals.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    Set<String> excludedTopics)
      throws OptimizationFailureException {
    LOG.debug("balancing broker {}, optimized goals = {}", broker, optimizedGoals);
    for (Replica replica : new ArrayList<>(broker.replicas())) {
      if (broker.isAlive() && broker.replicas().size() <= _balancingConstraint.maxReplicasPerBroker()) {
        break;
      }
      if (shouldExclude(replica, excludedTopics)) {
        continue;
      }

      // The goal requirements are violated. Move replica to an eligible broker.
      List<Broker> eligibleBrokers =
          eligibleBrokers(replica, clusterModel).stream().map(BrokerReplicaCount::broker).collect(Collectors.toList());

      Broker b = maybeApplyBalancingAction(clusterModel, replica, eligibleBrokers, ActionType.REPLICA_MOVEMENT, optimizedGoals);
      if (b == null) {
        if (!broker.isAlive()) {
          // If the replica resides in a dead broker, throw an exception!
          throw new OptimizationFailureException(String.format("Failed to move dead broker replica %s of partition %s "
                                                               + "to a broker in %s. Limit: %d for brokers: %s", replica,
                                                               clusterModel.partition(replica.topicPartition()),
                                                               eligibleBrokers, _balancingConstraint.maxReplicasPerBroker(),
                                                               clusterModel.brokers()));
        }
        LOG.debug("Failed to move replica {} to any broker in {}.", replica, eligibleBrokers);
      }
    }
  }

  /**
   * Get a list of replica capacity eligible brokers for the given replica in the given cluster.
   *
   * A healthy destination broker is eligible for a given replica if
   * (1) the broker contains less than allowed maximum number of replicas, or
   * (2) If the the self healing mode is true.
   *
   * Returned brokers are sorted by number of replicas on them in ascending order.
   *
   * @param replica      Replica for which a set of replica capacity eligible brokers are requested.
   * @param clusterModel The state of the cluster.
   * @return A list of replica capacity eligible brokers for the given replica in the given cluster.
   */
  private SortedSet<BrokerReplicaCount> eligibleBrokers(Replica replica, ClusterModel clusterModel) {
    // Populate partition rack ids.
    SortedSet<BrokerReplicaCount> eligibleBrokers = new TreeSet<>();

    int sourceBrokerId = replica.broker().id();

    for (Broker broker : clusterModel.healthyBrokers()) {
      if ((_isSelfHealingMode || broker.replicas().size() < _balancingConstraint.maxReplicasPerBroker())
          && broker.id() != sourceBrokerId) {
        eligibleBrokers.add(new BrokerReplicaCount(broker));
      }
    }

    // Return eligible brokers.
    return eligibleBrokers;
  }

  private static class ReplicaCapacityGoalStatsComparator implements ClusterModelStatsComparator {

    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // This goal do not care about stats. The optimization would have already failed if the goal is not met.
      return 0;
    }

    @Override
    public String explainLastComparison() {
      return null;
    }
  }

  /**
   * A helper class for this goal to keep track of the number of replicas assigned to brokers.
   */
  private static class BrokerReplicaCount implements Comparable<BrokerReplicaCount> {
    private final Broker _broker;
    private int _replicaCount;

    BrokerReplicaCount(Broker broker) {
      _broker = broker;
      _replicaCount = broker.replicas().size();
    }

    public Broker broker() {
      return _broker;
    }

    int replicaCount() {
      return _replicaCount;
    }

    @Override
    public int compareTo(BrokerReplicaCount o) {
      if (_replicaCount > o.replicaCount()) {
        return 1;
      } else if (_replicaCount < o.replicaCount()) {
        return -1;
      } else {
        return Integer.compare(_broker.id(), o.broker().id());
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BrokerReplicaCount that = (BrokerReplicaCount) o;
      return _replicaCount == that._replicaCount && _broker.id() == that._broker.id();
    }

    @Override
    public int hashCode() {
      return Objects.hash(_broker.id(), _replicaCount);
    }
  }
}
