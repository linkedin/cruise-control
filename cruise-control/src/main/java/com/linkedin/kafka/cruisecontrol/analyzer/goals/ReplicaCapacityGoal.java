/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.HashSet;
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
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;


/**
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
      case INTER_BROKER_REPLICA_MOVEMENT:
      case REPLICA_ADDITION:
        Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
        return destinationBroker.replicas().size() < _balancingConstraint.maxReplicasPerBroker() ? ACCEPT : REPLICA_REJECT;
      case INTER_BROKER_REPLICA_SWAP:
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
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
  }

  @Override
  public String name() {
    return ReplicaCapacityGoal.class.getSimpleName();
  }

  @Override
  public boolean isHardGoal() {
    return true;
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
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    List<String> topicsToRebalance = new ArrayList<>(clusterModel.topics());
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    topicsToRebalance.removeAll(excludedTopics);
    if (topicsToRebalance.isEmpty()) {
      LOG.warn("All topics are excluded from {}.", name());
    }

    // Sanity check: excluded topic replicas in a broker cannot exceed the max number of allowed replicas per broker.
    int totalReplicasInCluster = 0;
    for (Broker broker : brokersToBalance(clusterModel)) {
      // Calculate total number of replicas for the next sanity check.
      totalReplicasInCluster += broker.replicas().size();
      if (!broker.isAlive()) {
        _isSelfHealingMode = true;
        continue;
      }
      Set<Replica> excludedReplicasInBroker = new HashSet<>();
      for (String topic : excludedTopics) {
        excludedReplicasInBroker.addAll(broker.replicasOfTopicInBroker(topic));
      }
      if (broker.state() == Broker.State.BAD_DISKS) {
        _isSelfHealingMode = true;
        excludedReplicasInBroker.removeAll(broker.currentOfflineReplicas());
      }

      if (excludedReplicasInBroker.size() > _balancingConstraint.maxReplicasPerBroker()) {
        String mitigation = GoalUtils.mitigationForOptimizationFailures(optimizationOptions);
        throw new OptimizationFailureException(
            String.format("[%s] Replicas of excluded topics in broker: %d exceeds the maximum allowed number of replicas per "
                              + "broker: %d. %s", name(), excludedReplicasInBroker.size(), _balancingConstraint.maxReplicasPerBroker(), mitigation));
      }
    }

    // Sanity check: total replicas in the cluster cannot be more than the allowed replicas in the cluster.
    long maxReplicasInCluster = _balancingConstraint.maxReplicasPerBroker() * clusterModel.aliveBrokers().size();
    if (totalReplicasInCluster > maxReplicasInCluster) {
      throw new OptimizationFailureException(
          String.format("[%s] Total replicas in cluster: %d exceeds the maximum allowed replicas in cluster: %d (Alive "
                            + "brokers: %d, Allowed number of replicas per broker: %d).",
                        name(), totalReplicasInCluster, maxReplicasInCluster, clusterModel.aliveBrokers().size(),
                        _balancingConstraint.maxReplicasPerBroker()));
    }

    // Filter out some replicas based on optimization options.
    new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(),
                                                     optimizationOptions.onlyMoveImmigrantReplicas())
                              .addSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics))
                              .trackSortedReplicasFor(replicaSortName(this, false, false), clusterModel);
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
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    // Sanity check: No replica should be moved to a broker, which used to host any replica of the same partition on its broken disk.
    GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());

    if (!_isSelfHealingMode) {
      // One pass in non-self-healing mode is sufficient to satisfy or alert impossibility of this goal.
      // Sanity check to confirm that the final distribution has less than the allowed number of replicas per broker.
      ensureReplicaCapacitySatisfied(clusterModel, optimizationOptions);
      finish();
    } else {
      _isSelfHealingMode = false;
    }
  }

  @Override
  public void finish() {
    _finished = true;
  }

  /**
   * Sanity check: Ensure that the replica capacity per broker is not exceeded.
   *
   * @param clusterModel The cluster model.
   * @param optimizationOptions Options to take into account during optimization.
   */
  private void ensureReplicaCapacitySatisfied(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    for (Broker broker : brokersToBalance(clusterModel)) {
      int numBrokerReplicas = broker.replicas().size();
      if (numBrokerReplicas > _balancingConstraint.maxReplicasPerBroker()) {
        String mitigation = GoalUtils.mitigationForOptimizationFailures(optimizationOptions);
        throw new OptimizationFailureException(
            String.format("[%s] Replicas in broker %d exceeds the maximum allowed number of replicas per broker: %d. %s",
                          name(), numBrokerReplicas, _balancingConstraint.maxReplicasPerBroker(), mitigation));
      }
    }
  }

  /**
   * Rebalance the given broker without violating the constraints of the current goal and optimized goals.
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    LOG.debug("balancing broker {}, optimized goals = {}", broker, optimizedGoals);
    for (Replica replica : broker.trackedSortedReplicas(replicaSortName(this, false, false)).sortedReplicas(true)) {
      boolean isReplicaOffline = replica.isCurrentOffline();
      if (broker.replicas().size() <= _balancingConstraint.maxReplicasPerBroker() && !isReplicaOffline) {
        // The loop uses a TreeSet over replicas with the default replica comparator. This comparator prioritizes offline
        // replicas; hence, if the current replica is not offline, it means there is no other offline replica on the broker.
        break;
      }

      // The goal requirements are violated. Move replica to an eligible broker.
      List<Broker> eligibleBrokers =
          eligibleBrokers(replica, clusterModel).stream().map(BrokerReplicaCount::broker).collect(Collectors.toList());

      Broker b = maybeApplyBalancingAction(clusterModel, replica, eligibleBrokers, ActionType.INTER_BROKER_REPLICA_MOVEMENT,
                                           optimizedGoals, optimizationOptions);
      if (b == null) {
        if (!broker.isAlive()) {
          // If the replica resides in a dead broker, throw an exception!
          throw new OptimizationFailureException(
              String.format("[%s] Failed to move dead broker replica %s of partition %s to a broker in %s. Per broker limit: "
                            + "%d for brokers: %s", name(), replica, clusterModel.partition(replica.topicPartition()),
                            eligibleBrokers, _balancingConstraint.maxReplicasPerBroker(), clusterModel.brokers()));
        } else if (isReplicaOffline) {
          // If the replica is offline on a broker with bad disk, throw an exception!
          throw new OptimizationFailureException(
              String.format("[%s] Failed to move offline replica %s of partition %s to a broker in %s. Per broker limit: "
                            + "%d for brokers: %s", name(), replica, clusterModel.partition(replica.topicPartition()),
                            eligibleBrokers, _balancingConstraint.maxReplicasPerBroker(), clusterModel.brokers()));
        }
        LOG.debug("Failed to move replica {} to any broker in {}.", replica, eligibleBrokers);
      }
    }
  }

  /**
   * Get a list of replica capacity eligible brokers for the given replica in the given cluster.
   *
   * A alive destination broker is eligible for a given replica if
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

    for (Broker broker : clusterModel.aliveBrokers()) {
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
