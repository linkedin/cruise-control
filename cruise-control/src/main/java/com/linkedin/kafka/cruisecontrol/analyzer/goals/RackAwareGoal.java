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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.BROKER_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;


/**
 * HARD GOAL: Generate replica movement proposals to provide rack-aware replica distribution.
 */
public class RackAwareGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(RackAwareGoal.class);

  /**
   * Constructor for Rack Capacity Goal.
   */
  public RackAwareGoal() {

  }

  /**
   * Package private for unit test.
   */
  RackAwareGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
  }

  /**
   * Check whether given action is acceptable by this goal. An action is acceptable by a goal if it satisfies
   * requirements of the goal. Requirements(hard goal): rack awareness.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#BROKER_REJECT} if the action is rejected due to violating rack awareness in the destination
   * broker after moving source replica to destination broker, {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    switch (action.balancingAction()) {
      case LEADERSHIP_MOVEMENT:
        return ACCEPT;
      case INTER_BROKER_REPLICA_MOVEMENT:
      case INTER_BROKER_REPLICA_SWAP:
        if (isReplicaMoveViolateRackAwareness(clusterModel,
                                              c -> c.broker(action.sourceBrokerId()).replica(action.topicPartition()),
                                              c -> c.broker(action.destinationBrokerId()))) {
          return BROKER_REJECT;
        }

        if (action.balancingAction() == ActionType.INTER_BROKER_REPLICA_SWAP
            && isReplicaMoveViolateRackAwareness(clusterModel,
                                                 c -> c.broker(action.destinationBrokerId()).replica(action.destinationTopicPartition()),
                                                 c -> c.broker(action.sourceBrokerId()))) {
          return REPLICA_REJECT;
        }
        return ACCEPT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  private boolean isReplicaMoveViolateRackAwareness(ClusterModel clusterModel,
                                                    Function<ClusterModel, Replica> sourceReplicaFunction,
                                                    Function<ClusterModel, Broker> destinationBrokerFunction) {
    Replica sourceReplica = sourceReplicaFunction.apply(clusterModel);
    Broker destinationBroker = destinationBrokerFunction.apply(clusterModel);
    // Destination broker cannot be in a rack that violates rack awareness.
    Set<Broker> partitionBrokers = clusterModel.partition(sourceReplica.topicPartition()).partitionBrokers();
    partitionBrokers.remove(sourceReplica.broker());

    // Remove brokers in partition broker racks except the brokers in replica broker rack.
    for (Broker broker : partitionBrokers) {
      if (broker.rack().brokers().contains(destinationBroker)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new RackAwareGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    // We only need the latest snapshot and include all the topics.
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
  }

  /**
   * Get the name of this goal. Name of a goal provides an identification for the goal in human readable format.
   */
  @Override
  public String name() {
    return RackAwareGoal.class.getSimpleName();
  }

  @Override
  public boolean isHardGoal() {
    return true;
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
   * Sanity Check: There exists sufficient number of racks for achieving rack-awareness.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    // Sanity Check: not enough racks to satisfy rack awareness.
    int numAliveRacks = clusterModel.numAliveRacks();
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    if (!excludedTopics.isEmpty()) {
      int maxReplicationFactorOfIncludedTopics = 1;
      Map<String, Integer> replicationFactorByTopic = clusterModel.replicationFactorByTopic();

      for (Map.Entry<String, Integer> replicationFactorByTopicEntry: replicationFactorByTopic.entrySet()) {
        if (!excludedTopics.contains(replicationFactorByTopicEntry.getKey())) {
          maxReplicationFactorOfIncludedTopics =
              Math.max(maxReplicationFactorOfIncludedTopics, replicationFactorByTopicEntry.getValue());
          if (maxReplicationFactorOfIncludedTopics > numAliveRacks) {
            throw new OptimizationFailureException(
                String.format("[%s] Insufficient number of racks to distribute included replicas (Current: %d, Needed: %d).",
                              name(), numAliveRacks, maxReplicationFactorOfIncludedTopics));
          }
        }
      }
    } else if (clusterModel.maxReplicationFactor() > numAliveRacks) {
      throw new OptimizationFailureException(
          String.format("[%s] Insufficient number of racks to distribute each replica (Current: %d, Needed: %d).",
                        name(), numAliveRacks, clusterModel.maxReplicationFactor()));
    }

    // Filter out some replicas based on optimization options.
    new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(),
                                                     optimizationOptions.onlyMoveImmigrantReplicas())
                              .addSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics))
                              .trackSortedReplicasFor(replicaSortName(this, false, false), clusterModel);
  }

  /**
   * Update goal state.
   * (1) Sanity check: After completion of balancing / self-healing all resources, confirm that replicas of each
   * partition reside at a separate rack and finish.
   * (2) Update the current resource that is being balanced if there are still resources to be balanced.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    // One pass is sufficient to satisfy or alert impossibility of this goal.
    // Sanity check to confirm that the final distribution is rack aware.
    ensureRackAware(clusterModel, optimizationOptions);
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    // Sanity check: No replica should be moved to a broker, which used to host any replica of the same partition on its broken disk.
    GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
    finish();
  }

  @Override
  public void finish() {
    _finished = true;
  }

  /**
   * Rack-awareness violations can be resolved with replica movements.
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
                                    OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    LOG.debug("balancing broker {}, optimized goals = {}", broker, optimizedGoals);
    for (Replica replica : broker.trackedSortedReplicas(replicaSortName(this, false, false)).sortedReplicas(true)) {
      if (broker.isAlive() && !broker.currentOfflineReplicas().contains(replica) && satisfiedRackAwareness(replica, clusterModel)) {
        continue;
      }
      // Rack awareness is violated. Move replica to a broker in another rack.
      if (maybeApplyBalancingAction(clusterModel, replica, rackAwareEligibleBrokers(replica, clusterModel),
                                    ActionType.INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions) == null) {
        throw new OptimizationFailureException(
            String.format("[%s] Violated rack-awareness requirement for broker with id %d.", name(), broker.id()));
      }
    }
  }

  private void ensureRackAware(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    // Sanity check to confirm that the final distribution is rack aware.
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    for (Replica leader : clusterModel.leaderReplicas()) {
      if (excludedTopics.contains(leader.topicPartition().topic())) {
        continue;
      }

      Set<String> replicaBrokersRackIds = new HashSet<>();
      Set<Broker> followerBrokers = new HashSet<>(clusterModel.partition(leader.topicPartition()).followerBrokers());

      // Add rack Id of replicas.
      for (Broker followerBroker : followerBrokers) {
        String followerRackId = followerBroker.rack().id();
        replicaBrokersRackIds.add(followerRackId);
      }
      replicaBrokersRackIds.add(leader.broker().rack().id());
      if (replicaBrokersRackIds.size() != (followerBrokers.size() + 1)) {
        String mitigation = GoalUtils.mitigationForOptimizationFailures(optimizationOptions);
        throw new OptimizationFailureException(String.format("Optimization for goal %s failed for rack-awareness of "
                                                             + "partition %s. Leader (%s) and follower brokers (%s). %s",
                                                             name(), leader.topicPartition(), leader.broker(),
                                                             followerBrokers, mitigation));
      }
    }
  }

  /**
   * Get a list of rack aware eligible brokers for the given replica in the given cluster. A broker is rack aware
   * eligible for a given replica if the broker resides in a rack where no other broker in the same rack contains a
   * replica from the same partition of the given replica.
   *
   * @param replica      Replica for which a set of rack aware eligible brokers are requested.
   * @param clusterModel The state of the cluster.
   * @return A list of rack aware eligible brokers for the given replica in the given cluster.
   */
  private SortedSet<Broker> rackAwareEligibleBrokers(Replica replica, ClusterModel clusterModel) {
    // Populate partition rack ids.
    List<String> partitionRackIds = clusterModel.partition(replica.topicPartition()).partitionBrokers()
        .stream().map(partitionBroker -> partitionBroker.rack().id()).collect(Collectors.toList());

    // Remove rack id of the given replica, but if there is any other replica from the partition residing in the
    // same cluster, keep its rack id in the list.
    partitionRackIds.remove(replica.broker().rack().id());

    SortedSet<Broker> rackAwareEligibleBrokers = new TreeSet<>((o1, o2) -> {
      return Integer.compare(o1.id(), o2.id()); });
    for (Broker broker : clusterModel.aliveBrokers()) {
      if (!partitionRackIds.contains(broker.rack().id())) {
        rackAwareEligibleBrokers.add(broker);
      }
    }
    // Return eligible brokers.
    return rackAwareEligibleBrokers;
  }

  /**
   * Check whether given replica satisfies rack awareness in the given cluster state. Rack awareness requires no more
   * than one replica from a given partition residing in any rack in the cluster.
   *
   * @param replica      Replica to check for other replicas in the same rack.
   * @param clusterModel The state of the cluster.
   * @return True if there is no other replica from the same partition of the given replica in the same rack, false
   * otherwise.
   */
  private boolean satisfiedRackAwareness(Replica replica, ClusterModel clusterModel) {
    String myRackId = replica.broker().rack().id();
    int myBrokerId = replica.broker().id();
    for (Broker partitionBroker : clusterModel.partition(replica.topicPartition()).partitionBrokers()) {
      if (myRackId.equals(partitionBroker.rack().id()) && myBrokerId != partitionBroker.id()) {
        return false;
      }
    }
    return true;
  }

  private static class RackAwareGoalStatsComparator implements ClusterModelStatsComparator {

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
}
