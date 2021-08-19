/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;


/**
 * HARD GOAL: Generate replica movement proposals to provide rack-aware replica distribution.
 */
public class RackAwareGoal extends AbstractRackAwareGoal {
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

  @Override
  protected boolean doesReplicaMoveViolateActionAcceptance(ClusterModel clusterModel, Replica sourceReplica, Broker destinationBroker) {
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
  public String name() {
    return RackAwareGoal.class.getSimpleName();
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
            int missingRacks = maxReplicationFactorOfIncludedTopics - numAliveRacks;
            ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
                .numRacks(missingRacks).build();
            throw new OptimizationFailureException(
                String.format("[%s] Insufficient number of racks to distribute included replicas (Current: %d, Needed: %d).",
                              name(), numAliveRacks, maxReplicationFactorOfIncludedTopics), recommendation);
          }
        }
      }
    } else if (clusterModel.maxReplicationFactor() > numAliveRacks) {
      int missingRacks = clusterModel.maxReplicationFactor() - numAliveRacks;
      ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
          .numRacks(missingRacks).build();
      throw new OptimizationFailureException(
          String.format("[%s] Insufficient number of racks to distribute each replica (Current: %d, Needed: %d).",
                        name(), numAliveRacks, clusterModel.maxReplicationFactor()), recommendation);
    }
    int numExtraRacks = numAliveRacks - clusterModel.maxReplicationFactor();
    if (numExtraRacks >= _balancingConstraint.overprovisionedMinExtraRacks()) {
      int numRacksToDrop = numExtraRacks - _balancingConstraint.overprovisionedMinExtraRacks() + 1;
      ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.OVER_PROVISIONED)
          .numRacks(numRacksToDrop).build();
      _provisionResponse = new ProvisionResponse(ProvisionStatus.OVER_PROVISIONED, recommendation, name());
    }

    // Filter out some replicas based on optimization options.
    new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(),
                                                     optimizationOptions.onlyMoveImmigrantReplicas())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .trackSortedReplicasFor(replicaSortName(this, false, false), clusterModel);
  }

  /**
   * Update goal state.
   * Sanity check: After completion of balancing / self-healing, confirm that replicas of each partition reside at a
   * separate rack.
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
    if (_provisionResponse.status() != ProvisionStatus.OVER_PROVISIONED) {
      _provisionResponse = new ProvisionResponse(ProvisionStatus.RIGHT_SIZED);
    }
    finish();
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
                                    OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    rebalanceForBroker(broker, clusterModel, optimizedGoals, optimizationOptions, true);
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
        int missingRacks = (followerBrokers.size() + 1) - replicaBrokersRackIds.size();
        ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
            .numRacks(missingRacks).build();
        throw new OptimizationFailureException(String.format("[%s] Partition %s is not rack-aware. Leader (%s) and follower brokers (%s).",
                                                             name(), leader.topicPartition(), leader.broker(), followerBrokers),
                                               recommendation);
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
  @Override
  protected SortedSet<Broker> rackAwareEligibleBrokers(Replica replica, ClusterModel clusterModel) {
    // Populate partition rack ids.
    List<String> partitionRackIds = clusterModel.partition(replica.topicPartition()).partitionBrokers()
        .stream().map(partitionBroker -> partitionBroker.rack().id()).collect(Collectors.toList());

    // Remove rack id of the given replica, but if there is any other replica from the partition residing in the
    // same cluster, keep its rack id in the list.
    partitionRackIds.remove(replica.broker().rack().id());

    SortedSet<Broker> rackAwareEligibleBrokers = new TreeSet<>(Comparator.comparingInt(Broker::id));
    for (Broker broker : clusterModel.aliveBrokers()) {
      if (!partitionRackIds.contains(broker.rack().id())) {
        rackAwareEligibleBrokers.add(broker);
      }
    }
    // Return eligible brokers.
    return rackAwareEligibleBrokers;
  }

  @Override
  protected boolean shouldKeepInTheCurrentBroker(Replica replica, ClusterModel clusterModel) {
    // Rack awareness requires no more than one replica from a given partition residing in any rack in the cluster
    String myRackId = replica.broker().rack().id();
    int myBrokerId = replica.broker().id();
    for (Broker partitionBroker : clusterModel.partition(replica.topicPartition()).partitionBrokers()) {
      if (myRackId.equals(partitionBroker.rack().id()) && myBrokerId != partitionBroker.id()) {
        return false;
      }
    }
    return true;
  }
}
