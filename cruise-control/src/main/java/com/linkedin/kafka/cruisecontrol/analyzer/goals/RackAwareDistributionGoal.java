/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;
import static java.util.Collections.max;
import static java.util.Collections.min;


/**
 * HARD GOAL: Generate replica movement proposals to evenly distribute replicas over alive racks not excluded for replica moves.
 *
 * This is a relaxed version of {@link RackAwareGoal}. Contrary to {@link RackAwareGoal}, as long as replicas of each
 * partition can achieve a perfectly even distribution across the racks, this goal lets placement of multiple replicas
 * of a partition into a single rack.
 *
 * <p>
 *   For example, suppose a topic with 1 partition has 4 replicas in a cluster with 2 racks. Then the following
 *   distribution will be acceptable by this goal (but would be unacceptable by {@link RackAwareGoal}):
 * </p>
 *
 * <pre>
 * Rack A                    | Rack B
 * -----------------------------------------------------
 * Broker1-rack A  replica-0 | Broker2-rack B
 * Broker3-rack A            | Broker4-rack B replica-1
 * Broker5-rack A  replica-2 | Broker6-rack B
 * Broker7-rack A            | Broker8-rack B replica-3
 * </pre>
 *
 * <p>
 *   However, this goal will yield an {@link OptimizationFailureException} for the same partition in the following
 *   cluster due to the lack of a second broker to place a replica of this partition in {@code Rack B}:
 * </p>
 *
 * <pre>
 * Rack A                    | Rack B
 * -----------------------------------------------------
 * Broker1-rack A  replica-0 | Broker2-rack B replica-1
 * Broker3-rack A  replica-3 |
 * Broker5-rack A  replica-2 |
 * </pre>
 */
public class RackAwareDistributionGoal extends AbstractRackAwareGoal {
  private BalanceLimit _balanceLimit;
  // This is used to identify brokers not excluded for replica moves.
  private Set<Integer> _brokersAllowedReplicaMove;

  /**
   * Constructor for Rack Aware Distribution Goal.
   */
  public RackAwareDistributionGoal() {
  }

  /**
   * Package private for unit test.
   */
  RackAwareDistributionGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
  }

  @Override
  protected boolean doesReplicaMoveViolateActionAcceptance(ClusterModel clusterModel, Replica sourceReplica, Broker destinationBroker) {
    String destinationRackId = destinationBroker.rack().id();
    String sourceRackId = sourceReplica.broker().rack().id();

    if (sourceRackId.equals(destinationRackId)) {
      // A replica move within the same rack cannot violate rack aware distribution.
      return false;
    }

    // The replica move shall not increase the replica distribution imbalance of the partition across racks.
    Set<Broker> partitionBrokers = clusterModel.partition(sourceReplica.topicPartition()).partitionBrokers();
    Map<String, Integer> numReplicasByRack = numPartitionReplicasByRackId(partitionBrokers);

    // Once this goal is optimized, it is guaranteed to have 0 replicas on brokers excluded for replica moves.
    // Hence, no explicit check is necessary for verifying the replica source.
    return numReplicasByRack.getOrDefault(destinationRackId, 0) >= numReplicasByRack.getOrDefault(sourceRackId, 0);
  }

  /**
   * Given the brokers that host replicas of a partition, retrieves a map containing number of replicas by the id of the
   * rack they reside in.
   *
   * @param partitionBrokers Brokers that host replicas of some partition
   * @return A map containing the number of replicas by rack id that these replicas reside in.
   */
  private static Map<String, Integer> numPartitionReplicasByRackId(Set<Broker> partitionBrokers) {
    Map<String, Integer> numPartitionReplicasByRackId = new HashMap<>();
    for (Broker broker : partitionBrokers) {
      numPartitionReplicasByRackId.merge(broker.rack().id(), 1, Integer::sum);
    }
    return numPartitionReplicasByRackId;
  }

  @Override
  public String name() {
    return RackAwareDistributionGoal.class.getSimpleName();
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
   * This is a hard goal; hence, the proposals are not limited to dead broker replicas in case of self-healing.
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
    _balanceLimit = new BalanceLimit(clusterModel, optimizationOptions);
    int numExtraRacks = _balanceLimit.numAliveRacksAllowedReplicaMoves() - clusterModel.maxReplicationFactor();
    if (numExtraRacks >= _balancingConstraint.overprovisionedMinExtraRacks()) {
      int numRacksToDrop = numExtraRacks - _balancingConstraint.overprovisionedMinExtraRacks() + 1;
      ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.OVER_PROVISIONED)
          .numRacks(numRacksToDrop).build();
      _provisionResponse = new ProvisionResponse(ProvisionStatus.OVER_PROVISIONED, recommendation, name());
    }
    Set<String> excludedTopics = optimizationOptions.excludedTopics();

    // Filter out some replicas based on optimization options.
    new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(),
                                                     optimizationOptions.onlyMoveImmigrantReplicas())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .trackSortedReplicasFor(replicaSortName(this, false, false), clusterModel);
  }

  /**
   * Update goal state.
   * Sanity check: After completion of balancing / self-healing, confirm that replicas of each partition are evenly
   * distributed across the racks.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    // One pass is sufficient to satisfy or alert impossibility of this goal.
    // Sanity check to confirm that replicas of each partition are evenly distributed across the racks
    ensureRackAwareDistribution(clusterModel, optimizationOptions);
    // Sanity check: No replica should be moved to a broker, which used to host any replica of the same partition on its broken disk.
    GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
    if (_provisionResponse.status() != ProvisionStatus.OVER_PROVISIONED) {
      _provisionResponse = new ProvisionResponse(ProvisionStatus.RIGHT_SIZED);
    }
    finish();
  }

  @Override
  public void finish() {
    super.finish();
    // Clean up the memory
    _balanceLimit.clear();
  }

  /**
   * Violations of rack-aware distribution can be resolved with replica movements.
   *
   * @param broker Broker to be balanced.
   * @param clusterModel The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    rebalanceForBroker(broker, clusterModel, optimizedGoals, optimizationOptions, false);
  }

  /**
   * Get a list of eligible brokers for moving the given replica in the given cluster to satisfy the specific
   * requirements of the custom rack aware goal.
   * A broker is rack aware distribution eligible for a given replica if it is in a rack
   * <ul>
   *   <li>that needs more replicas -- i.e. has fewer than base limit replicas</li>
   *   <li>with at most as many as base limit replicas, and the number of racks with at least one more replica over the
   *   base limit is fewer than the allowed number of such racks</li>
   * </ul>
   *
   * @param replica Replica for which a set of rack aware eligible brokers are requested.
   * @param clusterModel The state of the cluster.
   * @return A list of rack aware eligible brokers for the given replica in the given cluster.
   */
  @Override
  protected SortedSet<Broker> rackAwareEligibleBrokers(Replica replica, ClusterModel clusterModel) {
    Set<Broker> partitionBrokers = clusterModel.partition(replica.topicPartition()).partitionBrokers();
    Map<String, Integer> numReplicasByRack = numPartitionReplicasByRackId(partitionBrokers);

    // Decrement the number of replicas in this rack.
    numReplicasByRack.merge(replica.broker().rack().id(), -1, Integer::sum);

    int baseLimit = _balanceLimit.baseLimitByRF(partitionBrokers.size());

    // A replica is allowed to be moved to a rack at the base limit only if the number of racks with at least one more
    // replica over the base limit is fewer than the allowed number of such racks.
    // For example, suppose a partition has 5 replicas in a cluster with 3 racks. In the ideal distribution, 2 racks has
    // 2 replicas and the other rack has 1 replica from the partition. Suppose that in the current distribution, Rack-1
    // has 1 offline and 1 online replica, Rack-2 has 2 online replicas, and Rack-3 has 1 online replica. In this scenario,
    // we can place the offline replica to an alive broker in either Rack-1 or Rack-3, because the cluster has only one
    // rack with at least one more replica over the base limit and we know that the ideal distribution allows 2 such racks.
    boolean canMoveToRacksAtBaseLimit = false;
    int numRacksWithOneMoreReplicaLimit = _balanceLimit.numRacksWithOneMoreReplicaByRF(partitionBrokers.size());
    int numRacksWithAtLeastOneMoreReplica = (int) numReplicasByRack.values().stream().filter(r -> r > baseLimit).count();
    if (numRacksWithAtLeastOneMoreReplica < numRacksWithOneMoreReplicaLimit) {
      canMoveToRacksAtBaseLimit = true;
    }

    // Prefer brokers whose rack has fewer replicas from the partition
    SortedSet<Broker> rackAwareDistributionEligibleBrokers = new TreeSet<>(
        Comparator.comparingInt((Broker b) -> numReplicasByRack.getOrDefault(b.rack().id(), 0))
                  .thenComparingInt(Broker::id));

    for (Broker destinationBroker : clusterModel.aliveBrokers()) {
      int numReplicasInThisRack = numReplicasByRack.getOrDefault(destinationBroker.rack().id(), 0);
      if (numReplicasInThisRack < baseLimit || (canMoveToRacksAtBaseLimit && numReplicasInThisRack == baseLimit)) {
        // Either the (1) destination rack needs more replicas or (2) the replica is allowed to be moved to a rack at
        // the base limit and the destination broker is in a rack at the base limit
        if (!partitionBrokers.contains(destinationBroker)) {
          rackAwareDistributionEligibleBrokers.add(destinationBroker);
        }
      }
    }

    // Return eligible brokers
    return rackAwareDistributionEligibleBrokers;
  }

  @Override
  protected boolean shouldKeepInTheCurrentBroker(Replica replica, ClusterModel clusterModel) {
    if (isExcludedForReplicaMove(replica.broker())) {
      // A replica in a broker excluded for the replica moves must be relocated to another broker.
      return false;
    }
    // Rack aware distribution requires perfectly even distribution for replicas of each partition across the racks.
    // This permits placement of multiple replicas of a partition into a single rack.
    Set<Broker> partitionBrokers = clusterModel.partition(replica.topicPartition()).partitionBrokers();
    int replicationFactor = partitionBrokers.size();
    Map<String, Integer> numReplicasByRack = numPartitionReplicasByRackId(partitionBrokers);

    int baseLimit = _balanceLimit.baseLimitByRF(replicationFactor);
    int numRacksWithOneMoreReplicaLimit = _balanceLimit.numRacksWithOneMoreReplicaByRF(replicationFactor);
    int upperLimit = baseLimit + (numRacksWithOneMoreReplicaLimit == 0 ? 0 : 1);

    int numReplicasInThisRack = numReplicasByRack.get(replica.broker().rack().id());
    if (numReplicasInThisRack <= baseLimit) {
      // Rack does not have extra replicas to give away
      return true;
    } else if (numReplicasInThisRack > upperLimit) {
      // The rack has extra replica(s) to give away
      return false;
    } else {
      // This is a rack either with an extra replica to give away or keep.
      int numRacksWithOneMoreReplica = (int) numReplicasByRack.values().stream().filter(r -> r > baseLimit).count();
      // If the current number of racks with one more replica over the base limit are more than the allowed number of
      // such racks, then this rack has an extra replica to give away, otherwise it keeps the replica.
      return numRacksWithOneMoreReplica <= numRacksWithOneMoreReplicaLimit;
    }
  }

  /**
   * Ensures that replicas of all partitions in the cluster satisfy rack-aware distribution.
   *
   * @param clusterModel A cluster model with no offline replicas.
   * @param optimizationOptions Options to take into account during optimization.
   */
  private void ensureRackAwareDistribution(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    // Sanity check to confirm that replicas of each partition are evenly distributed across the racks.
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    for (Replica leader : clusterModel.leaderReplicas()) {
      if (excludedTopics.contains(leader.topicPartition().topic())) {
        continue;
      }

      Set<Broker> partitionBrokers = clusterModel.partition(leader.topicPartition()).partitionBrokers();
      Map<String, Integer> numReplicasByRack = numPartitionReplicasByRackId(partitionBrokers);

      int maxNumReplicasInARack = max(numReplicasByRack.values());

      // Check if rack(s) have multiple replicas from the same partition (i.e. otherwise the distribution is rack-aware).
      if (maxNumReplicasInARack > 1) {
        // Check whether there are alive racks allowed replica moves with (1) no replicas despite having RF > number of
        // alive racks allowed replica moves or (2) more replicas that they could have been placed into other racks.
        boolean someAliveRacksHaveNoReplicas = numReplicasByRack.size() < _balanceLimit.numAliveRacksAllowedReplicaMoves();
        if (someAliveRacksHaveNoReplicas || maxNumReplicasInARack - min(numReplicasByRack.values()) > 1) {
          Set<String> excludedRackIds = new HashSet<>();
          if (someAliveRacksHaveNoReplicas) {
            // Exclude all racks containing replicas of this partition.
            excludedRackIds.addAll(numReplicasByRack.keySet());
          } else {
            for (Map.Entry<String, Integer> entry : numReplicasByRack.entrySet()) {
              if (entry.getValue() == maxNumReplicasInARack) {
                // Racks except for the one that hosts the maximum number of replicas are available for hosting more replicas.
                excludedRackIds.add(entry.getKey());
              }
            }
          }

          ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
              .numBrokers(1).excludedRackIds(excludedRackIds).build();
          throw new OptimizationFailureException(String.format("[%s] Partition %s is not rack-aware. Brokers (%s) and replicas per rack (%s).",
                                                               name(), leader.topicPartition(), partitionBrokers, numReplicasByRack),
                                                 recommendation);
        }
      }
    }
  }

  /**
   * A wrapper to facilitate describing per-rack replica count limits for a partition with the given replication factor.
   *
   * These limits are expressed in terms of the following:
   * <ul>
   *   <li>{@link #_baseLimitByRF}: The minimum number of replicas from the partition with the given replication factor
   *   that each alive rack that is allowed replica moves must contain</li>
   *   <li>{@link #_numRacksWithOneMoreReplicaByRF}: The exact number of racks that are allowed replica moves must contain
   *   an additional replica (i.e. the base limit + 1) from the partition with the given replication factor</li>
   * </ul>
   *
   * <p>
   *   For example, for a given replication factor (RF), if the base limit is 1 and the number of racks (that are allowed
   *   replica moves) with one more replica is 3, then 3 racks should have 2 replicas and each remaining rack should have
   *   1 replica from the partition with the given RF.
   * </p>
   */
  private static class BalanceLimit {
    private final int _numAliveRacksAllowedReplicaMoves;
    private final Map<Integer, Integer> _baseLimitByRF;
    private final Map<Integer, Integer> _numRacksWithOneMoreReplicaByRF;

    BalanceLimit(ClusterModel clusterModel, OptimizationOptions optimizationOptions) throws OptimizationFailureException {
      _numAliveRacksAllowedReplicaMoves = clusterModel.numAliveRacksAllowedReplicaMoves(optimizationOptions);
      if (_numAliveRacksAllowedReplicaMoves == 0) {
        // Handle the case when all alive racks are excluded from replica moves.
        ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
            .numRacks(clusterModel.maxReplicationFactor()).build();
        throw new OptimizationFailureException("All alive racks are excluded from replica moves.", recommendation);
      }
      int maxReplicationFactor = clusterModel.maxReplicationFactor();
      _baseLimitByRF = new HashMap<>();
      _numRacksWithOneMoreReplicaByRF = new HashMap<>();

      // Precompute the limits for each possible replication factor up to maximum replication factor.
      for (int replicationFactor = 1; replicationFactor <= maxReplicationFactor; replicationFactor++) {
        int baseLimit = replicationFactor / _numAliveRacksAllowedReplicaMoves;
        _baseLimitByRF.put(replicationFactor, baseLimit);
        _numRacksWithOneMoreReplicaByRF.put(replicationFactor, replicationFactor % _numAliveRacksAllowedReplicaMoves);
      }
    }

    public int numAliveRacksAllowedReplicaMoves() {
      return _numAliveRacksAllowedReplicaMoves;
    }

    public Integer baseLimitByRF(int replicationFactor) {
      return _baseLimitByRF.get(replicationFactor);
    }

    public Integer numRacksWithOneMoreReplicaByRF(int replicationFactor) {
      return _numRacksWithOneMoreReplicaByRF.get(replicationFactor);
    }

    /**
     * Clear balance limits.
     */
    public void clear() {
      _baseLimitByRF.clear();
      _numRacksWithOneMoreReplicaByRF.clear();
    }
  }
}
