/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.BROKER_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;


public class KafkaAssignerEvenRackAwareGoal implements Goal {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaAssignerEvenRackAwareGoal.class);
  private final ProvisionResponse _provisionResponse;
  private final Map<Integer, SortedSet<BrokerReplicaCount>> _aliveBrokerReplicaCountByPosition;
  private Map<String, List<Partition>> _partitionsByTopic;

  public KafkaAssignerEvenRackAwareGoal() {
    _partitionsByTopic = null;
    _aliveBrokerReplicaCountByPosition = new HashMap<>();
    _provisionResponse = new ProvisionResponse(ProvisionStatus.UNDECIDED);
  }

  /**
   * Sanity Check: There exists sufficient number of racks for achieving rack-awareness.
   * 1. Initialize partitions by topic.
   * 2. Initialize the number of excluded replicas by position for each broker.
   * 3. Initialize the alive broker replica count by position.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization action.
   */
  private void initGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws OptimizationFailureException {
    // Sanity check: Ensure that rack awareness is satisfiable.
    ensureRackAwareSatisfiable(clusterModel, excludedTopics);

    // 1. Initialize partitions by topic.
    _partitionsByTopic = clusterModel.getPartitionsByTopic();

    // 2. Initialize the number of excluded replicas by position for each broker.
    Map<Integer, Map<Integer, Integer>> numExcludedReplicasByPositionInBroker = new HashMap<>();
    clusterModel.brokers().forEach(broker -> numExcludedReplicasByPositionInBroker.put(broker.id(), new HashMap<>()));
    for (String excludedTopic : excludedTopics) {
      for (Partition partition : _partitionsByTopic.get(excludedTopic)) {
        // Add 1 to the number of excluded replicas in relevant position for the broker that the replica resides in.
        // Leader is at position 0.
        int position = 0;
        numExcludedReplicasByPositionInBroker.get(partition.leader().broker().id()).merge(position, 1, Integer::sum);

        // Followers are ordered in positions [1, numFollowers].
        for (Broker followerBroker : partition.followerBrokers()) {
          position++;
          numExcludedReplicasByPositionInBroker.get(followerBroker.id()).merge(position, 1, Integer::sum);
        }
      }
    }

    // 3. Initialize the alive broker replica count by position.
    int maxReplicationFactor = clusterModel.maxReplicationFactor();
    for (int i = 0; i < maxReplicationFactor; i++) {
      SortedSet<BrokerReplicaCount> aliveBrokersByReplicaCount = new TreeSet<>();
      for (Broker broker : clusterModel.aliveBrokers()) {
        int numExcludedReplicasInPosition = numExcludedReplicasByPositionInBroker.get(broker.id()).getOrDefault(i, 0);
        BrokerReplicaCount brokerReplicaCount = new BrokerReplicaCount(broker, numExcludedReplicasInPosition);
        aliveBrokersByReplicaCount.add(brokerReplicaCount);
      }
      _aliveBrokerReplicaCountByPosition.put(i, aliveBrokersByReplicaCount);
    }
  }

  @Override
  public boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, OptimizationOptions optimizationOptions)
      throws KafkaCruiseControlException {
    KafkaAssignerUtils.sanityCheckOptimizationOptions(optimizationOptions);
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    LOG.debug("Starting {} with excluded topics = {}", name(), excludedTopics);

    if (!optimizedGoals.isEmpty()) {
      throw new IllegalArgumentException(String.format("Goals %s cannot be optimized before %s.", optimizedGoals, name()));
    }

    initGoalState(clusterModel, excludedTopics);

    // STEP1: Move leader to the first position in partition replica list.
    for (Map.Entry<String, List<Partition>> entry : _partitionsByTopic.entrySet()) {
      for (Partition partition : entry.getValue()) {
        // Ensure the first replica is the leader.
        if (partition.replicas().get(0) != partition.leader()) {
          partition.swapReplicaPositions(0, partition.replicas().indexOf(partition.leader()));
        }
      }
    }

    // STEP2: Perform optimization.
    int maxReplicationFactor = clusterModel.maxReplicationFactor();
    for (int position = 0; position < maxReplicationFactor; position++) {
      for (Map.Entry<String, List<Partition>> entry : _partitionsByTopic.entrySet()) {
        for (Partition partition : entry.getValue()) {
          if (partition.replicas().size() <= position) {
            continue;
          }
          if (shouldExclude(partition, position, excludedTopics)) {
            continue;
          }
          // Apply the necessary move (if needed).
          if (!maybeApplyMove(clusterModel, partition, position)) {
            throw new OptimizationFailureException(
                String.format("[%s] Unable to apply move for replica %s.", name(), replicaAtPosition(partition, position)));
          }
        }
      }
    }
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    ensureRackAware(clusterModel, excludedTopics);
    return true;
  }

  /**
   * Apply the move to the first eligible destination broker selected from _aliveBrokerReplicaCountByPosition for the
   * relevant replica position.
   *
   * An eligible destination broker must reside in a rack that has no other replicas from the same partition, which has
   * a position smaller than the given replicaPosition.
   *
   * If the destination broker has:
   * (1) no other replica from the same partition, move the replica to there.
   * (2) a replica with a larger position AND the source broker is alive, swap positions.
   * (3) the conditions (1-2) are {@code false} AND the source broker is dead.
   * (4) the current replica under consideration, do nothing -- i.e. do not move replica or swap positions.
   *
   * @param clusterModel The state of the cluster.
   * @param partition The partition whose replica might be moved.
   * @param replicaPosition The position of the replica in the given partition.
   * @return {@code true} if a move is applied, {@code false} otherwise.
   */
  private boolean maybeApplyMove(ClusterModel clusterModel, Partition partition, int replicaPosition) {
    // Racks with replica whose position is in [0, replicaPosition - 1] are ineligible for assignment.
    Set<String> ineligibleRackIds = new HashSet<>();
    for (int pos = 0; pos < replicaPosition; pos++) {
      Replica replica = replicaAtPosition(partition, pos);
      ineligibleRackIds.add(replica.broker().rack().id());
    }
    // Find an eligible destination and apply the relevant move.
    BrokerReplicaCount eligibleBrokerReplicaCount = null;
    for (final Iterator<BrokerReplicaCount> it = _aliveBrokerReplicaCountByPosition.get(replicaPosition).iterator();
         it.hasNext(); ) {
      BrokerReplicaCount destinationBrokerReplicaCount = it.next();
      if (ineligibleRackIds.contains(destinationBrokerReplicaCount.broker().rack().id())) {
        continue;
      }

      // Get the replica in the destination broker (if any)
      Broker destinationBroker = destinationBrokerReplicaCount.broker();
      Replica destinationReplica = destinationBroker.replica(partition.topicPartition());
      Replica replicaAtPosition = partition.replicas().get(replicaPosition);

      if (destinationReplica == null) {
        // The destination broker has no replica from the source partition: move the source replica to the destination broker.
        LOG.trace("Destination broker {} has no other replica from the same partition, move the replica {} to there.",
                  destinationBroker, replicaAtPosition);
        applyBalancingAction(clusterModel, replicaAtPosition, destinationBroker, ActionType.INTER_BROKER_REPLICA_MOVEMENT);
      } else if (destinationBroker.id() != replicaAtPosition.broker().id() && replicaAtPosition.broker().isAlive()) {
        // The destination broker contains a replica from the source partition AND the destination broker is different
        // from the source replica broker AND the source broker is alive. Hence, we can safely swap replica positions.
        LOG.trace("Destination broker has a replica {} with a larger position than source replica {}, swap positions.",
                  destinationReplica, replicaAtPosition);
        if (replicaPosition == 0) {
          // Transfer leadership -- i.e swap the position of leader with its follower in destination broker.
          applyBalancingAction(clusterModel, replicaAtPosition, destinationBroker, ActionType.LEADERSHIP_MOVEMENT);
        } else {
          // Swap the follower position of this replica with the follower position of destination replica.
          int destinationPos = followerPosition(partition, destinationBroker.id());
          partition.swapFollowerPositions(replicaPosition, destinationPos);
        }
      } else if (!replicaAtPosition.broker().isAlive()) {
        // The broker of source replica is dead. Hence, we have to move the source replica away from it. But, destination
        // broker contains a replica from the same source partition. This prevents moving the source replica to it.
        LOG.trace("Source broker {} is dead and either the destination broker {} is the same as the source, or has a "
                  + "replica from the same partition.", replicaAtPosition.broker(), destinationBroker);
        // Unable apply any valid move.
        continue;
      }
      // Increment the replica count on the destination. Note that if the source and the destination brokers are the
      // same, then the source replica will simply stay in the same broker.
      eligibleBrokerReplicaCount = destinationBrokerReplicaCount;
      it.remove();
      break;
    }

    if (eligibleBrokerReplicaCount != null) {
      // Success: Increment the replica count on the destination.
      eligibleBrokerReplicaCount.incReplicaCount();
      _aliveBrokerReplicaCountByPosition.get(replicaPosition).add(eligibleBrokerReplicaCount);
      return true;
    }
    // Failure: Unable to apply any valid move -- i.e. optimization failed to place the source replica to a valid broker.
    return false;
  }

  /**
   * Get the position of follower that is part of the given partition and reside in the broker with the given id.
   *
   * @param partition The partition that the follower is a part of.
   * @param brokerId Id of the broker that the follower resides.
   * @return The position of follower that is part of the given partition and reside in the broker with the given id.
   */
  private int followerPosition(Partition partition, int brokerId) {
    int followerPos = 0;
    for (Replica replica: partition.replicas()) {
      if (replica.broker().id() == brokerId) {
        return followerPos;
      }
      followerPos++;
    }
    throw new IllegalArgumentException(String.format("Partition %s has no follower on %d.", partition, brokerId));
  }

  /**
   * Apply the given balancing action.
   *
   * @param clusterModel The state of the cluster
   * @param sourceReplica The source replica to be moved or giving its leadership.
   * @param destinationBroker The broker that will receive the source broker or its leadership.
   * @param action The balancing action is either replica or leadership move.
   */
  private void applyBalancingAction(ClusterModel clusterModel,
                                    Replica sourceReplica,
                                    Broker destinationBroker,
                                    ActionType action) {

    if (action == ActionType.LEADERSHIP_MOVEMENT) {
      clusterModel.relocateLeadership(sourceReplica.topicPartition(), sourceReplica.broker().id(), destinationBroker.id());
    } else if (action == ActionType.INTER_BROKER_REPLICA_MOVEMENT) {
      clusterModel.relocateReplica(sourceReplica.topicPartition(), sourceReplica.broker().id(), destinationBroker.id());
    }
  }

  /**
   * Get replica of the given partition residing in the given position.
   *
   * @param sourcePartition Partition whose replica will be returned.
   * @param position The position of replica in the partition, i.e. 0 means leader, 1 is the first follower, and so on.
   * @return The replica of the given partition residing in the given position.
   */
  private Replica replicaAtPosition(Partition sourcePartition, int position) {
    return sourcePartition.replicas().get(position);
  }

  /**
   * Check whether the replica should be excluded from the rebalance. A replica should be excluded if (1) its topic
   * is in the excluded topics set, (2) its original broker is still alive, and (3) its original disk is alive.
   *
   * @param partition The partition of replica to be checked.
   * @param position The position of replica in the given partition.
   * @param excludedTopics The excluded topics set.
   * @return {@code true} if the replica should be excluded, {@code false} otherwise.
   */
  private boolean shouldExclude(Partition partition, int position, Set<String> excludedTopics) {
    Replica replica = replicaAtPosition(partition, position);
    return excludedTopics.contains(replica.topicPartition().topic()) && !replica.isOriginalOffline();
  }

  /**
   * Sanity Check: There exists sufficient number of racks for achieving rack-awareness.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposals.
   */
  private void ensureRackAwareSatisfiable(ClusterModel clusterModel, Set<String> excludedTopics)
      throws OptimizationFailureException {
    // Sanity Check: not enough racks to satisfy rack awareness.
    int numAliveRacks = clusterModel.numAliveRacks();
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
  }

  /**
   * Sanity Check: Replicas are distributed in a rack-aware way.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposals.
   */
  private void ensureRackAware(ClusterModel clusterModel, Set<String> excludedTopics)
      throws OptimizationFailureException {
    // Sanity check to confirm that the final distribution is rack aware.
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
        throw new OptimizationFailureException("Optimization for goal " + name() + " failed for rack-awareness of "
                                               + "partition " + leader.topicPartition());
      }
    }
  }

  /**
   * Check whether the given action is acceptable by this goal. This goal is used to generate an initially balanced,
   * rack-aware distribution. Hence, as long as the rack awareness is preserved, it is acceptable to break the
   * distribution generated by this goal for the sake of satisfying the requirements of the other goals.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
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
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    // We only need the latest snapshot and include all the topics.
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
  }

  /**
   * Get the name of this goal. Name of a goal provides an identification for the goal in human readable format.
   */
  @Override
  public String name() {
    return KafkaAssignerEvenRackAwareGoal.class.getSimpleName();
  }

  @Override
  public void finish() {
    // Done.
  }

  @Override
  public boolean isHardGoal() {
    return true;
  }

  @Override
  public ProvisionStatus provisionStatus() {
    // Provision status computation is not supported for kafka_assigner goals.
    return provisionResponse().status();
  }

  @Override
  public ProvisionResponse provisionResponse() {
    return _provisionResponse;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new GoalUtils.HardGoalStatsComparator();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // No external config is used.
  }

  /**
   * A helper class for this goal to keep track of the number of replicas assigned to brokers at each position during
   * the optimization process.
   */
  private static class BrokerReplicaCount implements Comparable<BrokerReplicaCount> {
    private final Broker _broker;
    private int _replicaCount;

    BrokerReplicaCount(Broker broker, int replicaCount) {
      _broker = broker;
      _replicaCount = replicaCount;
    }

    public Broker broker() {
      return _broker;
    }

    int replicaCount() {
      return _replicaCount;
    }

    void incReplicaCount() {
      _replicaCount++;
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
