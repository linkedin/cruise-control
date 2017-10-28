/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;

import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class for achieving the following hard goal:
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
   * Check whether given proposal is acceptable by this goal. A proposal is acceptable by a goal if it satisfies
   * requirements of the goal. Requirements(hard goal): rack awareness.
   *
   * @param proposal     Proposal to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return True if proposal is acceptable by this goal, false otherwise.
   */
  @Override
  public boolean isProposalAcceptable(BalancingProposal proposal, ClusterModel clusterModel) {
    if (proposal.balancingAction().equals(BalancingAction.REPLICA_MOVEMENT)) {
      Replica sourceReplica = clusterModel.broker(proposal.sourceBrokerId()).replica(proposal.topicPartition());
      Broker destinationBroker = clusterModel.broker(proposal.destinationBrokerId());

      // Destination broker cannot be in a rack that violates rack awareness.
      Set<Broker> partitionBrokers = clusterModel.partition(sourceReplica.topicPartition()).partitionBrokers();
      partitionBrokers.remove(sourceReplica.broker());

      // Remove brokers in partition broker racks except the brokers in replica broker rack.
      for (Broker broker : partitionBrokers) {
        if (broker.rack().brokers().contains(destinationBroker)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new RackAwareGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    // We only need the latest snapshot and include all the topics.
    return new ModelCompletenessRequirements(1, 0.0, true);
  }

  /**
   * Get the name of this goal. Name of a goal provides an identification for the goal in human readable format.
   */
  @Override
  public String name() {
    return RackAwareGoal.class.getSimpleName();
  }

  /**
   * Check if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param proposal     Proposal containing information about
   * @return True if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingProposal proposal) {
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
  protected Collection<Broker> brokersToBalance(ClusterModel clusterModel) {
    return clusterModel.brokers();
  }

  /**
   * This is a hard goal; hence, the proposals are not limited to dead broker replicas in case of self-healing.
   * Sanity Check: There exists sufficient number of racks for achieving rack-awareness.
   *
   * @param clusterModel The state of the cluster.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel)
      throws AnalysisInputException, ModelInputException {
    // Sanity Check: not enough racks to satisfy rack awareness.
    if (clusterModel.maxReplicationFactor() > clusterModel.numHealthyRacks()) {
      throw new AnalysisInputException("Insufficient number of racks to distribute each replica over a rack.");
    }
  }

  /**
   * Update goal state.
   * (1) Sanity check: After completion of balancing / self-healing all resources, confirm that replicas of each
   * partition reside at a separate rack and finish.
   * (2) Update the current resource that is being balanced if there are still resources to be balanced.
   *
   * @param clusterModel The state of the cluster.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel)
      throws AnalysisInputException, OptimizationFailureException {
    // One pass is sufficient to satisfy or alert impossibility of this goal.
    // Sanity check to confirm that the final distribution is rack aware.
    ensureRackAware(clusterModel);
    // Sanity check: No self-healing eligible replica should remain at a decommissioned broker.
    AnalyzerUtils.ensureNoReplicaOnDeadBrokers(clusterModel);
    finish();
  }

  /**
   * Rack-awareness violations can be resolved with replica movements.
   *
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param excludedTopics The topics that should be excluded from the optimization proposal.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
      ClusterModel clusterModel,
      Set<Goal> optimizedGoals,
      Set<String> excludedTopics)
      throws AnalysisInputException, ModelInputException {
    LOG.debug("balancing broker {}, optimized goals = {}", broker, optimizedGoals);
    // Satisfy rack awareness requirement.
    for (Replica replica : new ArrayList<>(broker.replicas())) {
      if ((broker.isAlive() && satisfiedRackAwareness(replica, clusterModel))
          || excludedTopics.contains(replica.topicPartition().topic())) {
        continue;
      }
      // Rack awareness is violated. Move replica to a broker in another rack.
      if (maybeApplyBalancingAction(clusterModel, replica, rackAwareEligibleBrokers(replica, clusterModel),
          BalancingAction.REPLICA_MOVEMENT, optimizedGoals) == null) {
        throw new AnalysisInputException(
            "Violated rack-awareness requirement for broker with id " + broker.id() + ".");
      }
    }
  }

  private void ensureRackAware(ClusterModel clusterModel) throws OptimizationFailureException {
    // Sanity check to confirm that the final distribution is rack-aware.
    for (Replica leader : clusterModel.leaderReplicas()) {
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
   * Get a list of rack aware eligible brokers for the given replica in the given cluster. A broker is rack aware
   * eligible for a given replica if the broker resides in a rack where no other broker in the same rack contains a
   * replica from the same partition of the given replica.
   *
   * @param replica      Replica for which a set of rack aware eligible brokers are requested.
   * @param clusterModel The state of the cluster.
   * @return A list of rack aware eligible brokers for the given replica in the given cluster.
   */
  private List<Broker> rackAwareEligibleBrokers(Replica replica, ClusterModel clusterModel) {
    // Populate partition rack ids.
    List<String> partitionRackIds = clusterModel.partition(replica.topicPartition()).partitionBrokers()
        .stream().map(partitionBroker -> partitionBroker.rack().id()).collect(Collectors.toList());

    // Remove rack id of the given replica, but if there is any other replica from the partition residing in the
    // same cluster, keep its rack id in the list.
    partitionRackIds.remove(replica.broker().rack().id());

    // Return eligible brokers.
    return clusterModel.healthyBrokers().stream().filter(broker -> !partitionRackIds.contains(broker.rack().id()))
        .collect(Collectors.toList());
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
