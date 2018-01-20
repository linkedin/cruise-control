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
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class for achieving the following hard goal:
 * HARD GOAL: Generate replica movement proposals to ensure that each broker has less than the given number of replicas.
 */
public class ReplicaCapacityGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaCapacityGoal.class);

  /**
   * Constructor for Replica Capacity Goal.
   */
  public ReplicaCapacityGoal() {

  }

  /**
   * Package private for unit test.
   */
  ReplicaCapacityGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
  }

  /**
   * Check whether given proposal is acceptable by this goal. A proposal is acceptable by a goal if it satisfies
   * requirements of the goal. Requirements(hard goal): replica capacity goal.
   *
   * @param proposal     Proposal to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return True if proposal is acceptable by this goal, false otherwise.
   */
  @Override
  public boolean isProposalAcceptable(BalancingProposal proposal, ClusterModel clusterModel) {
    if (proposal.balancingAction().equals(BalancingAction.REPLICA_MOVEMENT) ||
        proposal.balancingAction().equals(BalancingAction.REPLICA_ADDITION)) {
      Broker destinationBroker = clusterModel.broker(proposal.destinationBrokerId());
      if (destinationBroker.replicas().size() >= _balancingConstraint.maxReplicasPerBroker()) {
        return false;
      }
    }
    return true;
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
  protected void initGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws AnalysisInputException, ModelInputException {
    List<String> topicsToRebalance = new ArrayList<>(clusterModel.topics());
    topicsToRebalance.removeAll(excludedTopics);
    if (topicsToRebalance.isEmpty()) {
      LOG.warn("All topics are excluded from {}.", name());
    }

    // Sanity check: excluded topic replicas in a broker cannot exceed the max number of allowed replicas per broker.
    int totalReplicasInCluster = 0;
    for (Broker broker : brokersToBalance(clusterModel)) {
      if (!broker.isAlive()) {
        continue;
      }
      int excludedReplicasInBroker = 0;
      for (String topic : excludedTopics) {
        excludedReplicasInBroker += broker.replicasOfTopicInBroker(topic).size();
      }

      if (excludedReplicasInBroker > _balancingConstraint.maxReplicasPerBroker()) {
        throw new AnalysisInputException(String.format("Replicas of excluded topics in broker: %d exceeds the maximum "
            + "allowed number of replicas: %d.", excludedReplicasInBroker, _balancingConstraint.maxReplicasPerBroker()));
      }
      // Calculate total number of replicas for the next sanity check.
      totalReplicasInCluster += broker.replicas().size();
    }

    // Sanity check: total replicas in the cluster cannot be more than the allowed replicas in the cluster.
    long maxReplicasInCluster = _balancingConstraint.maxReplicasPerBroker() * clusterModel.healthyBrokers().size();
    if (totalReplicasInCluster > maxReplicasInCluster) {
      throw new AnalysisInputException(String.format("Total replicas in cluster: %d exceeds the maximum allowed "
          + "replicas in cluster: %d.", totalReplicasInCluster, maxReplicasInCluster));
    }
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
    Broker destinationBroker = clusterModel.broker(proposal.destinationBrokerId());
    if (destinationBroker.replicas().size() >= _balancingConstraint.maxReplicasPerBroker()) {
      return false;
    }
    return true;
  }

  /**
   * Update goal state after one round of self-healing / rebalance.
   *
   *  @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposal.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws AnalysisInputException, OptimizationFailureException {
    // One pass is sufficient to satisfy or alert impossibility of this goal.
    // Sanity check to confirm that the final distribution has less than the allowed number of replicas per broker.
    ensureReplicaCapacitySatisfied(clusterModel);
    // Sanity check: No self-healing eligible replica should remain at a decommissioned broker.
    AnalyzerUtils.ensureNoReplicaOnDeadBrokers(clusterModel);
    finish();
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
      throws AnalysisInputException, ModelInputException, OptimizationFailureException {
    LOG.debug("balancing broker {}, optimized goals = {}", broker, optimizedGoals);
    for (Replica replica : new ArrayList<>(broker.replicas())) {
      if (broker.isAlive() && broker.replicas().size() <= _balancingConstraint.maxReplicasPerBroker()) {
        break;
      }
      if (shouldExclude(replica, excludedTopics)) {
        continue;
      }

      // The goal requirements are violated. Move replica to an available broker.
      SortedSet<Broker> replicaCapacityEligibleBrokers = replicaCapacityEligibleBrokers(replica, clusterModel);
      Broker b = maybeApplyBalancingAction(clusterModel, replica, replicaCapacityEligibleBrokers,
          BalancingAction.REPLICA_MOVEMENT, optimizedGoals);
      if (b == null) {
        LOG.debug("Failed to move replica {} to any broker in {}", replica, replicaCapacityEligibleBrokers);
      }
    }
  }

  /**
   * Get a list of replica capacity eligible brokers for the given replica in the given cluster. A broker is eligible
   * for a given replica if the broker contains less than allowed maximum number of replicas.
   *
   * @param replica      Replica for which a set of replica capacity eligible brokers are requested.
   * @param clusterModel The state of the cluster.
   * @return A list of replica capacity eligible brokers for the given replica in the given cluster.
   */
  private SortedSet<Broker> replicaCapacityEligibleBrokers(Replica replica, ClusterModel clusterModel) {
    // Populate partition rack ids.
    SortedSet<Broker> eligibleBrokers = new TreeSet<>((o1, o2) -> {
      return Integer.compare(o1.replicas().size(), o2.replicas().size());
    });

    int sourceBrokerId = replica.broker().id();

    for (Broker broker : clusterModel.healthyBrokers()) {
      if (broker.replicas().size() < _balancingConstraint.maxReplicasPerBroker() && broker.id() != sourceBrokerId) {
        eligibleBrokers.add(broker);
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
}
