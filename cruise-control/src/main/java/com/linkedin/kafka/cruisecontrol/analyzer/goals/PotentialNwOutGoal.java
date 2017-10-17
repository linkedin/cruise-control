/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class for achieving the following soft goal:
 * <p>
 * SOFT GOAL#1: Generate proposals to keep the outbound network utilization on a broker such that even if all partitions
 * within the broker become the leader, the outbound network utilization would not exceed the corresponding broker
 * capacity threshold. This goal can be interpreted as keeping the potential outbound network utilization under a threshold.
 */
public class PotentialNwOutGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(PotentialNwOutGoal.class);

  // Flag to indicate whether the self healing failed to relocate all replicas away from dead brokers in its initial
  // attempt and currently omitting the potential outbound network limit to relocate remaining replicas.
  private boolean _selfHealingDeadBrokersOnly;

  /**
   * Empty constructor for Potential Network Outbound Goal.
   */
  public PotentialNwOutGoal() {
  }

  /**
   * Package private for unit test.
   */
  PotentialNwOutGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
  }

  /**
   * Check whether given proposal is acceptable by this goal. Proposal is acceptable by this goal if it satisfies
   * either of the following:
   * (1) it is a leadership movement,
   * (2) it satisfies {@link #selfSatisfied},
   * (3) replica movement does not make the potential nw outbound goal on destination broker more than the source.
   *
   * @param proposal     Proposal to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return True if proposal is acceptable by this goal, false otherwise.
   */
  @Override
  public boolean isProposalAcceptable(BalancingProposal proposal, ClusterModel clusterModel) {
    Replica replica = clusterModel.broker(proposal.sourceBrokerId()).replica(proposal.topicPartition());
    if (proposal.balancingAction().equals(BalancingAction.LEADERSHIP_MOVEMENT) || selfSatisfied(clusterModel,
        proposal)) {
      return true;
    }
    double destinationBrokerUtilization =
        clusterModel.potentialLeadershipLoadFor(clusterModel.broker(proposal.destinationBrokerId()).id()).expectedUtilizationFor(Resource.NW_OUT);
    double sourceBrokerUtilization = clusterModel.potentialLeadershipLoadFor(replica.broker().id()).expectedUtilizationFor(Resource.NW_OUT);
    double replicaUtilization = clusterModel.partition(replica.topicPartition()).leader().load().expectedUtilizationFor(Resource.NW_OUT);

    return destinationBrokerUtilization + replicaUtilization <= sourceBrokerUtilization;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new PotentialNwOutGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(_numSnapshots, _minMonitoredPartitionPercentage, false);
  }

  /**
   * Get the name of this goal. Name of a goal provides an identification for the goal in human readable format.
   */
  @Override
  public String name() {
    return PotentialNwOutGoal.class.getSimpleName();
  }

  /**
   * Get brokers that the rebalance process will go over to apply balancing actions to replicas they contain.
   *
   * @param clusterModel The state of the cluster.
   * @return A collection of brokers that the rebalance process will go over to apply balancing actions to replicas
   * they contain.
   */
  @Override
  protected Collection<Broker> brokersToBalance(ClusterModel clusterModel) {
    return clusterModel.deadBrokers().isEmpty() ? clusterModel.brokers() : clusterModel.deadBrokers();
  }

  /**
   * Check if the movement of potential outbound network utilization from the given source replica to given
   * destination broker is acceptable for this goal. The proposal is unacceptable if both of the following conditions
   * are met: (1) transfer of replica makes the potential network outbound utilization of the destination broker go
   * out of its allowed capacity, (2) broker containing the source replica is alive.
   *
   * @param clusterModel The state of the cluster.
   * @param proposal     Proposal containing information about
   * @return True if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingProposal proposal) {
    Replica sourceReplica = clusterModel.broker(proposal.sourceBrokerId()).replica(proposal.topicPartition());
    // If the source broker is dead and currently self healing dead brokers only, then the proposal must be executed.
    if (!sourceReplica.broker().isAlive() && _selfHealingDeadBrokersOnly) {
      return true;
    }
    Broker destinationBroker = clusterModel.broker(proposal.destinationBrokerId());
    double destinationBrokerUtilization = clusterModel.potentialLeadershipLoadFor(destinationBroker.id()).expectedUtilizationFor(Resource.NW_OUT);
    double allowedDestinationCapacity = destinationBroker.capacityFor(Resource.NW_OUT) * _balancingConstraint.capacityThreshold(Resource.NW_OUT);
    double replicaUtilization = clusterModel.partition(sourceReplica.topicPartition()).leader().load()
        .expectedUtilizationFor(Resource.NW_OUT);

    // Check whether replica or leadership transfer leads to violation of capacity limit requirement.
    return destinationBrokerUtilization + replicaUtilization <= allowedDestinationCapacity;
  }

  /**
   * Set the flag which indicates whether the self healing failed to relocate all replicas away from dead brokers in
   * its initial attempt. Since self healing has not been executed yet, this flag is false.
   *
   * @param clusterModel The state of the cluster.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel)
      throws AnalysisInputException, ModelInputException {
    _selfHealingDeadBrokersOnly = false;
  }

  /**
   * Update goal state after one round of self-healing / rebalance.
   *
   * @param clusterModel The state of the cluster.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel)
      throws AnalysisInputException {
    // Sanity check: No self-healing eligible replica should remain at a decommissioned broker.
    for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
      if (replica.broker().isAlive()) {
        continue;
      }
      if (_selfHealingDeadBrokersOnly) {
        throw new AnalysisInputException(
            "Self healing failed to move the replica away from decommissioned brokers.");
      }
      _selfHealingDeadBrokersOnly = true;
      LOG.warn(
          "Ignoring potential network outbound limit to relocate remaining replicas from dead brokers to healthy ones.");
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
   * @param excludedTopics The topics that should be excluded from the optimization proposal.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    Set<String> excludedTopics)
      throws AnalysisInputException, ModelInputException {
    double capacityLimit = broker.capacityFor(Resource.NW_OUT) * _balancingConstraint.capacityThreshold(Resource.NW_OUT);
    boolean estimatedMaxPossibleNwOutOverLimit = !broker.replicas().isEmpty() &&
        clusterModel.potentialLeadershipLoadFor(broker.id()).expectedUtilizationFor(Resource.NW_OUT) > capacityLimit;
    if (!estimatedMaxPossibleNwOutOverLimit) {
      // Estimated max possible utilization in broker is under the limit.
      return;
    }
    // Get candidate brokers
    Set<Broker> candidateBrokers = _selfHealingDeadBrokersOnly ?
        clusterModel.healthyBrokers() : brokersUnderEstimatedMaxPossibleNwOut(clusterModel);
    // Attempt to move replicas to eligible brokers until either the estimated max possible network out
    // limit requirement is satisfied for the broker or all replicas are checked.
    for (Replica replica : new ArrayList<>(broker.replicas())) {
      if (excludedTopics.contains(replica.topicPartition().topic())) {
        continue;
      }
      // Find the eligible brokers that this replica is allowed to move. Unless the target broker would go
      // over the potential outbound network capacity the movement will be successful.
      List<Broker> eligibleBrokers = new ArrayList<>(candidateBrokers);
      eligibleBrokers.removeAll(clusterModel.partition(replica.topicPartition()).partitionBrokers());
      eligibleBrokers.sort((b1, b2) -> Double.compare(b2.leadershipLoad().expectedUtilizationFor(Resource.NW_OUT),
                                                      b1.leadershipLoad().expectedUtilizationFor(Resource.NW_OUT)));
      Broker destinationBroker =
          maybeApplyBalancingAction(clusterModel, replica, eligibleBrokers, BalancingAction.REPLICA_MOVEMENT,
              optimizedGoals);
      if (destinationBroker != null) {
        int destinationBrokerId = destinationBroker.id();
        // Check if broker capacity limit is satisfied now.
        estimatedMaxPossibleNwOutOverLimit = !broker.replicas().isEmpty() &&
            clusterModel.potentialLeadershipLoadFor(broker.id()).expectedUtilizationFor(Resource.NW_OUT) > capacityLimit;
        if (!estimatedMaxPossibleNwOutOverLimit) {
          break;
        }
        // Update brokersUnderEstimatedMaxPossibleNwOut (for destination broker).
        double updatedDestBrokerPotentialNwOut =
            clusterModel.potentialLeadershipLoadFor(destinationBrokerId).expectedUtilizationFor(Resource.NW_OUT);
        if (!_selfHealingDeadBrokersOnly && updatedDestBrokerPotentialNwOut > capacityLimit) {
          candidateBrokers.remove(clusterModel.broker(destinationBrokerId));
        }
      }
    }
    if (estimatedMaxPossibleNwOutOverLimit) {
      // Utilization is above the max possible limit after all replicas in the source broker were checked.
      LOG.warn("Violated estimated max possible network out limit for broker id:{} limit:{} utilization:{}.",
          broker.id(), capacityLimit, clusterModel.potentialLeadershipLoadFor(broker.id()).expectedUtilizationFor(Resource.NW_OUT));
    }
  }

  /**
   * Get a set of brokers that for which the utilization of the estimated maximum possible network outbound
   * utilization is under the outbound network capacity limit.
   *
   * @param clusterModel The state of the cluster.
   * @return A set of brokers that for which the utilization of the estimated maximum possible network outbound
   * utilization is under the outbound network capacity limit.
   */
  private Set<Broker> brokersUnderEstimatedMaxPossibleNwOut(ClusterModel clusterModel) {
    Set<Broker> brokersUnderEstimatedMaxPossibleNwOut = new HashSet<>();
    double capacityThreshold = _balancingConstraint.capacityThreshold(Resource.NW_OUT);

    for (Broker healthyBroker : clusterModel.healthyBrokers()) {
      // We use the hosts capacity instead of the broker capacity.
      double capacityLimit = healthyBroker.host().capacityFor(Resource.NW_OUT) * capacityThreshold;
      if (clusterModel.potentialLeadershipLoadFor(healthyBroker.id()).expectedUtilizationFor(Resource.NW_OUT) < capacityLimit) {
        brokersUnderEstimatedMaxPossibleNwOut.add(healthyBroker);
      }
    }
    return brokersUnderEstimatedMaxPossibleNwOut;
  }

  private class PotentialNwOutGoalStatsComparator implements ClusterModelStatsComparator {
    private String _reasonForLastNegativeResult;

    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // Number of brokers under potential nw out in the current cannot be more than the pre-optimized stats.
      int stat1 = stats1.numBrokersUnderPotentialNwOut();
      int stat2 = stats2.numBrokersUnderPotentialNwOut();
      int result = Integer.compare(stat1, stat2);
      if (result < 0) {
        _reasonForLastNegativeResult = String.format("Violated %s. [Number of brokers under potential NwOut] "
                                                         + "post-optimization:%d " + "pre-optimization:%d",
                                                     name(), stat1, stat2);
      }
      return result;
    }

    @Override
    public String explainLastComparison() {
      return _reasonForLastNegativeResult;
    }
  }

}
