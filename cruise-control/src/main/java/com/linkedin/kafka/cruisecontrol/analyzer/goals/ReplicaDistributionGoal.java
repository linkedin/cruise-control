/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;

import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;


/**
 * Class for achieving the following soft goal:
 * <p>
 * SOFT GOAL#4: Distribute partitions (independent of their topics) evenly over brokers.
 */
public class ReplicaDistributionGoal extends AbstractGoal {
  private ReplicaDistributionTarget _replicaDistributionTarget;

  /**
   * Constructor for Replica Distribution Goal. Initially replica distribution target is null.
   */
  public ReplicaDistributionGoal() {

  }

  public ReplicaDistributionGoal(BalancingConstraint balancingConstraint) {
    _balancingConstraint = balancingConstraint;
  }

  /**
   * Check whether given proposal is acceptable by this goal. A proposal is acceptable if the number of replicas at
   * the source broker are more than the number of replicas at the destination (remote) broker.
   *
   * @param proposal     Proposal to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return True if proposal is acceptable by this goal, false otherwise.
   */
  @Override
  public boolean isProposalAcceptable(BalancingProposal proposal, ClusterModel clusterModel) {
    int numLocalReplicas = clusterModel.broker(proposal.sourceBrokerId()).replicas().size();
    int numRemoteReplicas = clusterModel.broker(proposal.destinationBrokerId()).replicas().size();

    return numRemoteReplicas < numLocalReplicas;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new ReplicaDistributionGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(1, 0.0, true);
  }

  /**
   * Get the name of this goal. Name of a goal provides an identification for the goal in human readable format.
   */
  @Override
  public String name() {
    return ReplicaDistributionGoal.class.getSimpleName();
  }

  /**
   * Heal the given cluster without violating the requirements of optimized goals.
   *
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   */
  protected void healCluster(ClusterModel clusterModel, Set<Goal> optimizedGoals)
      throws AnalysisInputException, ModelInputException {
    // Move self healed replicas (if their broker is overloaded or they reside at dead brokers) to eligible ones.
    for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
      _replicaDistributionTarget.moveSelfHealingEligibleReplicaToEligibleBroker(clusterModel, replica,
          replica.broker().replicas().size(), optimizedGoals);
    }
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
    if (!clusterModel.deadBrokers().isEmpty()) {
      return clusterModel.deadBrokers();
    }
    // Brokers having over minimum number of replicas per broker are eligible for balancing.
    Set<Broker> brokersToBalance = new HashSet<>();
    int minNumReplicasPerBroker = _replicaDistributionTarget.minNumReplicasPerBroker();
    brokersToBalance.addAll(clusterModel.brokers().stream()
        .filter(broker -> broker.replicas().size() > minNumReplicasPerBroker)
        .collect(Collectors.toList()));
    return brokersToBalance;
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
    // This method is not used by this goal.
    return false;
  }

  /**
   * Initiates replica distribution goal.
   *
   * @param clusterModel The state of the cluster.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel)
      throws AnalysisInputException, ModelInputException {
    Set<Broker> brokers = clusterModel.healthyBrokers();
    // Populate a map of replica distribution target in the cluster.
    int numReplicasToBalance = 0;
    Map<TopicPartition, List<Integer>> replicaDistribution = clusterModel.getReplicaDistribution();
    for (List<Integer> replicaList : replicaDistribution.values()) {
      numReplicasToBalance += replicaList.size();
    }

    _replicaDistributionTarget = new ReplicaDistributionTarget(numReplicasToBalance, brokers);
    for (Broker broker : brokers) {
      _replicaDistributionTarget.setBrokerEligibilityForReceivingReplica(broker.id(), broker.replicas().size());
    }
  }

  /**
   * Finish healing / rebalance of this goal.
   *
   * @param clusterModel The state of the cluster.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel)
      throws AnalysisInputException {
    // Sanity check: No self-healing eligible replica should remain at a decommissioned broker.
    for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
      if (!replica.broker().isAlive()) {
        throw new AnalysisInputException(
            "Self healing failed to move the replica away from decommissioned broker.");
      }
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
    if (!clusterModel.selfHealingEligibleReplicas().isEmpty() && !broker.isAlive() && !broker.replicas().isEmpty()) {
      healCluster(clusterModel, optimizedGoals);
    } else {
      // If broker is overloaded, move local replicas to eligible brokers.
      _replicaDistributionTarget.moveReplicasInSourceBrokerToEligibleBrokers(clusterModel, new HashSet<>(broker.replicas()),
                                                                             optimizedGoals, excludedTopics);
    }
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
        _reasonForLastNegativeResult = String.format("Violated %s. [Standard Deviation of Replica Distribution] "
                                                         + "post-optimization:%.3f pre-optimization:%.3f",
                                                     name(), stDev1, stDev2);
      }
      return result;
    }

    @Override
    public String explainLastComparison() {
      return _reasonForLastNegativeResult;
    }
  }
}
