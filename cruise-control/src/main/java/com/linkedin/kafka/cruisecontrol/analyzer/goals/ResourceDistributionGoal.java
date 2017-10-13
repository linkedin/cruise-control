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
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for achieving the following soft goal:
 * <p>
 * SOFT GOAL#3: Balance resource distribution over brokers (e.g. cpu, disk, inbound / outbound network traffic).
 */
public abstract class ResourceDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  // Flag to indicate whether the self healing failed to relocate all replicas away from dead brokers in its initial
  // attempt and currently omitting the resource balance limit to relocate remaining replicas.
  private boolean _selfHealingDeadBrokersOnly;
  private Set<Integer> _brokerIdsOverBalanceLimit;

  /**
   * Constructor for Resource Distribution Goal.
   */
  public ResourceDistributionGoal() {

  }

  /**
   * Package private for unit test.
   */
  ResourceDistributionGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
    _brokerIdsOverBalanceLimit = new HashSet<>();
  }

  protected abstract Resource resource();

  /**
   * Check whether given proposal is acceptable by this goal. A proposal is acceptable by this goal if the movement
   * specified by the given proposal does not lead to a utilization in destination that is more than the broker
   * balance limit (in terms of utilization) broker utilization achieved after the balancing process of this goal.
   *
   * @param proposal     Proposal to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return True if proposal is acceptable by this goal, false otherwise.
   */
  @Override
  public boolean isProposalAcceptable(BalancingProposal proposal, ClusterModel clusterModel) {
    Replica sourceReplica = clusterModel.broker(proposal.sourceBrokerId()).replica(proposal.topicPartition());
    Broker destinationBroker = clusterModel.broker(proposal.destinationBrokerId());
    // Balanced resources cannot be more imbalanced. i.e. cannot go over the broker balance limit.
    return isMovementUnderBalanceLimit(clusterModel, sourceReplica, destinationBroker);
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new ResourceDistributionGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(_numSnapshots, _minMonitoredPartitionPercentage, false);
  }

  /**
   * Get the name of this goal. Name of a goal provides an identification for the goal in human readable format.
   */
  @Override
  public abstract String name();

  /**
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
   * Check if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise. A proposal is acceptable if: (1) destination broker utilization for the given resource is less
   * than the source broker utilization. (2) movement is acceptable (i.e. under the broker balance limit for balanced
   * resources) for already balanced resources. Already balanced resources are the ones that have gone through the
   * "resource distribution" process specified in this goal.
   *
   * @param clusterModel The state of the cluster.
   * @param proposal     Proposal containing information about
   * @return True if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingProposal proposal) {
    Broker destinationBroker = clusterModel.broker(proposal.destinationBrokerId());
    Replica sourceReplica = clusterModel.broker(proposal.sourceBrokerId()).replica(proposal.topicPartition());
    // If the source broker is dead and currently self healing dead brokers only, then the proposal must be executed.
    if (!sourceReplica.broker().isAlive() && _selfHealingDeadBrokersOnly) {
      return true;
    }

    //Check that current destination would not become more unbalanced.
    return isMovementUnderBalanceLimit(clusterModel, sourceReplica, destinationBroker);
  }

  /**
   * (1) Initialize the current resource to be balanced or self healed. resource selection is based on the order of
   * priority set in the _balancingConstraint.
   * (2) Set the flag which indicates whether the self healing failed to relocate all replicas away from dead brokers
   * in its initial attempt. Since self healing has not been executed yet, this flag is false.
   *
   * @param clusterModel The state of the cluster.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel) throws AnalysisInputException, ModelInputException {
    _brokerIdsOverBalanceLimit = new HashSet<>();
    _selfHealingDeadBrokersOnly = false;
  }

  /**
   * Update the current resource that is being balanced if there are still resources to be balanced, finish otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @throws AnalysisInputException
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel) throws AnalysisInputException {
    // Log broker Ids over balancing limit.
    if (!_brokerIdsOverBalanceLimit.isEmpty()) {
      LOG.warn("Utilization for broker ids:{} {} above the balance limit for:{} after {}.",
          _brokerIdsOverBalanceLimit, (_brokerIdsOverBalanceLimit.size() > 1) ? "are" : "is", resource(),
          (clusterModel.selfHealingEligibleReplicas().isEmpty()) ? "rebalance" : "self-healing");
      _brokerIdsOverBalanceLimit.clear();
      _succeeded = false;
    }
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
      LOG.warn("Omitting resource balance limit to relocate remaining replicas from dead brokers to healthy ones.");
      return;
    }
    // No dead broker contains replica.
    _selfHealingDeadBrokersOnly = false;

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
   * (1) REBALANCE BY LEADERSHIP MOVEMENT:
   * Perform leadership movement to ensure that the load on brokers for the outbound network load is under the balance
   * limit.
   * <p>
   * (2) REBALANCE BY REPLICA MOVEMENT:
   * Perform optimization via replica movement for the given resource (without breaking the balance for already
   * balanced resources) to ensure rebalance: The load on brokers for the given resource is under the balance limit.
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
    double brokerCapacity = broker.capacityFor(resource());
    double balanceThreshold = balanceThreshold(clusterModel);
    double brokerBalanceLimit = brokerCapacity * balanceThreshold;
    double brokerLowUtilizationBar = _balancingConstraint.lowUtilizationThreshold(resource()) * brokerCapacity;
    double brokerUtilization = broker.load().expectedUtilizationFor(resource());
    boolean brokerUtilizationOverLimit = brokerUtilization > brokerBalanceLimit && brokerUtilization > brokerLowUtilizationBar;

    double hostCapacity = broker.host().capacityFor(resource());
    double hostBalanceLimit = hostCapacity * balanceThreshold;
    double hostLowUtilizationBar = _balancingConstraint.lowUtilizationThreshold(resource()) * hostCapacity;
    double hostUtilization = broker.host().load().expectedUtilizationFor(resource());
    boolean hostUtilizationOverLimit = hostUtilization > hostBalanceLimit && hostUtilization > hostLowUtilizationBar;

    boolean isUtilizationOverlimit = resource().isHostResource() ?
        brokerUtilizationOverLimit && hostUtilizationOverLimit : brokerUtilizationOverLimit;
    // If the host utilization is over limit then there must be at least one broker whose utilization is over limit.
    // We should only balance those brokers.
    if (broker.isAlive() && !isUtilizationOverlimit) {
      // return if the broker is already under limit.
      return;
    }

    // First try leadership movement
    if (resource() == Resource.NW_OUT || resource() == Resource.CPU) {
      if (!rebalanceByMovingLeaders(broker, clusterModel, optimizedGoals, hostBalanceLimit, brokerBalanceLimit)) {
        LOG.debug("Successfully balanced {} for broker {} by moving leaders.", resource(), broker.id());
        return;
      }
    }

    // Update broker ids over the balance limit for logging purposes.
    if (rebalanceByMovingReplicas(broker, clusterModel, optimizedGoals, excludedTopics, balanceThreshold,
                                  hostBalanceLimit, brokerBalanceLimit)) {
      _brokerIdsOverBalanceLimit.add(broker.id());
      _succeeded = false;
      LOG.debug("Failed to balance {} for broker {} with replica and leader movements", resource(), broker.id());
    } else {
      LOG.debug("Successfully balanced {} for broker {} by moving leaders and replicas.", resource(), broker.id());
    }
  }

  /**
   * Rebalance the broker by leader movement until it reaches given host and broker utilization limit.
   * @return true if the utilization is still over limit, false otherwise.
   * @throws ModelInputException
   * @throws AnalysisInputException
   */
  protected boolean rebalanceByMovingLeaders(Broker broker,
                                             ClusterModel clusterModel,
                                             Set<Goal> optimizedGoals,
                                             double hostBalanceLimit,
                                             double brokerBalanceLimit)
      throws ModelInputException, AnalysisInputException {
    // Attempt to move leaders until the resource utilization on the broker is under the balance limit.
    // Leaders are sorted in by descending order of preference to relocate. Preference is based on utilization.
    for (Replica leader : broker.sortedReplicas(resource())) {
      // Get followers of this leader and sort them in ascending order by their broker resource utilization.
      List<Replica> followers = clusterModel.partition(leader.topicPartition()).followers();
      clusterModel.sortReplicasInAscendingOrderByBrokerResourceUtilization(followers, resource());
      List<Broker> eligibleBrokers = followers.stream().map(Replica::broker).collect(Collectors.toList());
      boolean isUtilizationOverLimit = maybeApplyResourceDistributionAction(clusterModel, leader, eligibleBrokers,
                                                                    BalancingAction.LEADERSHIP_MOVEMENT, optimizedGoals,
                                                                    hostBalanceLimit, brokerBalanceLimit);
      if (!isUtilizationOverLimit) {
        return false;
      }
    }
    return !broker.replicas().isEmpty();
  }

  /**
   * Rebalance the broker by replica movement until it reaches given host and broker utilization limit.
   * @return true if the utilization is still over limit, false otherwise.
   * @throws ModelInputException
   * @throws AnalysisInputException
   */
  protected boolean rebalanceByMovingReplicas(Broker broker,
                                              ClusterModel clusterModel,
                                              Set<Goal> optimizedGoals,
                                              Set<String> excludedTopics,
                                              double balanceThreshold,
                                              double hostBalanceLimit,
                                              double brokerBalanceLimit)
      throws ModelInputException, AnalysisInputException {
    LOG.debug("Balancing {} for broker {}, balanceThreshold = {}, hostBalanceLimit = {}, brokerBalanceLimit = {}",
              resource(), broker.id(), balanceThreshold, hostBalanceLimit, brokerBalanceLimit);
    // Get and sort candidate destination brokers.
    List<Broker> candidateBrokers;
    if (_selfHealingDeadBrokersOnly) {
      candidateBrokers = new ArrayList<>(clusterModel.healthyBrokers());
      candidateBrokers.sort((b1, b2) -> Double.compare(b2.leadershipLoad().expectedUtilizationFor(Resource.NW_OUT),
                                                       b1.leadershipLoad().expectedUtilizationFor(Resource.NW_OUT)));
    } else {
      candidateBrokers = clusterModel.sortedHealthyBrokersUnderThreshold(resource(), balanceThreshold);
    }
    // Attempt to move replicas until the resource utilization on the broker is under the balance limit.
    // Replicas are sorted in descending order of preference to relocate. Preference is based on utilization.
    for (Replica replica : broker.sortedReplicas(resource())) {
      if (excludedTopics.contains(replica.topicPartition().topic())) {
        continue;
      }

      // It does not make sense to move a replica without utilization from a live broker.
      if (replica.load().expectedUtilizationFor(resource()) == 0.0 && broker.isAlive()) {
        break;
      }
      // Eligible brokers are healthy brokers without partition brokers.
      List<Broker> eligibleBrokers = new ArrayList<>(candidateBrokers);
      eligibleBrokers.removeAll(clusterModel.partition(replica.topicPartition()).partitionBrokers());
      boolean isUtilizationOverLimit =
          maybeApplyResourceDistributionAction(clusterModel, replica, eligibleBrokers,
                                               BalancingAction.REPLICA_MOVEMENT, optimizedGoals,
                                               hostBalanceLimit, brokerBalanceLimit);
      if (!isUtilizationOverLimit) {
        return false;
      }
    }
    // If all the replicas has been moved away from the broker and we still reach here, that means the broker
    // capacity is negative, i.e. the broker is dead. So as long as there is no replicas on the broker anymore
    // we consider it as not over limit.
    return !broker.replicas().isEmpty();
  }

  /**
   * Attempt to apply the given balancing action to the given replica in the given cluster. Please refer to
   * {@link #maybeApplyBalancingAction(com.linkedin.kafka.cruisecontrol.model.ClusterModel, Replica, List,
   * BalancingAction, Set)} for the criteria of an attempt * to be successful.
   *
   * @param clusterModel       The state of the cluster.
   * @param replica            Replica for which the given action will be attempted to be applied.
   * @param eligibleBrokers    Eligible brokers to receive the given replica or its leadership.
   * @param action             Balancing action.
   * @param optimizedGoals     Optimized goals.
   * @param hostBalanceLimit   Indicates the maximum amount of utilization that the host can have.
   * @param brokerBalanceLimit Indicates the maximum amount of utilization that the broker can have.
   * @return True if utilization of source broker is over the balance limit, false otherwise.
   */
  private boolean maybeApplyResourceDistributionAction(ClusterModel clusterModel,
                                                       Replica replica,
                                                       List<Broker> eligibleBrokers,
                                                       BalancingAction action,
                                                       Set<Goal> optimizedGoals,
                                                       double hostBalanceLimit,
                                                       double brokerBalanceLimit)
      throws AnalysisInputException, ModelInputException {
    Broker originalBroker = replica.broker();
    LOG.trace("Moving {} to {} in order to balance {}", replica, eligibleBrokers, resource());
    maybeApplyBalancingAction(clusterModel, replica, eligibleBrokers, action, optimizedGoals);
    boolean brokerUtilizationOverLimit = originalBroker.load().expectedUtilizationFor(resource()) > brokerBalanceLimit;
    if (resource().isHostResource()) {
      boolean hostUtilizationOverLimit = originalBroker.host().load().expectedUtilizationFor(resource()) > hostBalanceLimit;
      return hostUtilizationOverLimit && brokerUtilizationOverLimit;
    } else {
      // If capacity limit was not satisfied before, check if it is satisfied now.
      return brokerUtilizationOverLimit;
    }

  }

  /**
   * Check whether movement is acceptable for balanced resources (resources that have gone through the "resource
   * distribution" process specified in this goal). Balanced resource distribution cannot be made more imbalanced.
   * In order to accept this move, balance limit for the destination broker shall not be exceeded.
   *
   * @param clusterModel      The state of the cluster.
   * @param sourceReplica     Source replica to be moved.
   * @param destinationBroker Destination broker of the movement.
   * @return True if movement is acceptable for balanced resources, false otherwise.
   */
  protected boolean isMovementUnderBalanceLimit(ClusterModel clusterModel,
                                                Replica sourceReplica,
                                                Broker destinationBroker) {
    double balanceThreshold = balanceThreshold(clusterModel);

    // Already balanced resources cannot be made more imbalanced.
    double destinationBrokerBalanceLimit = destinationBroker.capacityFor(resource()) * balanceThreshold;
    double destinationHostBalanceLimit = destinationBroker.host().capacityFor(resource()) * balanceThreshold;
    double destinationBrokerUtilization = destinationBroker.load().expectedUtilizationFor(resource());
    double destinationHostUtilization = destinationBroker.host().load().expectedUtilizationFor(resource());
    double replicaUtilization = sourceReplica.load().expectedUtilizationFor(resource());
    return (destinationBrokerUtilization + replicaUtilization <= destinationBrokerBalanceLimit) &&
        (!resource().isHostResource() || destinationHostUtilization + replicaUtilization <= destinationHostBalanceLimit);
  }

  protected double balanceThreshold(ClusterModel clusterModel) {
    return (clusterModel.load().expectedUtilizationFor(resource()) / clusterModel.capacityFor(resource()))
        * balancePercentageWithMargin(resource());
  }

  private double balancePercentageWithMargin(Resource resource) {
    return 1 + (_balancingConstraint.balancePercentage(resource) - 1) * BALANCE_MARGIN;
  }

  private class ResourceDistributionGoalStatsComparator implements ClusterModelStatsComparator {
    private String _reasonForLastNegativeResult;

    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // Number of balanced brokers in the highest priority resource cannot be more than the pre-optimized
      // stats. This constraint is applicable for the rest of the resources, if their higher priority resources
      // have the same number of balanced brokers in their corresponding pre- and post-optimized stats.
        int numBalancedBroker1 = stats1.numBalancedBrokersByResource().get(resource());
        int numBalancedBroker2 = stats2.numBalancedBrokersByResource().get(resource());
        // First compare the
        if (numBalancedBroker2 > numBalancedBroker1) {
            _reasonForLastNegativeResult = String.format(
                "Violated %s. [Number of Balanced Brokers] for resource %s. post-optimization:%d pre-optimization:%d",
                name(), resource(), numBalancedBroker1, numBalancedBroker2);
            return -1;
        }
      return 1;
    }

    @Override
    public String explainLastComparison() {
      return _reasonForLastNegativeResult;
    }
  }

  @Override
  public String goalType() {
    return _goalType.name();
  }

  @Override
  public String goalDescription() {
    return "Attempt to make the resource (CPU, Disk, Network) utilization variance among all the brokers are within a certain range.";
  }
}
