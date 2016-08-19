/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
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

import static com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils.EPSILON;


/**
 * Class for achieving the following soft goal:
 * <p>
 * SOFT GOAL#3: Balance resource distribution over brokers (e.g. cpu, disk, inbound / outbound network traffic).
 */
public class ResourceDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  private Set<Resource> _balancedResources;
  private Resource _currentResource;
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
    _balancedResources = new HashSet<>();
    _currentResource = null;
    _brokerIdsOverBalanceLimit = new HashSet<>();
  }

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

    if (proposal.balancingAction() == BalancingAction.LEADERSHIP_MOVEMENT) {
      return isMovementUnderBalanceLimit(Resource.NW_OUT, clusterModel, sourceReplica, destinationBroker)
          && isMovementUnderBalanceLimit(Resource.CPU, clusterModel, sourceReplica, destinationBroker);
    }
    // Balanced resources cannot be more imbalanced. i.e. cannot go over the broker balance limit.
    for (Resource resource : _balancedResources) {
      if (!isMovementUnderBalanceLimit(resource, clusterModel, sourceReplica, destinationBroker)) {
        return false;
      }
    }
    return true;
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
  public String name() {
    return ResourceDistributionGoal.class.getName();
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
    if (!isMovementUnderBalanceLimit(_currentResource, clusterModel, sourceReplica, destinationBroker)) {
      return false;
    }

    // Balanced resources cannot be more imbalanced. i.e. cannot go over the broker balance limit.
    for (Resource resource : _balancedResources) {
      if (!isMovementUnderBalanceLimit(resource, clusterModel, sourceReplica, destinationBroker)) {
        return false;
      }
    }
    return true;
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
    _balancedResources = new HashSet<>();
    _brokerIdsOverBalanceLimit = new HashSet<>();
    // Set the initial resource to heal or rebalance in the order of priority set in the _balancingConstraint.
    _currentResource = _balancingConstraint.resources().get(0);
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
          _brokerIdsOverBalanceLimit, (_brokerIdsOverBalanceLimit.size() > 1) ? "are" : "is", _currentResource,
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

    // Update balanced resources.
    _balancedResources.add(_currentResource);
    if (_balancedResources.size() == _balancingConstraint.resources().size()) {
      // Sanity check: No self-healing eligible replica should remain at a decommissioned broker.
      for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
        if (!replica.broker().isAlive()) {
          throw new AnalysisInputException(
              "Self healing failed to move the replica away from decommissioned broker.");
        }
      }
      finish();
    } else {
      // Set the current resource to heal or rebalance in the order of priority set in the _balancingConstraint.
      _currentResource = _balancingConstraint.resources().get(_balancedResources.size());
    }
  }

  /**
   * 1.1. HEAL BY LEADERSHIP MOVEMENT:
   * Ensure that leadership on self-healing eligible replicas are distributed to replicas on healthy brokers while
   * satisfying the balance limit.
   * <p>
   * 1.2. HEAL BY REPLICA MOVEMENT:
   * Ensure that the self-healing replicas are distributed to healthy brokers without making the cluster more
   * imbalanced, Please refer to {@link #selfSatisfied} for the criteria of accepting a replica movement for the
   * this goal.
   * <p>
   * 2. Log brokers containing self healing eligible replicas above the balance limit after self healing.
   *
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   */
  @Override
  protected void healCluster(ClusterModel clusterModel, Set<Goal> optimizedGoals)
      throws AnalysisInputException, ModelInputException {
    double clusterUtilization = clusterModel.load().expectedUtilizationFor(_currentResource);
    double clusterCapacity = clusterModel.capacityFor(_currentResource);
    double balanceThreshold = (clusterUtilization / clusterCapacity) * _balancingConstraint.balancePercentage(_currentResource);

    if (_currentResource.equals(Resource.NW_OUT)) {
      for (Replica leader : clusterModel.selfHealingEligibleReplicas()) {
        double brokerBalanceLimit = leader.broker().capacityFor(_currentResource) * balanceThreshold;
        if (!leader.isLeader()) {
          // Current self healing eligible replica is not leader.
          continue;
        }
        // Check whether the source broker is over the resource balance limit.
        boolean isUtilizationOverLimit = leader.broker().load().expectedUtilizationFor(_currentResource) > brokerBalanceLimit;
        // Utilization of broker is already under the limit for the current resource.
        if (!isUtilizationOverLimit) {
          continue;
        }
        // Get followers of this leader and sort them in ascending order by their broker resource utilization.
        List<Replica> followers = clusterModel.partition(leader.topicPartition()).followers();
        clusterModel.sortReplicasInAscendingOrderByBrokerResourceUtilization(followers, _currentResource);
        List<Broker> eligibleBrokers = followers.stream().map(Replica::broker).collect(Collectors.toList());

        maybeApplyBalancingAction(clusterModel, leader, eligibleBrokers, BalancingAction.LEADERSHIP_MOVEMENT,
            optimizedGoals);
      }
    } else {
      for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
        if (_selfHealingDeadBrokersOnly && replica.broker().isAlive()) {
          continue;
        }
        double brokerBalanceLimit = replica.broker().capacityFor(_currentResource) * balanceThreshold;
        // Check whether the source broker is over the resource balance limit.
        boolean isUtilizationOverLimit = replica.broker().load().expectedUtilizationFor(_currentResource) > brokerBalanceLimit;
        // Utilization of broker is already under the limit for the current resource.
        if (!isUtilizationOverLimit) {
          continue;
        }
        // Find the eligible brokers that this replica is allowed to move.
        List<Broker> eligibleBrokers;
        if (_selfHealingDeadBrokersOnly) {
          // The movement will be successful to mitigate source replicas.
          eligibleBrokers = new ArrayList<>(clusterModel.healthyBrokers());
        } else {
          // Unless the target broker would go over the balance limit, the movement will be successful.
          eligibleBrokers = new ArrayList<>(
              clusterModel.sortedHealthyBrokersUnderThreshold(_currentResource, balanceThreshold));
        }
        // Remove partition brokers from eligible brokers.
        eligibleBrokers.removeAll(clusterModel.partition(replica.topicPartition()).partitionBrokers());

        // Broker of this replica violates capacity threshold, attempt to move replica to another available broker.
        maybeApplyBalancingAction(clusterModel, replica, eligibleBrokers, BalancingAction.REPLICA_MOVEMENT,
            optimizedGoals);
      }
    }

    // Check and log the final utilization status of broker containing self healing eligible replica.
    for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
      double brokerBalanceLimit = replica.broker().capacityFor(_currentResource) * balanceThreshold;
      double expectedBrokerUtilizationForResource = replica.broker().load().expectedUtilizationFor(_currentResource);
      if (expectedBrokerUtilizationForResource > brokerBalanceLimit) {
        _brokerIdsOverBalanceLimit.add(replica.broker().id());
      }
    }
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
    double balanceThreshold = balanceThreshold(_currentResource, clusterModel);
    double brokerBalanceLimit = broker.capacityFor(_currentResource) * balanceThreshold;
    double brokerLowUtilizationBar =
        _balancingConstraint.lowUtilizationThreshold(_currentResource) * broker.capacityFor(_currentResource);
    double brokerUtilization = broker.load().expectedUtilizationFor(_currentResource);
    boolean brokerUtilizationOverLimit = brokerUtilization > brokerBalanceLimit && brokerUtilization > brokerLowUtilizationBar;

    double hostBalanceLimit = broker.host().capacityFor(_currentResource) * balanceThreshold;
    double hostLowUtilizationBar = _balancingConstraint.lowUtilizationThreshold(_currentResource) * broker.host().capacityFor(_currentResource);
    double hostUtilization = broker.host().load().expectedUtilizationFor(_currentResource);
    boolean hostUtilizationOverLimit = hostUtilization > hostBalanceLimit && hostUtilization > hostLowUtilizationBar;

    boolean isUtilizationOverLimit = brokerUtilizationOverLimit && hostUtilizationOverLimit;

    // Utilization for the broker is already under the limit for the current resource.
    if (!isUtilizationOverLimit) {
      return;
    }
    // REBALANCE BY LEADERSHIP MOVEMENT:
    if (_currentResource.equals(Resource.NW_OUT)) {
      // Attempt to move leaders until the resource utilization on the broker is under the balance limit.
      // Leaders are sorted in by descending order of preference to relocate. Preference is based on utilization.
      for (Replica leader : broker.sortedReplicas(_currentResource)) {
        // Get followers of this leader and sort them in ascending order by their broker resource utilization.
        List<Replica> followers = clusterModel.partition(leader.topicPartition()).followers();
        clusterModel.sortReplicasInAscendingOrderByBrokerResourceUtilization(followers, _currentResource);
        List<Broker> eligibleBrokers = followers.stream().map(Replica::broker).collect(Collectors.toList());
        isUtilizationOverLimit = maybeApplyResourceDistributionAction(clusterModel, leader, eligibleBrokers,
            BalancingAction.LEADERSHIP_MOVEMENT, optimizedGoals, hostBalanceLimit, brokerBalanceLimit);
        if (!isUtilizationOverLimit) {
          break;
        }
      }
    } else {
      // REBALANCE BY REPLICA MOVEMENT:
      // Get and sort candidate destination brokers.
      List<Broker> sortedHealthyBrokersUnderBalanceLimit =
          clusterModel.sortedHealthyBrokersUnderThreshold(_currentResource, balanceThreshold);
      // Attempt to move replicas until the resource utilization on the broker is under the balance limit.
      // Replicas are sorted in descending order of preference to relocate. Preference is based on utilization.
      for (Replica replica : broker.sortedReplicas(_currentResource)) {
        if (excludedTopics.contains(replica.topicPartition().topic())) {
          continue;
        }
        // Eligible brokers are healthy brokers without partition brokers.
        List<Broker> eligibleBrokers = new ArrayList<>(sortedHealthyBrokersUnderBalanceLimit);
        eligibleBrokers.removeAll(clusterModel.partition(replica.topicPartition()).partitionBrokers());
        isUtilizationOverLimit = maybeApplyResourceDistributionAction(clusterModel, replica, eligibleBrokers,
            BalancingAction.REPLICA_MOVEMENT, optimizedGoals, hostBalanceLimit, brokerBalanceLimit);
        if (!isUtilizationOverLimit) {
          break;
        }
      }
    }
    // Update broker ids over the balance limit for logging purposes.
    if (isUtilizationOverLimit) {
      _brokerIdsOverBalanceLimit.add(broker.id());
    }
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
    maybeApplyBalancingAction(clusterModel, replica, eligibleBrokers, action, optimizedGoals);
    boolean brokerUtilizationOverLimit = originalBroker.load().expectedUtilizationFor(_currentResource) > brokerBalanceLimit;
    if (_currentResource.isHostResource()) {
      boolean hostUtilizationOverLimit = originalBroker.host().load().expectedUtilizationFor(_currentResource) > hostBalanceLimit;
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
   * @param resource          Resource for which the movement will be checked.
   * @param clusterModel      The state of the cluster.
   * @param sourceReplica     Source replica to be moved.
   * @param destinationBroker Destination broker of the movement.
   * @return True if movement is acceptable for balanced resources, false otherwise.
   */
  protected boolean isMovementUnderBalanceLimit(Resource resource,
                                              ClusterModel clusterModel,
                                              Replica sourceReplica,
                                              Broker destinationBroker) {
    double balanceThreshold = balanceThreshold(resource, clusterModel);

    // Already balanced resources cannot be made more imbalanced.
    double destinationBrokerBalanceLimit = destinationBroker.capacityFor(resource) * balanceThreshold;
    double destinationHostBalanceLimit = destinationBroker.host().capacityFor(resource) * balanceThreshold;
    double destinationBrokerUtilization = destinationBroker.load().expectedUtilizationFor(resource);
    double destinationHostUtilization = destinationBroker.host().load().expectedUtilizationFor(resource);
    double replicaUtilization = sourceReplica.load().expectedUtilizationFor(resource);
    return (destinationBrokerUtilization + replicaUtilization <= destinationBrokerBalanceLimit) &&
        (!resource.isHostResource() || destinationHostUtilization + replicaUtilization <= destinationHostBalanceLimit);
  }

  protected double balanceThreshold(Resource resource, ClusterModel clusterModel) {
    return (clusterModel.load().expectedUtilizationFor(resource) / clusterModel.capacityFor(resource))
        * _balancingConstraint.balancePercentage(resource) * BALANCE_MARGIN;
  }

  private class ResourceDistributionGoalStatsComparator implements ClusterModelStatsComparator {
    private String _reasonForLastNegativeResult;

    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // Number of balanced brokers in the highest priority resource cannot be more than the pre-optimized
      // stats. This constraint is applicable for the rest of the resources, if their higher priority resources
      // have the same number of balanced brokers in their corresponding pre- and post-optimized stats.
      for (Resource resource : _balancingConstraint.resources()) {
        int stat1 = stats1.numBalancedBrokersByResource().get(resource);
        int stat2 = stats2.numBalancedBrokersByResource().get(resource);
        double stDev1 = stats1.resourceUtilizationStats().get(Statistic.ST_DEV).get(resource);
        double stDev2 = stats2.resourceUtilizationStats().get(Statistic.ST_DEV).get(resource);
        if (stat2 > stat1) {
            _reasonForLastNegativeResult = String.format(
                "Violated %s. [Number of Balanced Brokers] for resource %s. post-optimization:%d pre-optimization:%d",
                name(), resource, stat1, stat2);
            return -1;
        } else if (stat2 < stat1
            || AnalyzerUtils.compare(stDev1, stDev2, EPSILON) < 0) {
          // Cannot enforce the constraint for the rest of the resources because the initial stats for
          // the remaining resources has changed.
          break;
        }
      }
      return 1;
    }

    @Override
    public String explainLastComparison() {
      return _reasonForLastNegativeResult;
    }
  }
}