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
import com.linkedin.kafka.cruisecontrol.model.Host;
import com.linkedin.kafka.cruisecontrol.model.Load;
import com.linkedin.kafka.cruisecontrol.model.Replica;

import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal.ChangeType.ADD;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal.ChangeType.REMOVE;


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
  private Set<Integer> _brokerIdsAboveBalanceUpperLimit;
  private Set<Integer> _brokerIdsUnderBalanceLowerLimit;

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
    _brokerIdsAboveBalanceUpperLimit = new HashSet<>();
    _brokerIdsUnderBalanceLowerLimit = new HashSet<>();
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
    return isLoadUnderBalanceUpperLimitAfterChange(clusterModel, sourceReplica.load(), destinationBroker, ADD) &&
        isLoadAboveBalanceLowerLimitAfterChange(clusterModel, sourceReplica.load(), sourceReplica.broker(), REMOVE);
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
    return isLoadUnderBalanceUpperLimitAfterChange(clusterModel, sourceReplica.load(), destinationBroker, ADD) &&
        isLoadAboveBalanceLowerLimitAfterChange(clusterModel, sourceReplica.load(), sourceReplica.broker(), REMOVE);
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
    _brokerIdsAboveBalanceUpperLimit = new HashSet<>();
    _brokerIdsUnderBalanceLowerLimit = new HashSet<>();
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
    if (!_brokerIdsAboveBalanceUpperLimit.isEmpty()) {
      LOG.warn("Utilization for broker ids:{} {} above the balance limit for:{} after {}.",
               _brokerIdsAboveBalanceUpperLimit, (_brokerIdsAboveBalanceUpperLimit.size() > 1) ? "are" : "is", resource(),
               (clusterModel.selfHealingEligibleReplicas().isEmpty()) ? "rebalance" : "self-healing");
      _brokerIdsAboveBalanceUpperLimit.clear();
      _succeeded = false;
    }
    if (!_brokerIdsUnderBalanceLowerLimit.isEmpty()) {
      LOG.warn("Utilization for broker ids:{} {} under the balance limit for:{} after {}.",
               _brokerIdsUnderBalanceLowerLimit, (_brokerIdsUnderBalanceLowerLimit.size() > 1) ? "are" : "is", resource(),
               (clusterModel.selfHealingEligibleReplicas().isEmpty()) ? "rebalance" : "self-healing");
      _brokerIdsUnderBalanceLowerLimit.clear();
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
    // If the host utilization is over limit then there must be at least one broker whose utilization is over limit.
    // We should only balance those brokers.
    boolean requireLessLoad = !isLoadUnderBalanceUpperLimitAfterChange(clusterModel, null, broker, REMOVE);
    boolean requireMoreLoad = !isLoadAboveBalanceLowerLimitAfterChange(clusterModel, null, broker, ADD);
    if (broker.isAlive() && !requireMoreLoad && !requireLessLoad) {
      // return if the broker is already under limit.
      return;
    }

    // First try leadership movement
    if (resource() == Resource.NW_OUT || resource() == Resource.CPU) {
      if (requireLessLoad && !rebalanceByMovingLeadersOut(broker, clusterModel, optimizedGoals)) {
        LOG.debug("Successfully balanced {} for broker {} by moving out leaders.", resource(), broker.id());
        return;
      } else if (requireMoreLoad && !rebalanceByMovingLoadIn(broker, clusterModel, optimizedGoals,
                                                             BalancingAction.LEADERSHIP_MOVEMENT)) {
        LOG.debug("Successfully balanced {} for broker {} by moving in leaders.", resource(), broker.id());
        return;
      }
    }

    // Update broker ids over the balance limit for logging purposes.
    if (requireLessLoad && rebalanceByMovingReplicasOut(broker, clusterModel, optimizedGoals, excludedTopics)) {
      _brokerIdsAboveBalanceUpperLimit.add(broker.id());
      LOG.debug("Failed to balance {} for broker {} with replica and leader movements to reduce load.", resource(), broker.id());
    } else if (requireMoreLoad && rebalanceByMovingLoadIn(broker, clusterModel, optimizedGoals,
                                                          BalancingAction.REPLICA_MOVEMENT)) {
      _brokerIdsUnderBalanceLowerLimit.add(broker.id());
      LOG.debug("Failed to balance {} for broker {} with replica and leader movements to increase load.", resource(), broker.id());
    } else {
      LOG.debug("Successfully balanced {} for broker {} by moving leaders and replicas.", resource(), broker.id());
    }
  }

  protected boolean rebalanceByMovingLoadIn(Broker broker,
                                            ClusterModel clusterModel,
                                            Set<Goal> optimizedGoals,
                                            BalancingAction balancingAction)
      throws AnalysisInputException, ModelInputException {
    PriorityQueue<Broker> eligibleBrokers = new PriorityQueue<>(
        (b1, b2) -> Double.compare(utilizationPercentage(b2), utilizationPercentage(b1)));
    double clusterUtilizationPercentage =
        clusterModel.load().expectedUtilizationFor(resource()) / clusterModel.capacityFor(resource());
    clusterModel.healthyBrokers().forEach(b -> {
      if (utilizationPercentage(b) > clusterUtilizationPercentage) {
        eligibleBrokers.add(b);
      }
    });

    // Stop when all the replicas are leaders or there is no leader can be moved in anymore.
    while (broker.leaderReplicas().size() != broker.replicas().size() && !eligibleBrokers.isEmpty()) {
      Broker sourceBroker = eligibleBrokers.poll();
      for (Replica replica : sourceBroker.sortedReplicas(resource())) {
        boolean eligibleReplica = false;
        if (balancingAction == BalancingAction.REPLICA_MOVEMENT && broker.replica(replica.topicPartition()) == null) {
          eligibleReplica = true;
        } else if (balancingAction == BalancingAction.LEADERSHIP_MOVEMENT && replica.isLeader()
                && broker.replica(replica.topicPartition()) != null) {
          eligibleReplica = true;
        }
        if (eligibleReplica) {
          Integer brokerId = maybeApplyBalancingAction(clusterModel, replica, Collections.singletonList(broker),
                                                       balancingAction, optimizedGoals);
          // Only need to check status if the action is taken. This will also handle the case that the source broker
          // has nothing to move in. In that case we will never reenqueue that source broker.
          if (brokerId != null) {
            if (isLoadAboveBalanceLowerLimitAfterChange(clusterModel, null, broker, ADD)) {
              return false;
            }
            // If the source broker has a lower utilization than the next broker in the eligible broker in the queue,
            // we reenqueue the source broker and switch to the next broker.
            if (!eligibleBrokers.isEmpty() &&
                utilizationPercentage(sourceBroker) < utilizationPercentage(eligibleBrokers.peek())) {
              eligibleBrokers.add(sourceBroker);
              break;
            }
          }
        }
      }
    }
    return true;
  }

  /**
   * Rebalance the broker by leader movement until it reaches given host and broker utilization limit.
   * @return true if the utilization is still over limit, false otherwise.
   * @throws ModelInputException
   * @throws AnalysisInputException
   */
  protected boolean rebalanceByMovingLeadersOut(Broker broker,
                                                ClusterModel clusterModel,
                                                Set<Goal> optimizedGoals)
      throws ModelInputException, AnalysisInputException {
    // Attempt to move leaders until the resource utilization on the broker is under the balance limit.
    // Leaders are sorted in by descending order of preference to relocate. Preference is based on utilization.
    for (Replica leader : broker.sortedReplicas(resource())) {
      // Get followers of this leader and sort them in ascending order by their broker resource utilization.
      List<Replica> followers = clusterModel.partition(leader.topicPartition()).followers();
      clusterModel.sortReplicasInAscendingOrderByBrokerResourceUtilization(followers, resource());
      List<Broker> eligibleBrokers = followers.stream().map(Replica::broker).collect(Collectors.toList());
      maybeApplyBalancingAction(clusterModel, leader, eligibleBrokers, BalancingAction.LEADERSHIP_MOVEMENT, optimizedGoals);

      boolean isUtilizationUnderLimit = isLoadUnderBalanceUpperLimitAfterChange(clusterModel, null, broker, REMOVE);
      if (isUtilizationUnderLimit) {
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
  protected boolean rebalanceByMovingReplicasOut(Broker broker,
                                                 ClusterModel clusterModel,
                                                 Set<Goal> optimizedGoals,
                                                 Set<String> excludedTopics)
      throws ModelInputException, AnalysisInputException {

    // Get and sort candidate destination brokers.
    List<Broker> candidateBrokers;
    if (_selfHealingDeadBrokersOnly) {
      candidateBrokers = new ArrayList<>(clusterModel.healthyBrokers());
      candidateBrokers.sort((b1, b2) -> Double.compare(b2.leadershipLoad().expectedUtilizationFor(Resource.NW_OUT),
                                                       b1.leadershipLoad().expectedUtilizationFor(Resource.NW_OUT)));
    } else {
      candidateBrokers = clusterModel.sortedHealthyBrokersUnderThreshold(resource(),
                                                                         balanceUpperThreshold(clusterModel));
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
      LOG.trace("Moving {} to {} in order to balance {}", replica, eligibleBrokers, resource());
      maybeApplyBalancingAction(clusterModel, replica, eligibleBrokers,
                                BalancingAction.REPLICA_MOVEMENT, optimizedGoals);
      boolean isUtilizationUnderLimit =
          isLoadUnderBalanceUpperLimitAfterChange(clusterModel, null, broker, REMOVE);
      if (isUtilizationUnderLimit) {
        return false;
      }
    }
    // If all the replicas has been moved away from the broker and we still reach here, that means the broker
    // capacity is negative, i.e. the broker is dead. So as long as there is no replicas on the broker anymore
    // we consider it as not over limit.
    return !broker.replicas().isEmpty();
  }

  private boolean isLoadAboveBalanceLowerLimitAfterChange(ClusterModel clusterModel,
                                                          Load load,
                                                          Broker broker,
                                                          ChangeType changeType) {
    double utilizationDelta = load == null ? 0 : load.expectedUtilizationFor(resource());

    double balanceLowerThreshold = balanceLowerThreshold(clusterModel);
    double brokerCapacity = broker.capacityFor(resource());
    double brokerBalanceLowerLimit = brokerCapacity * balanceLowerThreshold;
    double brokerUtilization = broker.load().expectedUtilizationFor(resource());
    boolean isBrokerAboveLowerLimit = changeType == ADD ? brokerUtilization + utilizationDelta >= brokerBalanceLowerLimit :
        brokerUtilization - utilizationDelta >= brokerBalanceLowerLimit;

    if (resource().isHostResource()) {
      double hostCapacity = broker.host().capacityFor(resource());
      double hostBalanceLowerLimit = hostCapacity * balanceLowerThreshold;
      double hostUtilization = broker.host().load().expectedUtilizationFor(resource());
      boolean isHostAboveLowerLimit = changeType == ADD ? hostUtilization + utilizationDelta >= hostBalanceLowerLimit :
          hostUtilization - utilizationDelta >= hostBalanceLowerLimit;
      // As long as either the host or the broker is above the limit, we claim the host resource utilization is
      // above the limit. If the host is below limit, there must be at least one broker below limit. We should just
      // bring more load to that broker.
      return isHostAboveLowerLimit || isBrokerAboveLowerLimit;
    } else {
      return isBrokerAboveLowerLimit;
    }
  }

  private boolean isLoadUnderBalanceUpperLimitAfterChange(ClusterModel clusterModel,
                                                          Load load,
                                                          Broker broker,
                                                          ChangeType changeType) {
    double utilizationDelta = load == null ? 0 : load.expectedUtilizationFor(resource());

    double balanceUpperThreshold = balanceUpperThreshold(clusterModel);
    double brokerCapacity = broker.capacityFor(resource());
    double brokerBalanceUpperLimit = brokerCapacity * balanceUpperThreshold;
    double brokerUtilization = broker.load().expectedUtilizationFor(resource());
    boolean isBrokerUnderUpperLimit = changeType == ADD ? brokerUtilization + utilizationDelta <= brokerBalanceUpperLimit :
        brokerUtilization - utilizationDelta <= brokerBalanceUpperLimit;

    if (resource().isHostResource()) {
      double hostCapacity = broker.host().capacityFor(resource());
      double hostBalanceUpperLimit = hostCapacity * balanceUpperThreshold;
      double hostUtilization = broker.host().load().expectedUtilizationFor(resource());
      boolean isHostUnderUpperLimit = changeType == ADD ? hostUtilization + utilizationDelta <= hostBalanceUpperLimit :
          hostUtilization - utilizationDelta <= hostBalanceUpperLimit;
      // As long as either the host or the broker is under the limit, we claim the host resource utilization is
      // under the limit. If the host is above limit, there must be at least one broker above limit. We should just
      // move load off that broker.
      return isHostUnderUpperLimit || isBrokerUnderUpperLimit;
    } else {
      return isBrokerUnderUpperLimit;
    }
  }

  /**
   * @param clusterModel the cluster topology and load.
   * @return the utilization upper threshold in percent for the {@link #resource()}
   */
  protected double balanceUpperThreshold(ClusterModel clusterModel) {
    return (clusterModel.load().expectedUtilizationFor(resource()) / clusterModel.capacityFor(resource()))
        * (1 + balancePercentageWithMargin(resource()));
  }

  /**
   * @param clusterModel the cluster topology and load.
   * @return the utilization lower threshold in percent for the {@link #resource()}
   */
  protected double balanceLowerThreshold(ClusterModel clusterModel) {
    return (clusterModel.load().expectedUtilizationFor(resource()) / clusterModel.capacityFor(resource()))
        * Math.max(0, (1 - balancePercentageWithMargin(resource())));
  }

  protected double utilizationPercentage(Broker broker) {
    return broker.isAlive() ? broker.load().expectedUtilizationFor(resource()) / broker.capacityFor(resource()) : 1;
  }

  protected double utilizationPercentage(Host host) {
    return host.isAlive() ? host.load().expectedUtilizationFor(resource()) / host.capacityFor(resource()) : 1;
  }

  /**
   * To avoid churns, we add a balance margin to the user specified rebalance threshold. e.g. when user sets the
   * threshold to be 1.1, we use 1.09 instead.
   * @return the rebalance threshold with a margin.
   */
  private double balancePercentageWithMargin(Resource resource) {
    return (_balancingConstraint.balancePercentage(resource) - 1) * BALANCE_MARGIN;
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

  /**
   * Whether bring load in or bring load out.
   */
  protected enum ChangeType {
    ADD, REMOVE
  }
}
