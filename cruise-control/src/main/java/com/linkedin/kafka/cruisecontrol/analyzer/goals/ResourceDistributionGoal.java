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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal.ChangeType.ADD;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal.ChangeType.REMOVE;
import static com.linkedin.kafka.cruisecontrol.common.BalancingAction.LEADERSHIP_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.common.BalancingAction.REPLICA_MOVEMENT;


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
  protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
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
   * @param excludedTopics The topics that should be excluded from the optimization proposals.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws AnalysisInputException, ModelInputException {
    _selfHealingDeadBrokersOnly = false;
  }

  /**
   * Update the current resource that is being balanced if there are still resources to be balanced, finish otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @throws AnalysisInputException
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics) throws AnalysisInputException {
    Set<Integer> brokerIdsAboveBalanceUpperLimit = new HashSet<>();
    Set<Integer> brokerIdsUnderBalanceLowerLimit = new HashSet<>();
    // Log broker Ids over balancing limit.
    // While proposals exclude the excludedTopics, the balance still considers utilization of the excludedTopic replicas.
    for (Broker broker : clusterModel.healthyBrokers()) {
      if (!isLoadUnderBalanceUpperLimit(clusterModel, broker)) {
        brokerIdsAboveBalanceUpperLimit.add(broker.id());
      }
      if (!isLoadAboveBalanceLowerLimit(clusterModel, broker)) {
        brokerIdsUnderBalanceLowerLimit.add(broker.id());
      }
    }
    if (!brokerIdsAboveBalanceUpperLimit.isEmpty()) {
      LOG.warn("Utilization for broker ids:{} {} above the balance limit for:{} after {}.",
               brokerIdsAboveBalanceUpperLimit, (brokerIdsAboveBalanceUpperLimit.size() > 1) ? "are" : "is", resource(),
               (clusterModel.selfHealingEligibleReplicas().isEmpty()) ? "rebalance" : "self-healing");
      _succeeded = false;
    }
    if (!brokerIdsUnderBalanceLowerLimit.isEmpty()) {
      LOG.warn("Utilization for broker ids:{} {} under the balance limit for:{} after {}.",
               brokerIdsUnderBalanceLowerLimit, (brokerIdsUnderBalanceLowerLimit.size() > 1) ? "are" : "is", resource(),
               (clusterModel.selfHealingEligibleReplicas().isEmpty()) ? "rebalance" : "self-healing");
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
    boolean requireLessLoad = !isLoadUnderBalanceUpperLimit(clusterModel, broker);
    boolean requireMoreLoad = !isLoadAboveBalanceLowerLimit(clusterModel, broker);
    if (broker.isAlive() && !requireMoreLoad && !requireLessLoad) {
      // return if the broker is already under limit.
      return;
    } else if (!clusterModel.newBrokers().isEmpty() && requireMoreLoad && !broker.isNew()) {
      // return if we have new broker and the current broker is not a new broker but require more load.
      return;
    } else if (!clusterModel.deadBrokers().isEmpty() && requireLessLoad && broker.isAlive()
        && broker.immigrantReplicas().isEmpty()) {
      // return if the cluster is in self-healing mode and the broker requires less load but does not have any
      // immigrant replicas.
      return;
    }

    // First try leadership movement
    if (resource() == Resource.NW_OUT || resource() == Resource.CPU) {
      if (requireLessLoad && !rebalanceByMovingLoadOut(broker, clusterModel, optimizedGoals,
                                                       LEADERSHIP_MOVEMENT, excludedTopics)) {
        LOG.debug("Successfully balanced {} for broker {} by moving out leaders.", resource(), broker.id());
        return;
      } else if (requireMoreLoad && !rebalanceByMovingLoadIn(broker, clusterModel, optimizedGoals,
                                                             LEADERSHIP_MOVEMENT, excludedTopics)) {
        LOG.debug("Successfully balanced {} for broker {} by moving in leaders.", resource(), broker.id());
        return;
      }
    }

    // Update broker ids over the balance limit for logging purposes.
    boolean unbalanced = false;
    if (requireLessLoad) {
      unbalanced = rebalanceByMovingLoadOut(broker, clusterModel, optimizedGoals, REPLICA_MOVEMENT, excludedTopics);
    } else if (requireMoreLoad) {
      unbalanced = rebalanceByMovingLoadIn(broker, clusterModel, optimizedGoals, REPLICA_MOVEMENT, excludedTopics);
    }

    if (!unbalanced) {
      LOG.debug("Successfully balanced {} for broker {} by moving leaders and replicas.", resource(), broker.id());
    }

  }

  protected boolean rebalanceByMovingLoadIn(Broker broker,
                                            ClusterModel clusterModel,
                                            Set<Goal> optimizedGoals,
                                            BalancingAction balancingAction,
                                            Set<String> excludedTopics)
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

    // Stop when all the replicas are leaders for leader movement or there is no replicas can be moved in anymore
    // for replica movement.
    while (!eligibleBrokers.isEmpty() && (balancingAction == REPLICA_MOVEMENT ||
        (balancingAction == LEADERSHIP_MOVEMENT && broker.leaderReplicas().size() != broker.replicas().size()))) {
      Broker sourceBroker = eligibleBrokers.poll();
      for (Replica replica : sourceBroker.sortedReplicas(resource())) {
        if (shouldExclude(replica, excludedTopics)) {
          continue;
        }
        // It does not make sense to move a replica without utilization from a live broker.
        if (replica.load().expectedUtilizationFor(resource()) == 0.0 && broker.isAlive()) {
          break;
        }
        Broker b = maybeApplyBalancingAction(clusterModel, replica, Collections.singletonList(broker),
                                                     balancingAction, optimizedGoals);
        // Only need to check status if the action is taken. This will also handle the case that the source broker
        // has nothing to move in. In that case we will never reenqueue that source broker.
        if (b != null) {
          if (isLoadAboveBalanceLowerLimit(clusterModel, broker)) {
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
    return true;
  }

  protected boolean rebalanceByMovingLoadOut(Broker broker,
                                             ClusterModel clusterModel,
                                             Set<Goal> optimizedGoals,
                                             BalancingAction balancingAction,
                                             Set<String> excludedTopics)
      throws AnalysisInputException, ModelInputException {
    // Get th eligible brokers.
    SortedSet<Broker> candidateBrokers = new TreeSet<>((b1, b2) -> {
        int result = Double.compare(utilizationPercentage(b1), utilizationPercentage(b2));
        return result != 0 ? result : Integer.compare(b1.id(), b2.id());
    });
    double balancingUpperThreshold = balanceUpperThreshold(clusterModel);
    if (_selfHealingDeadBrokersOnly) {
      candidateBrokers.addAll(clusterModel.healthyBrokers());
    } else {
      candidateBrokers.addAll(clusterModel.sortedHealthyBrokersUnderThreshold(resource(), balancingUpperThreshold));
    }

    // Get the replicas to rebalance.
    List<Replica> replicasToMove;
    if (balancingAction == LEADERSHIP_MOVEMENT) {
      // Only take leader replicas to move leaders.
      replicasToMove = new ArrayList<>(broker.leaderReplicas());
      replicasToMove.sort((r1, r2) -> Double.compare(r2.load().expectedUtilizationFor(resource()),
                                                     r1.load().expectedUtilizationFor(resource())));
    } else {
      // Take all replicas for replica movements.
      replicasToMove = broker.sortedReplicas(resource());
    }

    // Now let's move things around.
    for (Replica replica : replicasToMove) {
      if (shouldExclude(replica, excludedTopics)) {
        continue;
      }
      // It does not make sense to move a replica without utilization from a live broker.
      if (replica.load().expectedUtilizationFor(resource()) == 0.0 && broker.isAlive()) {
        break;
      }

      // An optimization for leader movements.
      SortedSet<Broker> eligibleBrokers;
      if (balancingAction == LEADERSHIP_MOVEMENT) {
        eligibleBrokers = new TreeSet<>((b1, b2) -> {
          int result = Double.compare(utilizationPercentage(b1), utilizationPercentage(b2));
          return result != 0 ? result : Integer.compare(b1.id(), b2.id());
        });
        clusterModel.partition(replica.topicPartition()).followerBrokers().forEach(b -> {
          if (candidateBrokers.contains(b)) {
            eligibleBrokers.add(b);
          }
        });
      } else {
        eligibleBrokers = candidateBrokers;
      }

      Broker b = maybeApplyBalancingAction(clusterModel, replica, eligibleBrokers, balancingAction, optimizedGoals);
      // Only check if we successfully moved something.
      if (b != null) {
        if (isLoadUnderBalanceUpperLimit(clusterModel, broker)) {
          return false;
        }
        // Remove and reinsert the broker so the order is correct.
        candidateBrokers.remove(b);
        if (utilizationPercentage(b) < balancingUpperThreshold) {
          candidateBrokers.add(b);
        }
      }
    }
    // If all the replicas has been moved away from the broker and we still reach here, that means the broker
    // capacity is negative, i.e. the broker is dead. So as long as there is no replicas on the broker anymore
    // we consider it as not over limit.
    return !broker.replicas().isEmpty();
  }

  private boolean isLoadAboveBalanceLowerLimit(ClusterModel clusterModel, Broker broker) {
    // The action does not matter here because the load is null.
    return isLoadAboveBalanceLowerLimitAfterChange(clusterModel, null, broker, ADD);
  }

  private boolean isLoadUnderBalanceUpperLimit(ClusterModel clusterModel, Broker broker) {
    // The action does not matter here because the load is null.
    return isLoadUnderBalanceUpperLimitAfterChange(clusterModel, null, broker, REMOVE);
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
   * threshold to be balancePercentage, we use (balancePercentage-1)*balanceMargin instead.
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
