/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Load;
import com.linkedin.kafka.cruisecontrol.model.Replica;

import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;


/**
 * Class for achieving the following hard goal:
 * HARD GOAL: Generate leadership and replica movement proposals to push the load on brokers and/or hosts under the
 * capacity limit.
 */
public abstract class CapacityGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(CapacityGoal.class);

  /**
   * Constructor for Capacity Goal.
   */
  public CapacityGoal() {

  }

  /**
   * Package private for unit test.
   */
  CapacityGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
  }

  protected abstract Resource resource();

  /**
   * @deprecated
   * Please use {@link #actionAcceptance(BalancingAction, ClusterModel)} instead.
   */
  @Override
  public boolean isActionAcceptable(BalancingAction action, ClusterModel clusterModel) {
    return actionAcceptance(action, clusterModel) == ACCEPT;
  }

  /**
   * Check whether the given action is acceptable by this goal. An action is acceptable by a goal if it satisfies
   * requirements of the goal. Requirements(hard goal): Capacity.
   *
   * ## Leadership Movement: impacts only (1) network outbound and (2) CPU resources (See
   * {@link DiskCapacityGoal#actionAcceptance(BalancingAction, ClusterModel)} and
   * {@link NetworkInboundCapacityGoal#actionAcceptance(BalancingAction, ClusterModel)}).
   *   (1) Check if leadership NW_OUT movement is acceptable: NW_OUT movement carries all of leader's NW_OUT load.
   *   (2) Check if leadership CPU movement is acceptable: In reality, CPU movement carries only a fraction of
   * leader's CPU load.
   * To optimize CC performance, we avoid calculation of the expected leadership CPU utilization, and assume that
   * if (action.balancingAction() == ActionType.LEADERSHIP_MOVEMENT &amp;&amp; resource() == Resource.CPU),
   * then the expected leadership CPU utilization would be the full CPU utilization of the leader.
   * <p>
   * ## Replica Movement: impacts any resource.
   * ## Replica Swap: impacts any resource.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    Replica sourceReplica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());

    switch (action.balancingAction()) {
      case REPLICA_SWAP:
        Replica destinationReplica = destinationBroker.replica(action.destinationTopicPartition());
        return isSwapAcceptableForCapacity(sourceReplica, destinationReplica) ? ACCEPT : REPLICA_REJECT;
      case REPLICA_MOVEMENT:
      case LEADERSHIP_MOVEMENT:
        return isMovementAcceptableForCapacity(sourceReplica, destinationBroker) ? ACCEPT : REPLICA_REJECT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new CapGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    // We only need the latest snapshot and include all the topics.
    return new ModelCompletenessRequirements(1, _minMonitoredPartitionPercentage, true);
  }

  /**
   * Get the name of this goal. Name of a goal provides an identification for the goal in human readable format.
   */
  @Override
  public abstract String name();

  /**
   * This is a hard goal; hence, the proposals are not limited to dead broker replicas in case of self-healing.
   * Check if requirements of this goal are not violated if this action is applied to the given cluster state,
   * false otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model.
   * @return True if requirements of this goal are not violated if this action is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    Replica sourceReplica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    // To optimize CC performance, we avoid calculation of the expected leadership CPU utilization, and assume that
    // if (action.balancingAction() == ActionType.LEADERSHIP_MOVEMENT && resource() == Resource.CPU),
    // then the expected leadership CPU utilization would be the full CPU utilization of the leader.
    return isMovementAcceptableForCapacity(sourceReplica, destinationBroker);
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
   * Sanity checks: Existing total load on cluster is less than the limiting capacity
   * determined by the total capacity of healthy cluster multiplied by the capacity threshold.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposals.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, Set<String> excludedTopics) throws OptimizationFailureException {
    // Sanity Check -- i.e. not enough resources.
    Load recentClusterLoad = clusterModel.load();

    // While proposals exclude the excludedTopics, the existingUtilization still considers replicas of the excludedTopics.
    double existingUtilization = recentClusterLoad.expectedUtilizationFor(resource());
    double allowedCapacity = clusterModel.capacityFor(resource()) * _balancingConstraint.capacityThreshold(resource());

    if (allowedCapacity < existingUtilization) {
      throw new OptimizationFailureException("Insufficient healthy cluster capacity for resource:" + resource() +
          " existing cluster utilization " + existingUtilization + " allowed capacity " + allowedCapacity);
    }
    clusterModel.trackSortedReplicas(sortName(),
                                     ReplicaSortFunctionFactory.deprioritizeImmigrants(),
                                     ReplicaSortFunctionFactory.sortByMetricGroupValue(resource().name()));
    clusterModel.trackSortedReplicas(sortNameByLeader(),
                                     ReplicaSortFunctionFactory.selectLeaders(),
                                     ReplicaSortFunctionFactory.deprioritizeImmigrants(),
                                     ReplicaSortFunctionFactory.sortByMetricGroupValue(resource().name()));
  }

  /**
   * Update goal state.
   * Sanity check: After completion of balancing / self-healing the resource, confirm that the utilization is under
   * the capacity and finish.
   *
   * @param clusterModel The state of the cluster.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws OptimizationFailureException {
    // Ensure the resource utilization is under capacity limit.
    // While proposals exclude the excludedTopics, the utilization still considers replicas of the excludedTopics.
    ensureUtilizationUnderCapacity(clusterModel);
    // Sanity check: No self-healing eligible replica should remain at a decommissioned broker.
    AnalyzerUtils.ensureNoReplicaOnDeadBrokers(clusterModel);
    finish();
    clusterModel.untrackSortedReplicas(sortName());
    clusterModel.untrackSortedReplicas(sortNameByLeader());
  }

  /**
   * Ensure that for the resource, the utilization is under the capacity of the host/broker-level.
   * {@link Resource#_isBrokerResource} and {@link Resource#isHostResource()} determines the level of checks this
   * function performs.
   * @param clusterModel Cluster model.
   */
  private void ensureUtilizationUnderCapacity(ClusterModel clusterModel) throws OptimizationFailureException {
    Resource resource = resource();
    double capacityThreshold = _balancingConstraint.capacityThreshold(resource);

    for (Broker broker : clusterModel.brokers()) {
      // Host-level violation check.
      if (resource.isHostResource()) {
        double utilization = broker.host().load().expectedUtilizationFor(resource);
        double capacityLimit = broker.host().capacityFor(resource) * capacityThreshold;

        if (!broker.host().replicas().isEmpty() && utilization > capacityLimit) {
          // The utilization of the host for the resource is over the capacity limit.
          throw new OptimizationFailureException(String.format("Optimization for goal %s failed because %s utilization "
                                                               + "for host %s is %f which is above capacity limit %f.",
                                                               name(), resource, broker.host().name(), utilization, capacityLimit));
        }
      }
      // Broker-level violation check.
      if (resource.isBrokerResource()) {
        double utilization = broker.load().expectedUtilizationFor(resource);
        double capacityLimit = broker.capacityFor(resource) * capacityThreshold;

        if (!broker.replicas().isEmpty() && utilization > capacityLimit) {
          // The utilization of the broker for the resource is over the capacity limit.
          throw new OptimizationFailureException(String.format("Optimization for goal %s failed because %s utilization "
                                                               + "for broker %d is %f which is above capacity limit %f.",
                                                               name(), resource, broker.id(), utilization, capacityLimit));
        }
      }
    }
  }

  /**
   * (1) REBALANCE BY LEADERSHIP MOVEMENT:
   * Perform leadership movement to ensure that the load on brokers and/or hosts (see {@link Resource#isHostResource()}
   * and {@link Resource#isBrokerResource()}) for the outbound network load and CPU is under the capacity limit.
   *
   * <p>
   * (2) REBALANCE BY REPLICA MOVEMENT:
   * Perform optimization via replica movement for the given resource to ensure rebalance: The load on brokers and/or
   * hosts (see {@link Resource#isHostResource()} and {@link Resource#isBrokerResource()}) for the given resource is
   * under the capacity limit.
   *
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param excludedTopics The topics that should be excluded from the optimization action.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    Set<String> excludedTopics)
      throws OptimizationFailureException {
    LOG.debug("balancing broker {}, optimized goals = {}", broker, optimizedGoals);
    Resource currentResource = resource();
    double capacityThreshold = _balancingConstraint.capacityThreshold(currentResource);
    double brokerCapacityLimit = broker.capacityFor(currentResource) * capacityThreshold;
    double hostCapacityLimit = broker.host().capacityFor(currentResource) * capacityThreshold;

    boolean isUtilizationOverLimit =
        isUtilizationOverLimit(broker, currentResource, brokerCapacityLimit, hostCapacityLimit);
    if (!isUtilizationOverLimit) {
      // The utilization of source broker and/or host for the current resource is already under the capacity limit.
      return;
    }

    // First try REBALANCE BY LEADERSHIP MOVEMENT:
    if (currentResource == Resource.NW_OUT || currentResource == Resource.CPU) {
      // Sort replicas by descending order of preference to relocate. Preference is based on resource cost.
      // Only leaders in the source broker are sorted.
      List<Replica> sortedLeadersInSourceBroker =
          broker.trackedSortedReplicas(sortNameByLeader()).reverselySortedReplicas();
      for (Replica leader : sortedLeadersInSourceBroker) {
        if (shouldExclude(leader, excludedTopics)) {
          continue;
        }
        // Get followers of this leader and sort them in ascending order by their broker resource utilization.
        List<Replica> followers = clusterModel.partition(leader.topicPartition()).followers();
        clusterModel.sortReplicasInAscendingOrderByBrokerResourceUtilization(followers, currentResource);
        List<Broker> eligibleBrokers = followers.stream().map(Replica::broker).collect(Collectors.toList());

        Broker b = maybeApplyBalancingAction(clusterModel, leader, eligibleBrokers,
                                             ActionType.LEADERSHIP_MOVEMENT, optimizedGoals);
        if (b == null) {
          LOG.debug("Failed to move leader replica {} to any other brokers in {}", leader, eligibleBrokers);
        }
        isUtilizationOverLimit =
            isUtilizationOverLimit(broker, currentResource, brokerCapacityLimit, hostCapacityLimit);
        // Broker utilization has successfully been reduced under the capacity limit for the current resource.
        if (!isUtilizationOverLimit) {
          break;
        }
      }
    }

    // If leader movement did not work, move replicas.
    if (isUtilizationOverLimit) {
      // Get sorted healthy brokers under host and/or broker capacity limit (depending on the current resource).
      List<Broker> sortedHealthyBrokersUnderCapacityLimit =
          clusterModel.sortedHealthyBrokersUnderThreshold(currentResource, capacityThreshold);

      // Move replicas that are sorted in descending order of preference to relocate (preference is based on
      // utilization) until the source broker utilization gets under the capacity limit. If the capacity limit cannot
      // be satisfied, throw an exception.
      for (Replica replica : broker.trackedSortedReplicas(sortName()).reverselySortedReplicas()) {
        if (shouldExclude(replica, excludedTopics)) {
          continue;
        }
        // Unless the target broker would go over the host- and/or broker-level capacity,
        // the movement will be successful.
        Broker b = maybeApplyBalancingAction(clusterModel, replica, sortedHealthyBrokersUnderCapacityLimit,
                                             ActionType.REPLICA_MOVEMENT, optimizedGoals);
        if (b == null) {
          LOG.debug("Failed to move replica {} to any broker in {}", replica, sortedHealthyBrokersUnderCapacityLimit);
        }
        // If capacity limit was not satisfied before, check if it is satisfied now.
        isUtilizationOverLimit =
            isUtilizationOverLimit(broker, currentResource, brokerCapacityLimit, hostCapacityLimit);
        // Broker utilization has successfully been reduced under the capacity limit for the current resource.
        if (!isUtilizationOverLimit) {
          break;
        }
      }
    }

    if (isUtilizationOverLimit) {
      if (!currentResource.isHostResource()) {
        // Utilization is above the capacity limit after all replicas in the given source broker were checked.
        throw new OptimizationFailureException("Violated capacity limit of " + brokerCapacityLimit + " via broker "
            + "utilization of " + broker.load().expectedUtilizationFor(currentResource) + " with broker id "
            + broker.id() + " for resource " + currentResource);
      } else {
        throw new OptimizationFailureException("Violated capacity limit of " + hostCapacityLimit + " via host "
            + "utilization of " + broker.host().load().expectedUtilizationFor(currentResource) + " with hostname "
            + broker.host().name() + " for resource " + currentResource);
      }
    }
  }

  private String sortName() {
    return name() + "-" + resource().name() + "-ALL";
  }

  private String sortNameByLeader() {
    return name() + "-" + resource().name() + "-LEADER";
  }

  /**
   * Check whether the combined replica utilization for the given resource within the given (1) broker and (2) the
   * corresponding host are above the given capacity limits.
   * See {@link Resource#isHostResource()} and {@link Resource#isBrokerResource()} to determine whether host- and/or
   * broker-level capacity is relevant for the given resource.
   *
   * @param broker Broker to be checked for capacity limit violation.
   * @param resource Resource to be checked for capacity limit violation.
   * @param brokerCapacityLimit Capacity limit for the broker.
   * @param hostCapacityLimit Capacity limit for the host.
   * @return True if utilization is over the limit, false otherwise.
   */
  private boolean isUtilizationOverLimit(Broker broker,
                                         Resource resource,
                                         double brokerCapacityLimit,
                                         double hostCapacityLimit) {
    // Host-level violation check.
    if (!broker.host().replicas().isEmpty() && resource.isHostResource()) {
      double utilization = broker.host().load().expectedUtilizationFor(resource);
      if (utilization > hostCapacityLimit) {
        // The utilization of the host for the resource is over the capacity limit.
        return true;
      }
    }
    // Broker-level violation check.
    if (!broker.replicas().isEmpty() && resource.isBrokerResource()) {
      double utilization = broker.load().expectedUtilizationFor(resource);
      return utilization > brokerCapacityLimit;
    }
    return false;
  }

  /**
   * Check whether the movement of utilization for the current resource from the given source replica to given
   * destination broker is acceptable for this goal.
   *
   * @param sourceReplica     Source replica.
   * @param destinationBroker Destination broker.
   * @return True if movement of utilization for the given resource from the given source replica to given
   * destination broker is acceptable for this goal, false otherwise.
   */
  private boolean isMovementAcceptableForCapacity(Replica sourceReplica, Broker destinationBroker) {
    // The action is unacceptable if the movement of replica or leadership makes the utilization of the destination
    // broker (or destination host for a host-resource) go out of healthy capacity for the given resource.
    double replicaUtilization = sourceReplica.load().expectedUtilizationFor(resource());
    return isUtilizationUnderLimitAfterAddingLoad(destinationBroker, replicaUtilization);
  }

  /**
   * Check whether the swap for the current resource between source and destination replicas is acceptable for this goal.
   *
   * @param sourceReplica Source replica.
   * @param destinationReplica Destination replica.
   * @return True if the swap for the current resource between source and destination replicas is acceptable for this
   * goal, false otherwise
   */
  private boolean isSwapAcceptableForCapacity(Replica sourceReplica, Replica destinationReplica) {
    double sourceReplicaUtilization = sourceReplica.load().expectedUtilizationFor(resource());
    double destinationReplicaUtilization = destinationReplica.load().expectedUtilizationFor(resource());

    double sourceUtilizationDelta = destinationReplicaUtilization - sourceReplicaUtilization;
    return sourceUtilizationDelta > 0 ? isUtilizationUnderLimitAfterAddingLoad(sourceReplica.broker(), sourceUtilizationDelta)
                                      : isUtilizationUnderLimitAfterAddingLoad(destinationReplica.broker(),
                                                                                - sourceUtilizationDelta);
  }

  /**
   * Check whether the additional load on the destination makes the host (for host resources) or broker (for broker
   * resources) go out of the capacity limit.
   *
   * @param destinationBroker Destination broker.
   * @param replicaUtilization Replica utilization for the given resource.
   * @return True if utilization is equal or above the capacity limit, false otherwise.
   */
  private boolean isUtilizationUnderLimitAfterAddingLoad(Broker destinationBroker, double replicaUtilization) {
    Resource resource = resource();
    double capacityThreshold = _balancingConstraint.capacityThreshold(resource);

    // Host-level violation check.
    if (resource.isHostResource()) {
      double utilization = destinationBroker.host().load().expectedUtilizationFor(resource);
      double capacityLimit = destinationBroker.host().capacityFor(resource) * capacityThreshold;

      if (utilization + replicaUtilization >= capacityLimit) {
        // The utilization of the host for the resource is over the capacity limit.
        return false;
      }
    }
    // Broker-level violation check.
    if (resource.isBrokerResource()) {
      double utilization = destinationBroker.load().expectedUtilizationFor(resource);
      double capacityLimit = destinationBroker.capacityFor(resource) * capacityThreshold;

      return utilization + replicaUtilization < capacityLimit;
    }
    // Utilization would be under the limit after adding the load to the destination broker.
    return true;
  }

  private static class CapGoalStatsComparator implements ClusterModelStatsComparator {

    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // This goal does not care about stats. The optimization would have already failed if the goal is not met.
      return 0;
    }

    @Override
    public String explainLastComparison() {
      return null;
    }
  }
}
