/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Load;
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
 * Class for achieving the following hard goals:
 * HARD GOAL#1: Generate leadership and replica movement proposals to push the load on brokers under the capacity limit.
 * HARD GOAL#2: Generate replica movement proposals to provide rack-aware replica distribution.
 */
public class RackAwareCapacityGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(RackAwareCapacityGoal.class);
  private Set<Resource> _balancedResources;
  private Resource _currentResource;

  /**
   * Constructor for Rack Capacity Goal.
   */
  public RackAwareCapacityGoal() {

  }

  /**
   * Package private for unit test.
   */
  RackAwareCapacityGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
  }

  /**
   * Check whether given proposal is acceptable by this goal. A proposal is acceptable by a goal if it satisfies
   * requirements of the goal. Requirements(hard goal): (1) rack awareness and (2) capacity.
   *
   * @param proposal     Proposal to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return True if proposal is acceptable by this goal, false otherwise.
   */
  @Override
  public boolean isProposalAcceptable(BalancingProposal proposal, ClusterModel clusterModel) {
    Replica sourceReplica = clusterModel.broker(proposal.sourceBrokerId()).replica(proposal.topicPartition());
    Broker destinationBroker = clusterModel.broker(proposal.destinationBrokerId());

    if (proposal.balancingAction().equals(BalancingAction.LEADERSHIP_MOVEMENT)) {
      return isMovementAcceptableForCapacity(Resource.NW_OUT, sourceReplica, destinationBroker) &&
          isMovementAcceptableForCapacity(Resource.CPU, sourceReplica, destinationBroker);
    }
    // Replica Movement: impacts all resources.
    for (Resource resource : _balancingConstraint.resources()) {
      if (!isMovementAcceptableForCapacity(resource, sourceReplica, destinationBroker)) {
        return false;
      }
    }
    // Destination broker cannot be in a rack that violates rack awareness.
    Set<Broker> partitionBrokers = clusterModel.partition(sourceReplica.topicPartition()).partitionBrokers();
    partitionBrokers.remove(sourceReplica.broker());

    // Remove brokers in partition broker racks except the brokers in replica broker rack.
    for (Broker broker : partitionBrokers) {
      if (broker.rack().brokers().contains(destinationBroker)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new RackAwareCapacityGoalStatsComparator();
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
    return RackAwareCapacityGoal.class.getSimpleName();
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
    Replica sourceReplica = clusterModel.broker(proposal.sourceBrokerId()).replica(proposal.topicPartition());
    Broker destinationBroker = clusterModel.broker(proposal.destinationBrokerId());

    Set<Resource> resources;
    if (!clusterModel.selfHealingEligibleReplicas().isEmpty()) {
      // for self-healing the broker utilization should not go beyond any given resources.
      resources = new HashSet<>(_balancingConstraint.resources());
    } else {
      // Move must be acceptable for the current resource as well as balanced resources.
      resources = new HashSet<>(_balancedResources);
      resources.add(_currentResource);
    }
    // Check if movement is acceptable by this goal for the given resources.
    for (Resource resource : resources) {
      if (!isMovementAcceptableForCapacity(resource, sourceReplica, destinationBroker)) {
        return false;
      }
    }
    return true;
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
   * A. Sanity checks:
   * - Sanity Check #1: If this is a "self-healing" request, replicas on healthy brokers satisfy rack awareness.
   * - Sanity Check #2: For each resource, existing total load on cluster is less than the limiting capacity
   * determined by the total capacity of healthy cluster multiplied by the capacity threshold.
   * - Sanity Check #3: There exists sufficient number of racks for achieving rack-awareness.
   * <p>
   * B. Initialize the current resource to be balanced or self healed. resource selection is based on the order of
   * priority set in the _balancingConstraint.
   *
   * @param clusterModel The state of the cluster.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel)
      throws AnalysisInputException, ModelInputException {
    _currentResource = null;
    _balancedResources = new HashSet<>();
    // Run self-healing specific sanity check.
    if (!clusterModel.selfHealingEligibleReplicas().isEmpty()) {
      // Sanity Check #1 -- healthy brokers have multiple replicas from the same partition in the same rack.
      for (Replica leader : clusterModel.leaderReplicas()) {
        Set<String> replicaBrokersRackIds = new HashSet<>();
        Set<Broker> followerBrokers = new HashSet<>(clusterModel.partition(leader.topicPartition()).followerBrokers());

        // Check whether healthy brokers have multiple replicas from the same partition in the same rack.
        for (Broker followerBroker : followerBrokers) {
          if (followerBroker.isAlive() && !replicaBrokersRackIds.add(followerBroker.rack().id())) {
            throw new AnalysisInputException("Healthy brokers fail to satisfy rack-awareness.");
          }
        }
        if (leader.broker().isAlive() && !replicaBrokersRackIds.add(leader.broker().rack().id())) {
          throw new AnalysisInputException("Healthy brokers fail to satisfy rack-awareness.");
        }
      }
    }
    // Sanity Check #2. -- i.e. not enough resources.
    Load recentClusterLoad = clusterModel.load();
    for (Resource resource : _balancingConstraint.resources()) {
      double existingUtilization = recentClusterLoad.expectedUtilizationFor(resource);
      double allowedCapacity = clusterModel.capacityFor(resource) * _balancingConstraint.capacityThreshold(resource);

      if (allowedCapacity < existingUtilization) {
        throw new AnalysisInputException("Insufficient healthy cluster capacity for resource:" + resource +
            " existing cluster utilization " + existingUtilization + " allowed capacity " + allowedCapacity);
      }
    }
    // Sanity Check #3 -- i.e. not enough racks to satisfy rack awareness.
    if (clusterModel.maxReplicationFactor() > clusterModel.numHealthyRacks()) {
      throw new AnalysisInputException("Insufficient number of racks to distribute each replica over a rack.");
    }

    // Set the initial resource to heal or rebalance in the order of priority set in the _balancingConstraint.
    _currentResource = _balancingConstraint.resources().get(0);
  }

  /**
   * Update goal state.
   * (1) Sanity check: After completion of balancing / self-healing all resources, confirm that replicas of each
   * partition reside at a separate rack and finish.
   * (2) Update the current resource that is being balanced if there are still resources to be balanced.
   *
   * @param clusterModel The state of the cluster.
   * @throws AnalysisInputException
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel)
      throws AnalysisInputException, OptimizationFailureException {
    // Update balanced resources.
    _balancedResources.add(_currentResource);
    if (_balancedResources.size() == _balancingConstraint.resources().size()) {
      // Sanity check to confirm that the final distribution is rack aware.
      ensureRackAware(clusterModel);
      // Ensure the resource utilization is under capacity limit.
      ensureUtilizationUnderCapacity(clusterModel);
      // Sanity check: No self-healing eligible replica should remain at a decommissioned broker.
      AnalyzerUtils.ensureNoReplicaOnDeadBrokers(clusterModel);
      finish();
    } else {
      // Set the current resource to heal or rebalance in the order of priority set in the _balancingConstraint.
      _currentResource = _balancingConstraint.resources().get(_balancedResources.size());
    }
  }

  /**
   * (1) REBALANCE BY LEADERSHIP MOVEMENT:
   * Perform leadership movement to ensure that the load on brokers for the outbound network load is under the capacity
   * limit. (Leadership movement does not violate the rack awareness constraints.)
   * <p>
   * (2) REBALANCE BY REPLICA MOVEMENT:
   * Perform optimization via replica movement for the given resource (without breaking the balance for already
   * balanced resources) to ensure rebalance: The load on brokers for the given resource is under the capacity limit.
   *
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param excludedTopics The topics that should be excluded from the optimization proposal.
   * @throws AnalysisInputException
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    Set<String> excludedTopics)
      throws AnalysisInputException, ModelInputException {
    LOG.debug("balancing broker {}, optimized goals = {}", broker, optimizedGoals);
    double capacityThreshold = _balancingConstraint.capacityThreshold(_currentResource);
    double brokerCapacityLimit = broker.capacityFor(_currentResource) * capacityThreshold;
    double hostCapacityLimit = broker.host().capacityFor(_currentResource) * capacityThreshold;
    // 1. Satisfy rack awareness requirement.
    for (Replica replica : new ArrayList<>(broker.replicas())) {
      if (satisfiedRackAwareness(replica, clusterModel) || excludedTopics.contains(replica.topicPartition().topic())) {
        continue;
      }
      // Rack awareness is violated. Move replica to a broker in another rack.
      if (maybeApplyBalancingAction(clusterModel, replica, rackAwareEligibleBrokers(replica, clusterModel),
                                    BalancingAction.REPLICA_MOVEMENT, optimizedGoals) == null) {
        throw new AnalysisInputException(
            "Violated rack-awareness requirement for broker with id " + broker.id() + ".");
      }
    }

    boolean isUtilizationOverLimit =
        isUtilizationOverLimit(broker, _currentResource, brokerCapacityLimit, hostCapacityLimit);
    if (!isUtilizationOverLimit) {
      // The utilization of source broker for the current resource is already under the capacity limit.
      return;
    }

    // First try REBALANCE BY LEADERSHIP MOVEMENT:
    if (_currentResource.equals(Resource.NW_OUT)) {
      // Sort replicas in by descending order of preference to relocate. Preference is based on resource cost.
      List<Replica> sortedLeadersInSourceBroker = broker.sortedReplicas(_currentResource);
      for (Replica leader : sortedLeadersInSourceBroker) {
        if (excludedTopics.contains(leader.topicPartition().topic())) {
          continue;
        }
        // Get followers of this leader and sort them in ascending order by their broker resource utilization.
        List<Replica> followers = clusterModel.partition(leader.topicPartition()).followers();
        clusterModel.sortReplicasInAscendingOrderByBrokerResourceUtilization(followers, _currentResource);
        List<Broker> eligibleBrokers = followers.stream().map(Replica::broker).collect(Collectors.toList());

        Integer brokerId = maybeApplyBalancingAction(clusterModel, leader, eligibleBrokers, BalancingAction.LEADERSHIP_MOVEMENT,
                                                     optimizedGoals);
        if (brokerId == null) {
          LOG.debug("Failed to move leader replica {} to any other brokers in {}", leader, eligibleBrokers);
        }
        isUtilizationOverLimit =
            isUtilizationOverLimit(broker, _currentResource, brokerCapacityLimit, hostCapacityLimit);
        // Broker utilization has successfully been reduced under the capacity limit for the current resource.
        if (!isUtilizationOverLimit) {
          break;
        }
      }
    }

    // If leader movement did not work, move replicas.
    if (isUtilizationOverLimit) {
      // Get sorted healthy brokers under capacity limit.
      List<Broker> sortedHealthyBrokersUnderCapacityLimit =
          clusterModel.sortedHealthyBrokersUnderThreshold(_currentResource, capacityThreshold);

      // Move replicas that are sorted in descending order of preference to relocate (preference is based on
      // utilization) until the broker utilization is under the capacity limit. If the capacity limit cannot be
      // satisfied, throw an exception.
      for (Replica replica : broker.sortedReplicas(_currentResource)) {
        if (excludedTopics.contains(replica.topicPartition().topic())) {
          continue;
        }
        // Find eligible brokers that this replica is allowed to move. Unless the target broker would go over the
        // capacity the movement will be successful.
        List<Broker> eligibleBrokers = new ArrayList<>(
            removeBrokersViolatingRackAwareness(replica, sortedHealthyBrokersUnderCapacityLimit, clusterModel));

        Integer brokerId = maybeApplyBalancingAction(clusterModel, replica, eligibleBrokers, BalancingAction.REPLICA_MOVEMENT,
                                                     optimizedGoals);
        if (brokerId == null) {
          LOG.debug("Failed to move replica {} to any of the brokers in {}", replica, eligibleBrokers);
        }
        // If capacity limit was not satisfied before, check if it is satisfied now.
        isUtilizationOverLimit =
            isUtilizationOverLimit(broker, _currentResource, brokerCapacityLimit, hostCapacityLimit);
        // Broker utilization has successfully been reduced under the capacity limit for the current resource.
        if (!isUtilizationOverLimit) {
          break;
        }
      }
    }

    if (isUtilizationOverLimit) {
      if (_currentResource == Resource.DISK) {
        // Utilization is above the capacity limit after all replicas in the given source broker were checked.
        throw new AnalysisInputException("Violated capacity limit of " + brokerCapacityLimit + " via broker "
            + "utilization of " + broker.load().expectedUtilizationFor(_currentResource) + " with id "
            + broker.id() + " for resource " + _currentResource);
      } else {
        throw new AnalysisInputException("Violated capacity limit of " + hostCapacityLimit + " via host "
            + "utilization of " + broker.host().load().expectedUtilizationFor(_currentResource) + " with name "
            + broker.host().name() + " for resource " + _currentResource);
      }
    }
  }

  private void ensureRackAware(ClusterModel clusterModel) throws OptimizationFailureException {
    // Sanity check to confirm that the final distribution is rack aware.
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

  private void ensureUtilizationUnderCapacity(ClusterModel clusterModel) throws OptimizationFailureException {

    for (Resource resource : Resource.values()) {
      double capacityThreshold = _balancingConstraint.capacityThreshold(resource);
      for (Broker broker : clusterModel.brokers()) {
        double utilization = resource.isHostResource() ?
            broker.host().load().expectedUtilizationFor(resource) : broker.load().expectedUtilizationFor(resource);
        double capacity = resource.isHostResource() ? broker.host().capacityFor(resource) : broker.capacityFor(resource);
        double capacityLimit = capacity * capacityThreshold;
        if (!broker.replicas().isEmpty() && utilization > capacityLimit) {
          // The utilization of the broker for the resource is over the capacity limit.
          throw new OptimizationFailureException(String.format("Optimization for goal %s failed because %s utilization "
                                                                   + "for broker %d is %f which is above capacity "
                                                                   + "limit %f",
                                                               name(), resource, broker.id(), utilization, capacityLimit));
        }
      }
    }
  }

  private boolean isUtilizationOverLimit(Broker broker,
                                         Resource resource,
                                         double brokerCapacityLimit,
                                         double hostCapacityLimit) {
    if (broker.replicas().isEmpty()) {
      return false;
    }
    double brokerUtilization = broker.load().expectedUtilizationFor(resource);
    if (_currentResource == Resource.DISK) {
      return brokerUtilization > brokerCapacityLimit;
    } else {
      double hostUtilization = broker.host().load().expectedUtilizationFor(resource);
      return (brokerUtilization > brokerCapacityLimit) && hostUtilization > hostCapacityLimit;
    }
  }

  /**
   * Remove the set of brokers among candidate brokers that are violating rack awareness. A candidate broker violates
   * rack awareness if it is in the same rack with a broker which contains a replica within the partition set of the
   * given replica (except the given replica).
   *
   * @param replica          Replica for which the pruning algorithm will be used for.
   * @param candidateBrokers Candidate brokers.
   * @param clusterModel     The state of the cluster.
   * @return A subset of brokers not containing the ineligible brokers.
   */
  private List<Broker> removeBrokersViolatingRackAwareness(Replica replica,
                                                           List<Broker> candidateBrokers,
                                                           ClusterModel clusterModel) {
    Set<Broker> partitionBrokers = clusterModel.partition(replica.topicPartition()).partitionBrokers();
    partitionBrokers.remove(replica.broker());

    // Remove replica broker from candidate brokers.
    List<Broker> candidates = new ArrayList<>(candidateBrokers);
    candidates.remove(replica.broker());
    // Remove brokers in partition broker racks except the brokers in replica broker rack.
    for (Broker broker : partitionBrokers) {
      candidates.removeAll(broker.rack().brokers());
    }

    return candidates;
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

  /**
   * Check whether the movement of utilization for the given resource from the given source replica to given
   * destination broker is acceptable for this goal.
   *
   * @param resource          Resource for which the movement acceptance will be checked.
   * @param sourceReplica     Source replica.
   * @param destinationBroker Destination broker.
   * @return True if movement of utilization for the given resource from the given source replica to given
   * destination broker is acceptable for this goal, false otherwise.
   */
  private boolean isMovementAcceptableForCapacity(Resource resource, Replica sourceReplica, Broker destinationBroker) {
    // The proposal is unacceptable if the movement of replica or leadership makes the utilization of the
    // destination broker go out of healthy capacity for the given resource.
    double replicaUtilization = sourceReplica.load().expectedUtilizationFor(resource);
    return !isUtilizationAboveLimitAfterAddingLoad(resource, destinationBroker, replicaUtilization);
  }

  private boolean isUtilizationAboveLimit(Resource resource, Broker broker) {
    return isUtilizationAboveLimitAfterAddingLoad(resource, broker, 0);
  }

  private boolean isUtilizationAboveLimitAfterAddingLoad(Resource resource, Broker broker, double loadToAdd) {
    double capacityThreshold = _balancingConstraint.capacityThreshold(resource);
    double utilization = resource.isHostResource() ?
        broker.host().load().expectedUtilizationFor(resource) : broker.load().expectedUtilizationFor(resource);
    double capacity = resource.isHostResource() ? broker.host().capacityFor(resource) : broker.capacityFor(resource);
    double capacityLimit = capacity * capacityThreshold;
    return utilization + loadToAdd >= capacityLimit;
  }

  private static class RackAwareCapacityGoalStatsComparator implements ClusterModelStatsComparator {

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
