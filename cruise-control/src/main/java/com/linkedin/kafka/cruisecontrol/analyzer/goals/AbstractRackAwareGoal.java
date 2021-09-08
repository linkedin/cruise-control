/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.BROKER_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;


/**
 * An abstract class for custom rack aware goals.
 */
public abstract class AbstractRackAwareGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRackAwareGoal.class);

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new GoalUtils.HardGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
  }

  @Override
  public boolean isHardGoal() {
    return true;
  }

  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    return true;
  }

  /**
   * Check whether the given action is acceptable by this goal. The following actions are acceptable:
   * <ul>
   *   <li>All leadership moves</li>
   *   <li>Replica moves that do not violate {@link #doesReplicaMoveViolateActionAcceptance(ClusterModel, Replica, Broker)}</li>
   *   <li>Swaps that do not violate {@link #doesReplicaMoveViolateActionAcceptance(ClusterModel, Replica, Broker)}
   *   in both direction</li>
   * </ul>
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#BROKER_REJECT} if the action is rejected due to violating rack awareness in the destination
   * broker after moving source replica to destination broker, {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    switch (action.balancingAction()) {
      case LEADERSHIP_MOVEMENT:
        return ACCEPT;
      case INTER_BROKER_REPLICA_MOVEMENT:
      case INTER_BROKER_REPLICA_SWAP:
        if (doesReplicaMoveViolateActionAcceptance(clusterModel,
                                                   clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition()),
                                                   clusterModel.broker(action.destinationBrokerId()))) {
          return BROKER_REJECT;
        }

        if (action.balancingAction() == ActionType.INTER_BROKER_REPLICA_SWAP
            && doesReplicaMoveViolateActionAcceptance(clusterModel,
                                                      clusterModel.broker(action.destinationBrokerId()).replica(action.destinationTopicPartition()),
                                                      clusterModel.broker(action.sourceBrokerId()))) {
          return REPLICA_REJECT;
        }
        return ACCEPT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  /**
   * Check whether the given replica move would violate the action acceptance for this custom rack aware goal.
   *
   * @param clusterModel The state of the cluster.
   * @param sourceReplica Source replica
   * @param destinationBroker Destination broker to receive the given source replica.
   * @return {@code true} if the given replica move would violate action acceptance (i.e. the move is not acceptable),
   * {@code false} otherwise.
   */
  protected abstract boolean doesReplicaMoveViolateActionAcceptance(ClusterModel clusterModel,
                                                                    Replica sourceReplica,
                                                                    Broker destinationBroker);

  /**
   * Rebalance the given broker without violating the constraints of this custom rack aware goal and optimized goals.
   *
   * @param broker Broker to be balanced.
   * @param clusterModel The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param optimizationOptions Options to take into account during optimization.
   * @param throwExceptionIfCannotMove {@code true} to throw an {@link OptimizationFailureException} in case a required
   * balancing action for a replica fails for all rack-aware eligible brokers, {@code false} to just log the failure and return.
   * This parameter enables selected goals fail early in case the unsatisfiability of a goal can be determined early.
   */
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions,
                                    boolean throwExceptionIfCannotMove)
      throws OptimizationFailureException {
    for (Replica replica : broker.trackedSortedReplicas(replicaSortName(this, false, false)).sortedReplicas(true)) {
      if (broker.isAlive() && !broker.currentOfflineReplicas().contains(replica) && shouldKeepInTheCurrentBroker(replica, clusterModel)) {
        continue;
      }
      // The relevant rack awareness condition is violated. Move replica to an eligible broker
      SortedSet<Broker> eligibleBrokers = rackAwareEligibleBrokers(replica, clusterModel);
      if (maybeApplyBalancingAction(clusterModel, replica, eligibleBrokers,
                                    ActionType.INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions) == null) {
        if (throwExceptionIfCannotMove) {
          Set<String> partitionRackIds = clusterModel.partition(replica.topicPartition()).partitionBrokers()
                                                     .stream().map(partitionBroker -> partitionBroker.rack().id()).collect(Collectors.toSet());

          ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
              .numBrokers(1).excludedRackIds(partitionRackIds).build();
          throw new OptimizationFailureException(String.format("[%s] Cannot move %s to %s.", name(), replica, eligibleBrokers), recommendation);
        }
        LOG.debug("Cannot move replica {} to any broker in {}", replica, eligibleBrokers);
      }
    }
  }

  /**
   * Check whether the given alive replica should stay in the current broker or be moved to another broker to satisfy the
   * specific requirements of the custom rack aware goal in the given cluster state.
   *
   * @param replica An alive replica to check whether it should stay in the current broker.
   * @param clusterModel The state of the cluster.
   * @return {@code true} if the given alive replica should stay in the current broker, {@code false} otherwise.
   */
  protected abstract boolean shouldKeepInTheCurrentBroker(Replica replica, ClusterModel clusterModel);

  /**
   * Get a list of eligible brokers for moving the given replica in the given cluster to satisfy the specific
   * requirements of the custom rack aware goal.
   *
   * @param replica Replica for which a set of rack aware eligible brokers are requested.
   * @param clusterModel The state of the cluster.
   * @return A list of rack aware eligible brokers for the given replica in the given cluster.
   */
  protected abstract SortedSet<Broker> rackAwareEligibleBrokers(Replica replica, ClusterModel clusterModel);
}
