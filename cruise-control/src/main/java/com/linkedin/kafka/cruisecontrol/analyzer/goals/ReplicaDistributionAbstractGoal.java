/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;

/**
 * An abstract class for replica distribution goals. This class will be extended to create custom goals to balance replicas
 * of different categories (i.e. all replicas, leader replicas only) in the cluster.
 */
public abstract class ReplicaDistributionAbstractGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaDistributionAbstractGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  // Flag to indicate whether the self healing failed to relocate all replicas away from dead brokers in its initial
  // attempt and currently omitting the replica balance limit to relocate remaining replicas.
  protected boolean _selfHealingDeadBrokersOnly;
  protected final Set<Integer> _brokerIdsAboveBalanceUpperLimit;
  protected final Set<Integer> _brokerIdsUnderBalanceLowerLimit;
  protected double _avgReplicasOnAliveBroker;
  protected int _balanceUpperLimit;
  protected int _balanceLowerLimit;

  /**
   * Constructor for Replica Distribution Abstract Goal.
   */
  public ReplicaDistributionAbstractGoal() {
    _brokerIdsAboveBalanceUpperLimit = new HashSet<>();
    _brokerIdsUnderBalanceLowerLimit = new HashSet<>();
  }

  /**
   * Apply several adjustments to the requested rebalance threshold to be used by goal optimization.
   * <ol>
   *   <li>If the goal optimization is triggered by goal violation detector, increase threshold by multiplying with
   *       {@link com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig#GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG}</li>
   *   <li>Add a balance margin to avoid churn, e.g. if the threshold with prior adjustment applied is balancePercentage,
   *       we use (balancePercentage-1)*{@link #BALANCE_MARGIN} instead.</li>
   * </ol>
   *
   * @param optimizationOptions Options to adjust balance percentage with margin in case goal optimization is triggered
   *                            by goal violation detector.
   * @param balancePercentage The requested balance threshold.
   * @return the adjusted rebalance threshold.
   */
  private double adjustedBalancePercentage(OptimizationOptions optimizationOptions, double balancePercentage) {
    double adjustedBalancePercentage = optimizationOptions.isTriggeredByGoalViolation()
                                       ? balancePercentage
                                       * _balancingConstraint.goalViolationDistributionThresholdMultiplier()
                                       : balancePercentage;

    return (adjustedBalancePercentage - 1) * BALANCE_MARGIN;
  }

  /**
   * @param optimizationOptions Options to adjust balance upper limit in case goal optimization is triggered by goal
   *                            violation detector.
   * @param balancePercentage The requested balance threshold.
   * @return The replica balance upper threshold in number of replicas.
   */
  private int balanceUpperLimit(OptimizationOptions optimizationOptions, double balancePercentage) {
    return (int) Math.ceil(_avgReplicasOnAliveBroker * (1 + adjustedBalancePercentage(optimizationOptions, balancePercentage)));
  }

  /**
   * @param optimizationOptions Options to adjust balance lower limit in case goal optimization is triggered by goal
   *                            violation detector.
   * @param balancePercentage The requested balance threshold.
   * @return The replica balance lower threshold in number of replicas.
   */
  private int balanceLowerLimit(OptimizationOptions optimizationOptions, double balancePercentage) {
    return (int) Math.floor(_avgReplicasOnAliveBroker * Math.max(0, (1 - adjustedBalancePercentage(optimizationOptions, balancePercentage))));
  }

  boolean isReplicaCountUnderBalanceUpperLimitAfterChange(Broker broker, int currentReplicaCount, ChangeType changeType) {
    int brokerBalanceUpperLimit = broker.isAlive() ? _balanceUpperLimit : 0;

    return changeType == ChangeType.ADD ? currentReplicaCount + 1 <= brokerBalanceUpperLimit
                                        : currentReplicaCount - 1 <= brokerBalanceUpperLimit;
  }

  boolean isReplicaCountAboveBalanceLowerLimitAfterChange(Broker broker, int currentReplicaCount, ChangeType changeType) {
    int brokerBalanceLowerLimit = broker.isAlive() ? _balanceLowerLimit : 0;

    return changeType == ChangeType.ADD ? currentReplicaCount + 1 >= brokerBalanceLowerLimit
                                        : currentReplicaCount - 1 >= brokerBalanceLowerLimit;
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
  }

  @Override
  public boolean isHardGoal() {
    return false;
  }

  /**
   * Get brokers that the rebalance process will go over to apply balancing actions to replicas they contain.
   *
   * @param clusterModel The state of the cluster.
   * @return A collection of brokers that the rebalance process will go over to apply balancing actions to replicas
   *         they contain.
   */
  @Override
  protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
    return clusterModel.brokers();
  }

  /**
   * Initiates replica distribution abstract goal.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    // Initialize the average replicas on an alive broker.
    _avgReplicasOnAliveBroker = numInterestedReplicas(clusterModel) / (double) clusterModel.aliveBrokers().size();

    // Log a warning if all replicas are excluded.
    if (clusterModel.topics().equals(optimizationOptions.excludedTopics())) {
      LOG.warn("All replicas are excluded from {}.", name());
    }

    _selfHealingDeadBrokersOnly = false;
    _balanceUpperLimit = balanceUpperLimit(optimizationOptions, balancePercentage());
    _balanceLowerLimit = balanceLowerLimit(optimizationOptions, balancePercentage());
  }

  /**
   * Count of replicas of interest in the cluster, use to calculate balance upper/lower limit.
   */
  abstract int numInterestedReplicas(ClusterModel clusterModel);

  /**
   * The requested balance threshold.
   */
  abstract double balancePercentage();

  /**
   * Check if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model.
   * @return True if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   *         false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    // If the source broker is dead and currently self healing dead brokers only, then the proposal must be executed.
    if (!sourceBroker.isAlive() && _selfHealingDeadBrokersOnly) {
      return true;
    }

    //Check that destination and source would not become unbalanced.
    return actionAcceptance(action, clusterModel) == ACCEPT;
  }

  /**
   * Update goal state after one round of self-healing / rebalance.
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposal.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws OptimizationFailureException {
    // Log broker Ids over balancing limit.
    // While proposals exclude the excludedTopics, the balance still considers utilization of the excludedTopic replicas.
    if (!_brokerIdsAboveBalanceUpperLimit.isEmpty()) {
      LOG.debug("Replicas count on broker ids:{} {} above the balance limit of {} after {}.",
                _brokerIdsAboveBalanceUpperLimit, (_brokerIdsAboveBalanceUpperLimit.size() > 1) ? "are" : "is",
                _balanceUpperLimit,
                (clusterModel.selfHealingEligibleReplicas().isEmpty()) ? "rebalance" : "self-healing");
      _brokerIdsAboveBalanceUpperLimit.clear();
      _succeeded = false;
    }
    if (!_brokerIdsUnderBalanceLowerLimit.isEmpty()) {
      LOG.debug("Replica count on broker ids:{} {} under the balance limit of {} after {}.",
                _brokerIdsUnderBalanceLowerLimit, (_brokerIdsUnderBalanceLowerLimit.size() > 1) ? "are" : "is",
                _balanceLowerLimit,
                (clusterModel.selfHealingEligibleReplicas().isEmpty()) ? "rebalance" : "self-healing");
      _brokerIdsUnderBalanceLowerLimit.clear();
      _succeeded = false;
    }
    // Sanity check: No self-healing eligible replica should remain at a decommissioned broker.
    try {
      GoalUtils.ensureNoReplicaOnDeadBrokers(clusterModel, name());
    } catch (OptimizationFailureException ofe) {
      if (_selfHealingDeadBrokersOnly) {
        throw ofe;
      }
      _selfHealingDeadBrokersOnly = true;
      LOG.info("Ignoring replica balance limit to move replicas from dead brokers to healthy ones.");
      return;
    }

    finish();
  }

  /**
   * Whether bring replica in or out.
   */
  protected enum ChangeType {
    ADD, REMOVE
  }
}
