/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_DRYRUN;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_DESTINATION_BROKER_IDS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_EXCLUDED_TOPICS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_CONCURRENT_MOVEMENTS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_SKIP_HARD_GOAL_CHECK;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_MODEL_COMPLETENESS_REQUIREMENTS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_STOP_ONGOING_EXECUTION;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.isKafkaAssignerMode;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.maybeStopOngoingExecutionToModifyAndWait;


/**
 * The async runnable for balancing the cluster load.
 */
public class RebalanceRunnable extends OperationRunnable {
  public static final boolean SELF_HEALING_IGNORE_PROPOSAL_CACHE = false;
  public static final boolean SELF_HEALING_IS_REBALANCE_DISK_MODE = false;
  protected final List<String> _goals;
  protected final boolean _dryRun;
  protected final ModelCompletenessRequirements _modelCompletenessRequirements;
  protected final boolean _allowCapacityEstimation;
  protected final Integer _concurrentInterBrokerPartitionMovements;
  protected final Integer _concurrentIntraBrokerPartitionMovements;
  protected final Integer _concurrentLeaderMovements;
  protected final Long _executionProgressCheckIntervalMs;
  protected final boolean _skipHardGoalCheck;
  protected final Pattern _excludedTopics;
  protected final String _uuid;
  protected final Supplier<String> _reasonSupplier;
  protected final boolean _excludeRecentlyDemotedBrokers;
  protected final boolean _excludeRecentlyRemovedBrokers;
  protected final ReplicaMovementStrategy _replicaMovementStrategy;
  protected final Long _replicationThrottle;
  protected final boolean _ignoreProposalCache;
  protected final Set<Integer> _destinationBrokerIds;
  protected final boolean _isRebalanceDiskMode;
  protected final boolean _stopOngoingExecution;
  protected final boolean _isTriggeredByGoalViolation;

  /**
   * Constructor to be used for creating a runnable for self-healing.
   */
  public RebalanceRunnable(KafkaCruiseControl kafkaCruiseControl,
                           List<String> selfHealingGoals,
                           boolean allowCapacityEstimation,
                           boolean excludeRecentlyDemotedBrokers,
                           boolean excludeRecentlyRemovedBrokers,
                           String anomalyId,
                           Supplier<String> reasonSupplier) {
    super(kafkaCruiseControl, new OperationFuture("Goal Violation Self-Healing"));
    _goals = selfHealingGoals;
    _dryRun = SELF_HEALING_DRYRUN;
    _modelCompletenessRequirements = SELF_HEALING_MODEL_COMPLETENESS_REQUIREMENTS;
    _allowCapacityEstimation = allowCapacityEstimation;
    _concurrentInterBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _concurrentIntraBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _concurrentLeaderMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _executionProgressCheckIntervalMs = SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
    _skipHardGoalCheck = SELF_HEALING_SKIP_HARD_GOAL_CHECK;
    _excludedTopics = SELF_HEALING_EXCLUDED_TOPICS;
    _replicaMovementStrategy = SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
    _replicationThrottle = kafkaCruiseControl.config().getLong(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG);
    _uuid = anomalyId;
    _reasonSupplier = reasonSupplier;
    _excludeRecentlyDemotedBrokers = excludeRecentlyDemotedBrokers;
    _excludeRecentlyRemovedBrokers = excludeRecentlyRemovedBrokers;
    _ignoreProposalCache = SELF_HEALING_IGNORE_PROPOSAL_CACHE;
    _destinationBrokerIds = SELF_HEALING_DESTINATION_BROKER_IDS;
    _isRebalanceDiskMode = SELF_HEALING_IS_REBALANCE_DISK_MODE;
    _stopOngoingExecution = SELF_HEALING_STOP_ONGOING_EXECUTION;
    _isTriggeredByGoalViolation = true;
  }

  public RebalanceRunnable(KafkaCruiseControl kafkaCruiseControl,
                           OperationFuture future,
                           RebalanceParameters parameters,
                           String uuid) {
    super(kafkaCruiseControl, future);
    _goals = parameters.goals();
    _dryRun = parameters.dryRun();
    _modelCompletenessRequirements = parameters.modelCompletenessRequirements();
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _concurrentInterBrokerPartitionMovements = parameters.concurrentInterBrokerPartitionMovements();
    _concurrentIntraBrokerPartitionMovements = parameters.concurrentIntraBrokerPartitionMovements();
    _concurrentLeaderMovements = parameters.concurrentLeaderMovements();
    _executionProgressCheckIntervalMs = parameters.executionProgressCheckIntervalMs();
    _skipHardGoalCheck = parameters.skipHardGoalCheck();
    _excludedTopics = parameters.excludedTopics();
    _replicaMovementStrategy = parameters.replicaMovementStrategy();
    _replicationThrottle = parameters.replicationThrottle();
    _uuid = uuid;
    String reason = parameters.reason();
    _reasonSupplier = () -> reason;
    _excludeRecentlyDemotedBrokers = parameters.excludeRecentlyDemotedBrokers();
    _excludeRecentlyRemovedBrokers = parameters.excludeRecentlyRemovedBrokers();
    _ignoreProposalCache = parameters.ignoreProposalCache();
    _destinationBrokerIds = parameters.destinationBrokerIds();
    _isRebalanceDiskMode =  parameters.isRebalanceDiskMode();
    _stopOngoingExecution = parameters.stopOngoingExecution();
    _isTriggeredByGoalViolation = false;
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(rebalance(), _kafkaCruiseControl.config());
  }

  /**
   * Rebalance the cluster.
   *
   * @return The optimization result.
   *
   * @throws KafkaCruiseControlException When any exception occurred during the rebalance process.
   */
  public OptimizerResult rebalance() throws KafkaCruiseControlException {
    _kafkaCruiseControl.sanityCheckDryRun(_dryRun, _stopOngoingExecution);
    if (_stopOngoingExecution) {
      maybeStopOngoingExecutionToModifyAndWait(_kafkaCruiseControl, _future.operationProgress());
    }
    ProposalsRunnable proposalsRunnable = new ProposalsRunnable(_kafkaCruiseControl, _future, _goals, _modelCompletenessRequirements,
                                                                 _allowCapacityEstimation, _excludedTopics, _excludeRecentlyDemotedBrokers,
                                                                 _excludeRecentlyRemovedBrokers, _ignoreProposalCache, _destinationBrokerIds,
                                                                 _isRebalanceDiskMode);
    OptimizerResult result = proposalsRunnable.getProposals(_skipHardGoalCheck, _isTriggeredByGoalViolation);
    if (!_dryRun) {
      _kafkaCruiseControl.executeProposals(result.goalProposals(), Collections.emptySet(), isKafkaAssignerMode(_goals),
                                           _concurrentInterBrokerPartitionMovements, _concurrentIntraBrokerPartitionMovements,
                                           _concurrentLeaderMovements, _executionProgressCheckIntervalMs, _replicaMovementStrategy,
                                           _replicationThrottle, !_isTriggeredByGoalViolation, _uuid, _reasonSupplier);
    }
    return result;
  }
}
