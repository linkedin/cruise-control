/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.sanityCheckBrokersHavingOfflineReplicasOnBadDisks;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;


/**
 * The async runnable for getting proposals.
 */
public class ProposalsRunnable extends GoalBasedOperationRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(ProposalsRunnable.class);
  protected final boolean _ignoreProposalCache;
  protected final Set<Integer> _destinationBrokerIds;
  protected final boolean _isRebalanceDiskMode;
  protected final boolean _isTriggeredByGoalViolation;
  // This runnable does not start or modify executions, skips hard goal check to evaluate any combination of goals, and
  // is not triggered by goal violation unless specified otherwise.
  protected static final boolean PROPOSALS_DRYRUN = true;
  protected static final boolean PROPOSALS_STOP_ONGOING_EXECUTION = false;
  protected static final boolean PROPOSALS_SKIP_HARD_GOAL_CHECK = true;
  protected static final boolean PROPOSALS_IS_TRIGGERED_BY_GOAL_VIOLATION = false;

  /**
   * Constructor to be used for creating a runnable for rebalance.
   */
  public ProposalsRunnable(KafkaCruiseControl kafkaCruiseControl,
                           OperationFuture future,
                           List<String> goals,
                           ModelCompletenessRequirements modelCompletenessRequirements,
                           boolean allowCapacityEstimation,
                           Pattern excludedTopics,
                           boolean excludeRecentlyDemotedBrokers,
                           boolean excludeRecentlyRemovedBrokers,
                           boolean ignoreProposalCache,
                           Set<Integer> destinationBrokerIds,
                           boolean isRebalanceDiskMode,
                           boolean skipHardGoalCheck,
                           boolean isTriggeredByGoalViolation) {
    super(kafkaCruiseControl, future, PROPOSALS_DRYRUN, goals, PROPOSALS_STOP_ONGOING_EXECUTION,
          modelCompletenessRequirements, skipHardGoalCheck, excludedTopics, allowCapacityEstimation,
          excludeRecentlyDemotedBrokers, excludeRecentlyRemovedBrokers);
    _ignoreProposalCache = ignoreProposalCache;
    _destinationBrokerIds = destinationBrokerIds;
    _isRebalanceDiskMode = isRebalanceDiskMode;
    _isTriggeredByGoalViolation = isTriggeredByGoalViolation;
  }

  public ProposalsRunnable(KafkaCruiseControl kafkaCruiseControl, OperationFuture future, ProposalsParameters parameters) {
    super(kafkaCruiseControl, future, parameters, PROPOSALS_DRYRUN, PROPOSALS_STOP_ONGOING_EXECUTION, PROPOSALS_SKIP_HARD_GOAL_CHECK);
    _ignoreProposalCache = parameters.ignoreProposalCache();
    _destinationBrokerIds = parameters.destinationBrokerIds();
    _isRebalanceDiskMode = parameters.isRebalanceDiskMode();
    _isTriggeredByGoalViolation = PROPOSALS_IS_TRIGGERED_BY_GOAL_VIOLATION;
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(computeResult(), _kafkaCruiseControl.config());
  }

  @Override
  protected OptimizerResult workWithClusterModel() throws KafkaCruiseControlException, TimeoutException, NotEnoughValidWindowsException {
    ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(DEFAULT_START_TIME_FOR_CLUSTER_MODEL,
                                                                 _kafkaCruiseControl.timeMs(),
                                                                 _combinedCompletenessRequirements,
                                                                 _isRebalanceDiskMode,
                                                                 _allowCapacityEstimation,
                                                                 _operationProgress);
    sanityCheckBrokersHavingOfflineReplicasOnBadDisks(_goals, clusterModel);
    if (!clusterModel.isClusterAlive()) {
      throw new IllegalArgumentException("All brokers are dead in the cluster.");
    }
    if (!_destinationBrokerIds.isEmpty()) {
      _kafkaCruiseControl.sanityCheckBrokerPresence(_destinationBrokerIds);
    }
    ExecutorState executorState = _kafkaCruiseControl.executorState();
    Set<Integer> excludedBrokersForLeadership = _excludeRecentlyDemotedBrokers ? executorState.recentlyDemotedBrokers()
                                                                               : Collections.emptySet();

    Set<Integer> excludedBrokersForReplicaMove = _excludeRecentlyRemovedBrokers ? executorState.recentlyRemovedBrokers()
                                                                                : Collections.emptySet();

    Set<String> excludedTopics = _kafkaCruiseControl.excludedTopics(clusterModel, _excludedTopics);
    LOG.debug("Topics excluded from partition movement: {}", excludedTopics);
    OptimizationOptions optimizationOptions = new OptimizationOptions(excludedTopics,
                                                                      excludedBrokersForLeadership,
                                                                      excludedBrokersForReplicaMove,
                                                                      _isTriggeredByGoalViolation,
                                                                      _destinationBrokerIds,
                                                                      false);

    return _kafkaCruiseControl.optimizations(clusterModel, _goalsByPriority, _operationProgress, null, optimizationOptions);
  }

  @Override
  protected OptimizerResult workWithoutClusterModel() throws KafkaCruiseControlException {
    return _kafkaCruiseControl.getProposals(_operationProgress, _allowCapacityEstimation);
  }

  @Override
  protected boolean shouldWorkWithClusterModel() {
    return _kafkaCruiseControl.ignoreProposalCache(_goals,
                                                   _combinedCompletenessRequirements,
                                                   _excludedTopics,
                                                   _excludeRecentlyDemotedBrokers || _excludeRecentlyRemovedBrokers,
                                                   _ignoreProposalCache,
                                                   _isTriggeredByGoalViolation,
                                                   _destinationBrokerIds,
                                                   _isRebalanceDiskMode);
  }
}
