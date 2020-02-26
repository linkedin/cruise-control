/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.goalsByPriority;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckGoals;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckLoadMonitorReadiness;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.sanityCheckBrokersHavingOfflineReplicasOnBadDisks;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;


/**
 * The async runnable for getting proposals.
 */
public class ProposalsRunnable extends OperationRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(ProposalsRunnable.class);
  protected final List<String> _goals;
  protected final ModelCompletenessRequirements _modelCompletenessRequirements;
  protected final boolean _allowCapacityEstimation;
  protected final Pattern _excludedTopics;
  protected final boolean _excludeRecentlyDemotedBrokers;
  protected final boolean _excludeRecentlyRemovedBrokers;
  protected final boolean _ignoreProposalCache;
  protected final Set<Integer> _destinationBrokerIds;
  protected final boolean _isRebalanceDiskMode;

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
                           boolean isRebalanceDiskMode) {
    super(kafkaCruiseControl, future);
    _goals = goals;
    _modelCompletenessRequirements = modelCompletenessRequirements;
    _allowCapacityEstimation = allowCapacityEstimation;
    _excludedTopics = excludedTopics;
    _excludeRecentlyDemotedBrokers = excludeRecentlyDemotedBrokers;
    _excludeRecentlyRemovedBrokers = excludeRecentlyRemovedBrokers;
    _ignoreProposalCache = ignoreProposalCache;
    _destinationBrokerIds = destinationBrokerIds;
    _isRebalanceDiskMode = isRebalanceDiskMode;
  }

  public ProposalsRunnable(KafkaCruiseControl kafkaCruiseControl, OperationFuture future, ProposalsParameters parameters) {
    super(kafkaCruiseControl, future);
    _goals = parameters.goals();
    _modelCompletenessRequirements = parameters.modelCompletenessRequirements();
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _excludedTopics = parameters.excludedTopics();
    _excludeRecentlyDemotedBrokers = parameters.excludeRecentlyDemotedBrokers();
    _excludeRecentlyRemovedBrokers = parameters.excludeRecentlyRemovedBrokers();
    _ignoreProposalCache = parameters.ignoreProposalCache();
    _destinationBrokerIds = parameters.destinationBrokerIds();
    _isRebalanceDiskMode = parameters.isRebalanceDiskMode();
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(getProposals(true, false), _kafkaCruiseControl.config());
  }

  /**
   * Get proposals to optimize a cluster model.
   *
   * @return The optimization result.
   * @throws KafkaCruiseControlException If anything goes wrong in optimization proposal calculation.
   */
  public OptimizerResult getProposals(boolean skipHardGoalCheck, boolean isTriggeredByGoalViolation) throws KafkaCruiseControlException {
    OptimizerResult result;
    sanityCheckGoals(_goals, skipHardGoalCheck, _kafkaCruiseControl.config());
    List<Goal> goalsByPriority = goalsByPriority(_goals, _kafkaCruiseControl.config());
    if (goalsByPriority.isEmpty()) {
      throw new IllegalArgumentException("At least one goal must be provided to get an optimization result.");
    }
    ModelCompletenessRequirements completenessRequirements = _kafkaCruiseControl.modelCompletenessRequirements(goalsByPriority)
                                                                                .weaker(_modelCompletenessRequirements);
    sanityCheckLoadMonitorReadiness(completenessRequirements, _kafkaCruiseControl.getLoadMonitorTaskRunnerState());
    boolean excludeBrokers = _excludeRecentlyDemotedBrokers || _excludeRecentlyRemovedBrokers;
    OperationProgress operationProgress = _future.operationProgress();
    if (_kafkaCruiseControl.ignoreProposalCache(_goals,
                                                completenessRequirements,
                                                _excludedTopics,
                                                excludeBrokers,
                                                _ignoreProposalCache,
                                                isTriggeredByGoalViolation,
                                                _destinationBrokerIds,
                                                _isRebalanceDiskMode)) {
      try (AutoCloseable ignored = _kafkaCruiseControl.acquireForModelGeneration(operationProgress)) {
        ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(DEFAULT_START_TIME_FOR_CLUSTER_MODEL,
                                                                     _kafkaCruiseControl.timeMs(),
                                                                     completenessRequirements,
                                                                     _isRebalanceDiskMode,
                                                                     _allowCapacityEstimation,
                                                                     operationProgress);
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
                                                                          isTriggeredByGoalViolation,
                                                                          _destinationBrokerIds,
                                                                          false);

        result = _kafkaCruiseControl.optimizations(clusterModel, goalsByPriority, operationProgress, null, optimizationOptions);
      } catch (KafkaCruiseControlException kcce) {
        throw kcce;
      } catch (Exception e) {
        throw new KafkaCruiseControlException(e);
      }
    } else {
      result = _kafkaCruiseControl.getProposals(operationProgress, _allowCapacityEstimation);
    }
    return result;
  }
}
