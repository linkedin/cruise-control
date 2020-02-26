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
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddBrokerParameters;
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
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.isKafkaAssignerMode;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.maybeStopOngoingExecutionToModifyAndWait;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.sanityCheckBrokersHavingOfflineReplicasOnBadDisks;


/**
 * The async runnable for broker addition.
 */
public class AddBrokersRunnable extends OperationRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(AddBrokersRunnable.class);
  protected final Set<Integer> _brokerIds;
  protected final boolean _dryRun;
  protected final boolean _throttleAddedBrokers;
  protected final List<String> _goals;
  protected final ModelCompletenessRequirements _modelCompletenessRequirements;
  protected final boolean _allowCapacityEstimation;
  protected final Integer _concurrentInterBrokerPartitionMovements;
  protected final Integer _concurrentLeaderMovements;
  protected final Long _executionProgressCheckIntervalMs;
  protected final boolean _skipHardGoalCheck;
  protected final Pattern _excludedTopics;
  protected final String _uuid;
  protected final String _reason;
  protected final boolean _excludeRecentlyDemotedBrokers;
  protected final boolean _excludeRecentlyRemovedBrokers;
  protected final ReplicaMovementStrategy _replicaMovementStrategy;
  protected final Long _replicationThrottle;
  protected final boolean _stopOngoingExecution;

  public AddBrokersRunnable(KafkaCruiseControl kafkaCruiseControl,
                            OperationFuture future,
                            AddBrokerParameters parameters,
                            String uuid) {
    super(kafkaCruiseControl, future);
    _brokerIds = parameters.brokerIds();
    _dryRun = parameters.dryRun();
    _throttleAddedBrokers = parameters.throttleAddedBrokers();
    _goals = parameters.goals();
    _modelCompletenessRequirements = parameters.modelCompletenessRequirements();
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _concurrentInterBrokerPartitionMovements = parameters.concurrentInterBrokerPartitionMovements();
    _concurrentLeaderMovements = parameters.concurrentLeaderMovements();
    _executionProgressCheckIntervalMs = parameters.executionProgressCheckIntervalMs();
    _skipHardGoalCheck = parameters.skipHardGoalCheck();
    _excludedTopics = parameters.excludedTopics();
    _replicaMovementStrategy = parameters.replicaMovementStrategy();
    _replicationThrottle = parameters.replicationThrottle();
    _uuid = uuid;
    _reason = parameters.reason();
    _excludeRecentlyDemotedBrokers = parameters.excludeRecentlyDemotedBrokers();
    _excludeRecentlyRemovedBrokers = parameters.excludeRecentlyRemovedBrokers();
    _stopOngoingExecution = parameters.stopOngoingExecution();
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(addBrokers(), _kafkaCruiseControl.config());
  }

  /**
   * Add brokers.
   *
   * @return The optimization result.
   *
   * @throws KafkaCruiseControlException When any exception occurred during the broker addition.
   */
  public OptimizerResult addBrokers() throws KafkaCruiseControlException {
    _kafkaCruiseControl.sanityCheckDryRun(_dryRun, _stopOngoingExecution);
    sanityCheckGoals(_goals, _skipHardGoalCheck, _kafkaCruiseControl.config());
    List<Goal> goalsByPriority = goalsByPriority(_goals, _kafkaCruiseControl.config());
    OperationProgress operationProgress = _future.operationProgress();
    if (goalsByPriority.isEmpty()) {
      throw new IllegalArgumentException("At least one goal must be provided to get an optimization result.");
    } else if (_stopOngoingExecution) {
      maybeStopOngoingExecutionToModifyAndWait(_kafkaCruiseControl, operationProgress);
    }
    ModelCompletenessRequirements modelCompletenessRequirements =
        _kafkaCruiseControl.modelCompletenessRequirements(goalsByPriority).weaker(_modelCompletenessRequirements);
    sanityCheckLoadMonitorReadiness(modelCompletenessRequirements, _kafkaCruiseControl.getLoadMonitorTaskRunnerState());
    try (AutoCloseable ignored = _kafkaCruiseControl.acquireForModelGeneration(operationProgress)) {
      _kafkaCruiseControl.sanityCheckBrokerPresence(_brokerIds);
      ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(modelCompletenessRequirements, _allowCapacityEstimation, operationProgress);
      sanityCheckBrokersHavingOfflineReplicasOnBadDisks(_goals, clusterModel);
      _brokerIds.forEach(id -> clusterModel.setBrokerState(id, Broker.State.NEW));

      if (!clusterModel.isClusterAlive()) {
        throw new IllegalArgumentException("All brokers are dead in the cluster.");
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
                                                                        false,
                                                                        Collections.emptySet(),
                                                                        false);

      OptimizerResult result = _kafkaCruiseControl.optimizations(clusterModel, goalsByPriority, operationProgress, null, optimizationOptions);
      if (!_dryRun) {
        _kafkaCruiseControl.executeProposals(result.goalProposals(),
                                             _throttleAddedBrokers ? Collections.emptySet() : _brokerIds,
                                             isKafkaAssignerMode(_goals),
                                             _concurrentInterBrokerPartitionMovements,
                                             null,
                                             _concurrentLeaderMovements,
                                             _executionProgressCheckIntervalMs,
                                             _replicaMovementStrategy,
                                             _replicationThrottle,
                                             true,
                                             _uuid,
                                             () -> _reason);
      }
      return result;
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
  }
}
