/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.FixOfflineReplicasParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.goalsByPriority;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckGoals;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckLoadMonitorReadiness;
import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_DRYRUN;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_EXCLUDED_TOPICS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_CONCURRENT_MOVEMENTS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_SKIP_HARD_GOAL_CHECK;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_MODEL_COMPLETENESS_REQUIREMENTS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_STOP_ONGOING_EXECUTION;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.maybeStopOngoingExecutionToModifyAndWait;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.sanityCheckOfflineReplicaPresence;


/**
 * The async runnable for fixing offline replicas.
 */
public class FixOfflineReplicasRunnable extends OperationRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(FixOfflineReplicasRunnable.class);
  protected final boolean _dryRun;
  protected final List<String> _goals;
  protected final ModelCompletenessRequirements _modelCompletenessRequirements;
  protected final boolean _allowCapacityEstimation;
  protected final Integer _concurrentInterBrokerPartitionMovements;
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
  protected final boolean _stopOngoingExecution;
  protected final boolean _isTriggeredByUserRequest;

  /**
   * Constructor to be used for creating a runnable for self-healing.
   */
  public FixOfflineReplicasRunnable(KafkaCruiseControl kafkaCruiseControl,
                                    List<String> selfHealingGoals,
                                    boolean allowCapacityEstimation,
                                    boolean excludeRecentlyDemotedBrokers,
                                    boolean excludeRecentlyRemovedBrokers,
                                    String anomalyId,
                                    Supplier<String> reasonSupplier) {
    super(kafkaCruiseControl, new OperationFuture("Disk Failure Self-Healing"));
    _dryRun = SELF_HEALING_DRYRUN;
    _goals = selfHealingGoals;
    _modelCompletenessRequirements = SELF_HEALING_MODEL_COMPLETENESS_REQUIREMENTS;
    _allowCapacityEstimation = allowCapacityEstimation;
    _concurrentInterBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _concurrentLeaderMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _executionProgressCheckIntervalMs = SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
    _skipHardGoalCheck = SELF_HEALING_SKIP_HARD_GOAL_CHECK;
    _excludedTopics = SELF_HEALING_EXCLUDED_TOPICS;
    _uuid = anomalyId;
    _reasonSupplier = reasonSupplier;
    _excludeRecentlyDemotedBrokers = excludeRecentlyDemotedBrokers;
    _excludeRecentlyRemovedBrokers = excludeRecentlyRemovedBrokers;
    _replicaMovementStrategy = SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
    _replicationThrottle = kafkaCruiseControl.config().getLong(DEFAULT_REPLICATION_THROTTLE_CONFIG);
    _stopOngoingExecution = SELF_HEALING_STOP_ONGOING_EXECUTION;
    _isTriggeredByUserRequest = false;
  }

  public FixOfflineReplicasRunnable(KafkaCruiseControl kafkaCruiseControl,
                                    OperationFuture future,
                                    FixOfflineReplicasParameters parameters,
                                    String uuid) {
    super(kafkaCruiseControl, future);
    _dryRun = parameters.dryRun();
    _goals = parameters.goals();
    _modelCompletenessRequirements = parameters.modelCompletenessRequirements();
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _concurrentInterBrokerPartitionMovements = parameters.concurrentInterBrokerPartitionMovements();
    _concurrentLeaderMovements = parameters.concurrentLeaderMovements();
    _executionProgressCheckIntervalMs = parameters.executionProgressCheckIntervalMs();
    _skipHardGoalCheck = parameters.skipHardGoalCheck();
    _excludedTopics = parameters.excludedTopics();
    _uuid = uuid;
    String reason = parameters.reason();
    _reasonSupplier = () -> reason;
    _excludeRecentlyDemotedBrokers = parameters.excludeRecentlyDemotedBrokers();
    _excludeRecentlyRemovedBrokers = parameters.excludeRecentlyRemovedBrokers();
    _replicaMovementStrategy = parameters.replicaMovementStrategy();
    _replicationThrottle = parameters.replicationThrottle();
    _stopOngoingExecution = parameters.stopOngoingExecution();
    _isTriggeredByUserRequest = true;
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(fixOfflineReplicas(), _kafkaCruiseControl.config());
  }

  /**
   * Fix offline replicas on cluster -- i.e. move offline replicas to alive brokers.
   *
   * @return The optimization result.
   *
   * @throws KafkaCruiseControlException When any exception occurred during the process of fixing offline replicas.
   */
  public OptimizerResult fixOfflineReplicas() throws KafkaCruiseControlException {
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
      ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(modelCompletenessRequirements, _allowCapacityEstimation, operationProgress);
      // Ensure that the generated cluster model contains offline replicas.
      sanityCheckOfflineReplicaPresence(clusterModel);
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
                                             Collections.emptySet(),
                                             false,
                                             _concurrentInterBrokerPartitionMovements,
                                             null,
                                             _concurrentLeaderMovements,
                                             _executionProgressCheckIntervalMs,
                                             _replicaMovementStrategy,
                                             _replicationThrottle,
                                             _isTriggeredByUserRequest,
                                             _uuid,
                                             _reasonSupplier);
      }
      return result;
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
  }
}
