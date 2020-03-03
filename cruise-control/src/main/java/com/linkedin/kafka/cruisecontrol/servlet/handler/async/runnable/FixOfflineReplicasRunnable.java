/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.FixOfflineReplicasParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_CONCURRENT_MOVEMENTS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.sanityCheckOfflineReplicaPresence;


/**
 * The async runnable for fixing offline replicas -- i.e. move offline replicas to alive brokers/disks.
 */
public class FixOfflineReplicasRunnable extends GoalBasedOperationRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(FixOfflineReplicasRunnable.class);
  protected final Integer _concurrentInterBrokerPartitionMovements;
  protected final Integer _concurrentLeaderMovements;
  protected final Long _executionProgressCheckIntervalMs;
  protected final String _uuid;
  protected final Supplier<String> _reasonSupplier;
  protected final ReplicaMovementStrategy _replicaMovementStrategy;
  protected final Long _replicationThrottle;
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
    super(kafkaCruiseControl, new OperationFuture("Disk Failure Self-Healing"), selfHealingGoals, allowCapacityEstimation,
          excludeRecentlyDemotedBrokers, excludeRecentlyRemovedBrokers);
    _concurrentInterBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _concurrentLeaderMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _executionProgressCheckIntervalMs = SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
    _uuid = anomalyId;
    _reasonSupplier = reasonSupplier;
    _replicaMovementStrategy = SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
    _replicationThrottle = kafkaCruiseControl.config().getLong(DEFAULT_REPLICATION_THROTTLE_CONFIG);
    _isTriggeredByUserRequest = false;
  }

  public FixOfflineReplicasRunnable(KafkaCruiseControl kafkaCruiseControl,
                                    OperationFuture future,
                                    FixOfflineReplicasParameters parameters,
                                    String uuid) {
    super(kafkaCruiseControl, future, parameters, parameters.dryRun(), parameters.stopOngoingExecution(), parameters.skipHardGoalCheck());
    _concurrentInterBrokerPartitionMovements = parameters.concurrentInterBrokerPartitionMovements();
    _concurrentLeaderMovements = parameters.concurrentLeaderMovements();
    _executionProgressCheckIntervalMs = parameters.executionProgressCheckIntervalMs();
    _uuid = uuid;
    String reason = parameters.reason();
    _reasonSupplier = () -> reason;
    _replicaMovementStrategy = parameters.replicaMovementStrategy();
    _replicationThrottle = parameters.replicationThrottle();
    _isTriggeredByUserRequest = true;
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(computeResult(), _kafkaCruiseControl.config());
  }

  @Override
  protected OptimizerResult workWithClusterModel() throws KafkaCruiseControlException, TimeoutException, NotEnoughValidWindowsException {
    ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(_combinedCompletenessRequirements, _allowCapacityEstimation, _operationProgress);
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

    OptimizerResult result = _kafkaCruiseControl.optimizations(clusterModel, _goalsByPriority, _operationProgress, null, optimizationOptions);
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
  }

  @Override
  protected boolean shouldWorkWithClusterModel() {
    return true;
  }

  @Override
  protected OptimizerResult workWithoutClusterModel() {
    return null;
  }
}
