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
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.isKafkaAssignerMode;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.sanityCheckBrokersHavingOfflineReplicasOnBadDisks;


/**
 * The async runnable for broker addition.
 */
public class AddBrokersRunnable extends GoalBasedOperationRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(AddBrokersRunnable.class);
  protected final Set<Integer> _brokerIds;
  protected final boolean _throttleAddedBrokers;
  protected final Integer _concurrentInterBrokerPartitionMovements;
  protected final Integer _concurrentLeaderMovements;
  protected final Long _executionProgressCheckIntervalMs;
  protected final String _uuid;
  protected final String _reason;
  protected final ReplicaMovementStrategy _replicaMovementStrategy;
  protected final Long _replicationThrottle;

  public AddBrokersRunnable(KafkaCruiseControl kafkaCruiseControl,
                            OperationFuture future,
                            AddBrokerParameters parameters,
                            String uuid) {
    super(kafkaCruiseControl, future, parameters, parameters.dryRun(), parameters.stopOngoingExecution(), parameters.skipHardGoalCheck());
    _brokerIds = parameters.brokerIds();
    _throttleAddedBrokers = parameters.throttleAddedBrokers();
    _concurrentInterBrokerPartitionMovements = parameters.concurrentInterBrokerPartitionMovements();
    _concurrentLeaderMovements = parameters.concurrentLeaderMovements();
    _executionProgressCheckIntervalMs = parameters.executionProgressCheckIntervalMs();
    _replicaMovementStrategy = parameters.replicaMovementStrategy();
    _replicationThrottle = parameters.replicationThrottle();
    _uuid = uuid;
    _reason = parameters.reason();
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(computeResult(), _kafkaCruiseControl.config());
  }

  @Override
  protected OptimizerResult workWithClusterModel() throws KafkaCruiseControlException, TimeoutException, NotEnoughValidWindowsException {
    _kafkaCruiseControl.sanityCheckBrokerPresence(_brokerIds);
    ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(_combinedCompletenessRequirements, _allowCapacityEstimation, _operationProgress);
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

    OptimizerResult result = _kafkaCruiseControl.optimizations(clusterModel, _goalsByPriority, _operationProgress, null, optimizationOptions);
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
