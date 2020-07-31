/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.computeOptimizationOptions;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.isKafkaAssignerMode;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.sanityCheckBrokersHavingOfflineReplicasOnBadDisks;


/**
 * The async runnable for broker addition.
 */
public class AddBrokersRunnable extends GoalBasedOperationRunnable {
  protected final Set<Integer> _brokerIds;
  protected final boolean _throttleAddedBrokers;
  protected final Integer _concurrentInterBrokerPartitionMovements;
  protected final Integer _concurrentLeaderMovements;
  protected final Long _executionProgressCheckIntervalMs;
  protected final ReplicaMovementStrategy _replicaMovementStrategy;
  protected final Long _replicationThrottle;
  // Currently no self-healing action triggers broker addition -- i.e. all are triggered by user requests.
  protected static final boolean ADD_BROKERS_IS_TRIGGERED_BY_USER_REQUEST = true;
  protected static final boolean SKIP_AUTO_REFRESHING_CONCURRENCY = false;

  public AddBrokersRunnable(KafkaCruiseControl kafkaCruiseControl,
                            OperationFuture future,
                            AddBrokerParameters parameters,
                            String uuid) {
    super(kafkaCruiseControl, future, parameters, parameters.dryRun(), parameters.stopOngoingExecution(), parameters.skipHardGoalCheck(),
          uuid, parameters::reason, ADD_BROKERS_IS_TRIGGERED_BY_USER_REQUEST);
    _brokerIds = parameters.brokerIds();
    _throttleAddedBrokers = parameters.throttleAddedBrokers();
    _concurrentInterBrokerPartitionMovements = parameters.concurrentInterBrokerPartitionMovements();
    _concurrentLeaderMovements = parameters.concurrentLeaderMovements();
    _executionProgressCheckIntervalMs = parameters.executionProgressCheckIntervalMs();
    _replicaMovementStrategy = parameters.replicaMovementStrategy();
    _replicationThrottle = parameters.replicationThrottle();
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

    OptimizationOptions optimizationOptions = computeOptimizationOptions(clusterModel,
                                                                         false,
                                                                         _kafkaCruiseControl,
                                                                         _brokerIds,
                                                                         _dryRun,
                                                                         _excludeRecentlyDemotedBrokers,
                                                                         _excludeRecentlyRemovedBrokers,
                                                                         _excludedTopics,
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
                                           SKIP_AUTO_REFRESHING_CONCURRENCY);
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
