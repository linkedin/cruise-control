/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.FixOfflineReplicasParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.List;
import java.util.regex.Pattern;


/**
 * The async runnable for {@link KafkaCruiseControl#fixOfflineReplicas(boolean, List, ModelCompletenessRequirements,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean, Integer, Integer, boolean, Pattern,
 * ReplicaMovementStrategy, Long, String, boolean, boolean)}
 */
class FixOfflineReplicasRunnable extends OperationRunnable {
  private final boolean _dryRun;
  private final List<String> _goals;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  private final boolean _allowCapacityEstimation;
  private final Integer _concurrentInterBrokerPartitionMovements;
  private final Integer _concurrentLeaderMovements;
  private final boolean _skipHardGoalCheck;
  private final Pattern _excludedTopics;
  private final String _uuid;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final boolean _excludeRecentlyRemovedBrokers;
  private final ReplicaMovementStrategy _replicaMovementStrategy;
  private final Long _replicationThrottle;
  private final KafkaCruiseControlConfig _config;

  FixOfflineReplicasRunnable(KafkaCruiseControl kafkaCruiseControl,
                             OperationFuture future,
                             FixOfflineReplicasParameters parameters,
                             String uuid,
                             KafkaCruiseControlConfig config) {
    super(kafkaCruiseControl, future);
    _dryRun = parameters.dryRun();
    _goals = parameters.goals();
    _modelCompletenessRequirements = parameters.modelCompletenessRequirements();
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _concurrentInterBrokerPartitionMovements = parameters.concurrentInterBrokerPartitionMovements();
    _concurrentLeaderMovements = parameters.concurrentLeaderMovements();
    _skipHardGoalCheck = parameters.skipHardGoalCheck();
    _excludedTopics = parameters.excludedTopics();
    _uuid = uuid;
    _excludeRecentlyDemotedBrokers = parameters.excludeRecentlyDemotedBrokers();
    _excludeRecentlyRemovedBrokers = parameters.excludeRecentlyRemovedBrokers();
    _replicaMovementStrategy = parameters.replicaMovementStrategy();
    _replicationThrottle = parameters.replicationThrottle();
    _config = config;
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(_kafkaCruiseControl.fixOfflineReplicas(_dryRun, _goals, _modelCompletenessRequirements,
                                                                         _future.operationProgress(), _allowCapacityEstimation,
                                                                         _concurrentInterBrokerPartitionMovements, _concurrentLeaderMovements,
                                                                         _skipHardGoalCheck, _excludedTopics, _replicaMovementStrategy,
                                                                         _replicationThrottle,
                                                                         _uuid, _excludeRecentlyDemotedBrokers,
                                                                         _excludeRecentlyRemovedBrokers),
                                  _config);
  }
}
