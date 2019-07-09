/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicConfigurationParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.List;

import java.util.regex.Pattern;

/**
 * The async runnable for {@link KafkaCruiseControl#updateTopicConfiguration(Pattern, List, short, boolean, ModelCompletenessRequirements,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean, Integer, Integer, boolean, ReplicaMovementStrategy, Long,
 * boolean, boolean, boolean, String)}.
 */
public class UpdateTopicConfigurationRunnable extends OperationRunnable {
  private final Pattern _topic;
  private final short _replicationFactor;
  private final boolean _skipRackAwarenessCheck;
  private final String _uuid;
  private final KafkaCruiseControlConfig _config;
  private final List<String> _goals;
  private final boolean _dryRun;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  private final boolean _allowCapacityEstimation;
  private final Integer _concurrentInterBrokerPartitionMovements;
  private final Integer _concurrentLeaderMovements;
  private final boolean _skipHardGoalCheck;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final boolean _excludeRecentlyRemovedBrokers;
  private final ReplicaMovementStrategy _replicaMovementStrategy;
  private final Long _replicationThrottle;

  UpdateTopicConfigurationRunnable(KafkaCruiseControl kafkaCruiseControl,
                                   OperationFuture future,
                                   String uuid,
                                   TopicConfigurationParameters parameters,
                                   KafkaCruiseControlConfig config) {
    super(kafkaCruiseControl, future);
    _uuid = uuid;
    _topic = parameters.topic();
    _goals = parameters.goals();
    _replicationFactor = parameters.replicationFactor();
    _skipRackAwarenessCheck = parameters.skipRackAwarenessCheck();
    _dryRun = parameters.dryRun();
    _modelCompletenessRequirements = parameters.modelCompletenessRequirements();
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _concurrentInterBrokerPartitionMovements = parameters.concurrentInterBrokerPartitionMovements();
    _concurrentLeaderMovements = parameters.concurrentLeaderMovements();
    _skipHardGoalCheck = parameters.skipHardGoalCheck();
    _replicaMovementStrategy = parameters.replicaMovementStrategy();
    _replicationThrottle = parameters.replicationThrottle();
    _excludeRecentlyDemotedBrokers = parameters.excludeRecentlyDemotedBrokers();
    _excludeRecentlyRemovedBrokers = parameters.excludeRecentlyRemovedBrokers();
    _config = config;
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(_kafkaCruiseControl.updateTopicConfiguration(_topic,
                                                                               _goals,
                                                                               _replicationFactor,
                                                                               _skipRackAwarenessCheck,
                                                                               _modelCompletenessRequirements,
                                                                               _future.operationProgress(),
                                                                               _allowCapacityEstimation,
                                                                               _concurrentInterBrokerPartitionMovements,
                                                                               _concurrentLeaderMovements,
                                                                               _skipHardGoalCheck,
                                                                               _replicaMovementStrategy,
                                                                               _replicationThrottle,
                                                                               _excludeRecentlyDemotedBrokers,
                                                                               _excludeRecentlyRemovedBrokers,
                                                                               _dryRun,
                                                                               _uuid),
                                  _config);
  }
}
