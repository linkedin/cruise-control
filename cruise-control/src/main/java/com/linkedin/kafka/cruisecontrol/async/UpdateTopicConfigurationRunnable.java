/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicConfigurationParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicReplicationFactorChangeParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.List;

import java.util.Map;

/**
 * The async runnable for {@link KafkaCruiseControl#updateTopicReplicationFactor(Map, List, boolean, ModelCompletenessRequirements,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean, Integer, Integer, boolean, Long, ReplicaMovementStrategy,
 * boolean, boolean, boolean, String)}.
 */
public class UpdateTopicConfigurationRunnable extends OperationRunnable {
  private final TopicReplicationFactorChangeParameters _topicReplicationFactorChangeParameters;
  private final String _uuid;
  private final KafkaCruiseControlConfig _config;

  UpdateTopicConfigurationRunnable(KafkaCruiseControl kafkaCruiseControl,
                                   OperationFuture future,
                                   String uuid,
                                   TopicConfigurationParameters parameters,
                                   KafkaCruiseControlConfig config) {
    super(kafkaCruiseControl, future);
    _topicReplicationFactorChangeParameters = parameters.topicReplicationFactorChangeParameters();
    _uuid = uuid;
    _config = config;
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    if (_topicReplicationFactorChangeParameters != null) {
      return new OptimizationResult(
          _kafkaCruiseControl.updateTopicReplicationFactor(_topicReplicationFactorChangeParameters.topicPatternByReplicationFactor(),
                                                           _topicReplicationFactorChangeParameters.goals(),
                                                           _topicReplicationFactorChangeParameters.skipRackAwarenessCheck(),
                                                           _topicReplicationFactorChangeParameters.modelCompletenessRequirements(),
                                                           _future.operationProgress(),
                                                           _topicReplicationFactorChangeParameters.allowCapacityEstimation(),
                                                           _topicReplicationFactorChangeParameters.concurrentInterBrokerPartitionMovements(),
                                                           _topicReplicationFactorChangeParameters.concurrentLeaderMovements(),
                                                           _topicReplicationFactorChangeParameters.skipHardGoalCheck(),
                                                           _topicReplicationFactorChangeParameters.executionProgressCheckIntervalMs(),
                                                           _topicReplicationFactorChangeParameters.replicaMovementStrategy(),
                                                           _topicReplicationFactorChangeParameters.excludeRecentlyDemotedBrokers(),
                                                           _topicReplicationFactorChangeParameters.excludeRecentlyRemovedBrokers(),
                                                           _topicReplicationFactorChangeParameters.dryRun(),
                                                           _uuid),
          _config);
    }
    // Never reaches here.
    throw new IllegalArgumentException("Nothing executable found in request.");
  }
}
