/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Set;


/**
 * The async runnable for {@link KafkaCruiseControl#demoteBrokers(Set, boolean,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean, Integer, boolean, boolean,
 * Long, ReplicaMovementStrategy, String, boolean)}
 */
public class DemoteBrokerRunnable extends OperationRunnable {
  private final Set<Integer> _brokerIds;
  private final boolean _dryRun;
  private final boolean _allowCapacityEstimation;
  private final Integer _concurrentLeaderMovements;
  private final boolean _skipUrpDemotion;
  private final boolean _excludeFollowerDemotion;
  private final String _uuid;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final ReplicaMovementStrategy _replicaMovementStrategy;
  private final KafkaCruiseControlConfig _config;

  DemoteBrokerRunnable(KafkaCruiseControl kafkaCruiseControl,
                       OperationFuture future,
                       String uuid,
                       DemoteBrokerParameters parameters,
                       KafkaCruiseControlConfig config) {
    super(kafkaCruiseControl, future);
    _brokerIds = parameters.brokerIds();
    _dryRun = parameters.dryRun();
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _concurrentLeaderMovements = parameters.concurrentLeaderMovements();
    _skipUrpDemotion = parameters.skipUrpDemotion();
    _excludeFollowerDemotion = parameters.excludeFollowerDemotion();
    _replicaMovementStrategy = parameters.replicaMovementStrategy();
    _uuid = uuid;
    _excludeRecentlyDemotedBrokers = parameters.excludeRecentlyDemotedBrokers();
    _config = config;
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(_kafkaCruiseControl.demoteBrokers(_brokerIds,
                                                                    _dryRun,
                                                                    _future.operationProgress(),
                                                                    _allowCapacityEstimation,
                                                                    _concurrentLeaderMovements,
                                                                    _skipUrpDemotion,
                                                                    _excludeFollowerDemotion,
                                                                    null,
                                                                    _replicaMovementStrategy,
                                                                    _uuid,
                                                                    _excludeRecentlyDemotedBrokers),
                                  _config);
  }
}
