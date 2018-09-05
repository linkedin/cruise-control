/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import java.util.Collection;
import java.util.regex.Pattern;


public class DemoteBrokerRunnable extends OperationRunnable<GoalOptimizer.OptimizerResult> {
  private final Collection<Integer> _brokerIds;
  private final boolean _dryRun;
  private final boolean _allowCapacityEstimation;
  private final Integer _concurrentLeaderMovements;
  private final Pattern _excludedTopics;

  DemoteBrokerRunnable(KafkaCruiseControl kafkaCruiseControl,
                       OperationFuture<GoalOptimizer.OptimizerResult> future,
                       Collection<Integer> brokerIds,
                       boolean dryRun,
                       boolean allowCapacityEstimation,
                       Integer concurrentLeaderMovements,
                       Pattern excludedTopics) {
    super(kafkaCruiseControl, future);
    _brokerIds = brokerIds;
    _dryRun = dryRun;
    _allowCapacityEstimation = allowCapacityEstimation;
    _concurrentLeaderMovements = concurrentLeaderMovements;
    _excludedTopics = excludedTopics;
  }

  @Override
  protected GoalOptimizer.OptimizerResult getResult() throws Exception {
    return _kafkaCruiseControl.demoteBrokers(_brokerIds, _dryRun, _future.operationProgress(),
                                             _allowCapacityEstimation, _concurrentLeaderMovements, _excludedTopics);
  }
}
