/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import java.util.Collection;


public class DemoteBrokerRunnable extends OperationRunnable<GoalOptimizer.OptimizerResult> {
  private final Collection<Integer> _brokerIds;
  private final boolean _dryRun;

  DemoteBrokerRunnable(KafkaCruiseControl kafkaCruiseControl,
                       OperationFuture<GoalOptimizer.OptimizerResult> future,
                       Collection<Integer> brokerIds,
                       boolean dryRun) {
    super(kafkaCruiseControl, future);
    _brokerIds = brokerIds;
    _dryRun = dryRun;
  }

  @Override
  protected GoalOptimizer.OptimizerResult getResult() throws Exception {
    return _kafkaCruiseControl.demoteBrokers(_brokerIds, _dryRun, _future.operationProgress());
  }
}
