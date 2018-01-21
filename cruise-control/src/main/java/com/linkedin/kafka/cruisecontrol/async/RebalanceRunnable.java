/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.List;

/**
 * The async runnable for {@link KafkaCruiseControl#rebalance(List, boolean, ModelCompletenessRequirements, 
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress)}
 */
class RebalanceRunnable extends OperationRunnable<GoalOptimizer.OptimizerResult> {
  private final List<String> _goals;
  private final boolean _dryRun;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  
  RebalanceRunnable(KafkaCruiseControl kafkaCruiseControl,
                    OperationFuture<GoalOptimizer.OptimizerResult> future,
                    List<String> goals,
                    boolean dryRun,
                    ModelCompletenessRequirements modelCompletenessRequirements) {
    super(kafkaCruiseControl, future);
    _goals = goals;
    _dryRun = dryRun;
    _modelCompletenessRequirements = modelCompletenessRequirements;
  }

  @Override
  protected GoalOptimizer.OptimizerResult getResult() throws Exception {
    return _kafkaCruiseControl.rebalance(_goals, _dryRun, _modelCompletenessRequirements, _future.operationProgress());
  }
}
