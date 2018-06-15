/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.List;

/**
 * The async runnable for {@link KafkaCruiseControl#getOptimizationProposals(
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean)} and
 * {@link KafkaCruiseControl#getOptimizationProposals(List, ModelCompletenessRequirements,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean)}
 */
class GetOptimizationProposalsRunnable extends OperationRunnable<GoalOptimizer.OptimizerResult> {
  private final List<String> _goals;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  private final boolean _allowCapacityEstimation;

  GetOptimizationProposalsRunnable(KafkaCruiseControl kafkaCruiseControl,
                                   OperationFuture<GoalOptimizer.OptimizerResult> future,
                                   List<String> goals,
                                   ModelCompletenessRequirements modelCompletenessRequirements,
                                   boolean allowCapacityEstimation) {
    super(kafkaCruiseControl, future);
    _goals = goals;
    _modelCompletenessRequirements = modelCompletenessRequirements;
    _allowCapacityEstimation = allowCapacityEstimation;
  }

  @Override
  protected GoalOptimizer.OptimizerResult getResult() throws Exception {
    if (_goals != null) {
      return _kafkaCruiseControl.getOptimizationProposals(_goals,
                                                          _modelCompletenessRequirements,
                                                          _future.operationProgress(),
                                                          _allowCapacityEstimation);
    } else {
      return _kafkaCruiseControl.getOptimizationProposals(_future.operationProgress(), _allowCapacityEstimation);
    }
  }
}
