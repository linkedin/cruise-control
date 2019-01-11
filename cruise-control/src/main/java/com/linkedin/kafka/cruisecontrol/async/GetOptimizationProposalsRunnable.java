/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.List;
import java.util.regex.Pattern;


/**
 * The async runnable for {@link KafkaCruiseControl#getOptimizationProposals(
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean)} and
 * {@link KafkaCruiseControl#getOptimizationProposals(List, ModelCompletenessRequirements,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean, boolean, Pattern, boolean)}
 */
class GetOptimizationProposalsRunnable extends OperationRunnable {
  private final List<String> _goals;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  private final boolean _allowCapacityEstimation;
  private final Pattern _excludedTopics;
  private final boolean _excludeRecentlyDemotedBrokers;

  GetOptimizationProposalsRunnable(KafkaCruiseControl kafkaCruiseControl,
                                   OperationFuture future,
                                   List<String> goals,
                                   ModelCompletenessRequirements modelCompletenessRequirements,
                                   ProposalsParameters parameters) {
    super(kafkaCruiseControl, future);
    _goals = goals;
    _modelCompletenessRequirements = modelCompletenessRequirements;
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _excludedTopics = parameters.excludedTopics();
    _excludeRecentlyDemotedBrokers = parameters.excludeRecentlyDemotedBrokers();
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    GoalOptimizer.OptimizerResult optimizerResult;
    if (_goals != null || _excludedTopics != null || _excludeRecentlyDemotedBrokers) {
      optimizerResult = _kafkaCruiseControl.getOptimizationProposals(_goals,
                                                                     _modelCompletenessRequirements,
                                                                     _future.operationProgress(),
                                                                     _allowCapacityEstimation,
                                                                     true,
                                                                     _excludedTopics,
                                                                     _excludeRecentlyDemotedBrokers);
    } else {
      optimizerResult = _kafkaCruiseControl.getOptimizationProposals(_future.operationProgress(), _allowCapacityEstimation);
    }
    return new OptimizationResult(optimizerResult);
  }
}
