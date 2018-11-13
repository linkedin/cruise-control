/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.FixOfflineReplicasParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.List;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;


/**
 * The async runnable for {@link KafkaCruiseControl#fixOfflineReplicas(boolean, List, ModelCompletenessRequirements,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean,
 * Integer, Integer, boolean, Pattern, HttpServletRequest)}
 */
class FixOfflineReplicasRunnable extends OperationRunnable {
  private final boolean _dryRun;
  private final List<String> _goals;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  private final boolean _allowCapacityEstimation;
  private final Integer _concurrentPartitionMovements;
  private final Integer _concurrentLeaderMovements;
  private final boolean _skipHardGoalCheck;
  private final Pattern _excludedTopics;
  private final HttpServletRequest _request;

  FixOfflineReplicasRunnable(KafkaCruiseControl kafkaCruiseControl,
                             OperationFuture future,
                             List<String> goals,
                             ModelCompletenessRequirements modelCompletenessRequirements,
                             FixOfflineReplicasParameters parameters,
                             HttpServletRequest request) {
    super(kafkaCruiseControl, future);
    _dryRun = parameters.dryRun();
    _goals = goals;
    _modelCompletenessRequirements = modelCompletenessRequirements;
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _concurrentPartitionMovements = parameters.concurrentPartitionMovements();
    _concurrentLeaderMovements = parameters.concurrentLeaderMovements();
    _skipHardGoalCheck = parameters.skipHardGoalCheck();
    _excludedTopics = parameters.excludedTopics();
    _request = request;
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(_kafkaCruiseControl.fixOfflineReplicas(_dryRun, _goals, _modelCompletenessRequirements,
                                                                         _future.operationProgress(), _allowCapacityEstimation,
                                                                         _concurrentPartitionMovements, _concurrentLeaderMovements,
                                                                         _skipHardGoalCheck, _excludedTopics, _request));
  }
}
