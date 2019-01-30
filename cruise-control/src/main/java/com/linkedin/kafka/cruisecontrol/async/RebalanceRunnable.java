/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.List;
import java.util.regex.Pattern;


/**
 * The async runnable for {@link KafkaCruiseControl#rebalance(List, boolean, ModelCompletenessRequirements,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean, Integer, Integer, boolean, Pattern, String,
 * boolean, boolean)}
 */
class RebalanceRunnable extends OperationRunnable {
  private final List<String> _goals;
  private final boolean _dryRun;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  private final boolean _allowCapacityEstimation;
  private final Integer _concurrentPartitionMovements;
  private final Integer _concurrentLeaderMovements;
  private final boolean _skipHardGoalCheck;
  private final Pattern _excludedTopics;
  private final String _uuid;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final boolean _excludeRecentlyRemovedBrokers;

  RebalanceRunnable(KafkaCruiseControl kafkaCruiseControl,
                    OperationFuture future,
                    List<String> goals,
                    ModelCompletenessRequirements modelCompletenessRequirements,
                    RebalanceParameters parameters,
                    String uuid) {
    super(kafkaCruiseControl, future);
    _goals = goals;
    _dryRun = parameters.dryRun();
    _modelCompletenessRequirements = modelCompletenessRequirements;
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _concurrentPartitionMovements = parameters.concurrentPartitionMovements();
    _concurrentLeaderMovements = parameters.concurrentLeaderMovements();
    _skipHardGoalCheck = parameters.skipHardGoalCheck();
    _excludedTopics = parameters.excludedTopics();
    _uuid = uuid;
    _excludeRecentlyDemotedBrokers = parameters.excludeRecentlyDemotedBrokers();
    _excludeRecentlyRemovedBrokers = parameters.excludeRecentlyRemovedBrokers();
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(_kafkaCruiseControl.rebalance(_goals,
                                                                _dryRun,
                                                                _modelCompletenessRequirements,
                                                                _future.operationProgress(),
                                                                _allowCapacityEstimation,
                                                                _concurrentPartitionMovements,
                                                                _concurrentLeaderMovements,
                                                                _skipHardGoalCheck,
                                                                _excludedTopics,
                                                                _uuid,
                                                                _excludeRecentlyDemotedBrokers,
                                                                _excludeRecentlyRemovedBrokers));
  }
}
