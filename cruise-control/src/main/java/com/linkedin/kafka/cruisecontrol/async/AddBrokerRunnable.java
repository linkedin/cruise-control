/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;


/**
 * The async runnable for {@link KafkaCruiseControl#addBrokers(Collection, boolean, boolean, List,
 * ModelCompletenessRequirements, com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean, Integer,
 * Integer, boolean, Pattern)}
 */
class AddBrokerRunnable extends OperationRunnable<GoalOptimizer.OptimizerResult> {
  private final Collection<Integer> _brokerIds;
  private final boolean _dryRun;
  private final boolean _throttleAddedBrokers;
  private final List<String> _goals;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  private final boolean _allowCapacityEstimation;
  private final Integer _concurrentPartitionMovements;
  private final Integer _concurrentLeaderMovements;
  private final boolean _skipHardGoalCheck;
  private final Pattern _excludedTopics;

  AddBrokerRunnable(KafkaCruiseControl kafkaCruiseControl,
                    OperationFuture<GoalOptimizer.OptimizerResult> future,
                    Collection<Integer> brokerIds,
                    boolean dryRun,
                    boolean throttleAddedBrokers,
                    List<String> goals,
                    ModelCompletenessRequirements modelCompletenessRequirements,
                    boolean allowCapacityEstimation,
                    Integer concurrentPartitionMovements,
                    Integer concurrentLeaderMovements,
                    boolean skipHardGoalCheck,
                    Pattern excludedTopics) {
    super(kafkaCruiseControl, future);
    _brokerIds = brokerIds;
    _dryRun = dryRun;
    _throttleAddedBrokers = throttleAddedBrokers;
    _goals = goals;
    _modelCompletenessRequirements = modelCompletenessRequirements;
    _allowCapacityEstimation = allowCapacityEstimation;
    _concurrentPartitionMovements = concurrentPartitionMovements;
    _concurrentLeaderMovements = concurrentLeaderMovements;
    _skipHardGoalCheck = skipHardGoalCheck;
    _excludedTopics = excludedTopics;
  }

  @Override
  protected GoalOptimizer.OptimizerResult getResult() throws Exception {
    return _kafkaCruiseControl.addBrokers(_brokerIds, _dryRun, _throttleAddedBrokers, _goals,
                                          _modelCompletenessRequirements, _future.operationProgress(),
                                          _allowCapacityEstimation, _concurrentPartitionMovements,
                                          _concurrentLeaderMovements, _skipHardGoalCheck, _excludedTopics);
  }
}
