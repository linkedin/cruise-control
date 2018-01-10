/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Collection;
import java.util.List;

/**
 * The async runnable for {@link KafkaCruiseControl#addBrokers(Collection, boolean, boolean, List, 
 * ModelCompletenessRequirements, com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress)} 
 */
class AddBrokerRunnable extends OperationRunnable<GoalOptimizer.OptimizerResult> {
  private final Collection<Integer> _brokerIds;
  private final boolean _dryRun;
  private final boolean _throttleAddedBrokers;
  private final List<String> _goals;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;

  AddBrokerRunnable(KafkaCruiseControl kafkaCruiseControl, 
                    OperationFuture<GoalOptimizer.OptimizerResult> future,
                    Collection<Integer> brokerIds, 
                    boolean dryRun, 
                    boolean throttleAddedBrokers, 
                    List<String> goals,
                    ModelCompletenessRequirements modelCompletenessRequirements) {
    super(kafkaCruiseControl, future);
    _brokerIds = brokerIds;
    _dryRun = dryRun;
    _throttleAddedBrokers = throttleAddedBrokers;
    _goals = goals;
    _modelCompletenessRequirements = modelCompletenessRequirements;
  }

  @Override
  protected GoalOptimizer.OptimizerResult getResult() throws Exception {
    return _kafkaCruiseControl.addBrokers(_brokerIds, _dryRun, _throttleAddedBrokers, _goals,
                                          _modelCompletenessRequirements, _future.operationProgress());
  }
}
