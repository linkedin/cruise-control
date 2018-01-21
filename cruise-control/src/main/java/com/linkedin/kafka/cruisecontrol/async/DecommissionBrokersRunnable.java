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
 * The async runnable for {@link KafkaCruiseControl#decommissionBrokers(Collection, boolean, boolean, List, 
 * ModelCompletenessRequirements, com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress)}
 */
class DecommissionBrokersRunnable extends OperationRunnable<GoalOptimizer.OptimizerResult> {
  private final Collection<Integer> _brokerIds;
  private final boolean _dryRun;
  private final boolean _throttleRemovedBrokers;
  private final List<String> _goals;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;

  DecommissionBrokersRunnable(KafkaCruiseControl kafkaCruiseControl, 
                              OperationFuture<GoalOptimizer.OptimizerResult> future, 
                              Collection<Integer> brokerIds, 
                              boolean dryRun, 
                              boolean throttleRemovedBrokers, 
                              List<String> goals, 
                              ModelCompletenessRequirements modelCompletenessRequirements) {
    super(kafkaCruiseControl, future);
    _brokerIds = brokerIds;
    _dryRun = dryRun;
    _throttleRemovedBrokers = throttleRemovedBrokers;
    _goals = goals;
    _modelCompletenessRequirements = modelCompletenessRequirements;
  }

  @Override
  protected GoalOptimizer.OptimizerResult getResult() throws Exception {
    return _kafkaCruiseControl.decommissionBrokers(_brokerIds, _dryRun, _throttleRemovedBrokers, _goals, 
                                                   _modelCompletenessRequirements, _future.operationProgress());
  }
}
