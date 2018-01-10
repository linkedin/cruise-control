/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;

/**
 * The async runnable for {@link KafkaCruiseControl#clusterModel(long, long, ModelCompletenessRequirements,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress)}
 */
class GetClusterModelInRangeRunnable extends OperationRunnable<ClusterModel> {
  private final long _startMs;
  private final long _endMs;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  
  GetClusterModelInRangeRunnable(KafkaCruiseControl kafkaCruiseControl,
                                 OperationFuture<ClusterModel> future,
                                 long startMs,
                                 long endMs,
                                 ModelCompletenessRequirements modelCompletenessRequirements) {
    super(kafkaCruiseControl, future);
    _startMs = startMs;
    _endMs = endMs;
    _modelCompletenessRequirements = modelCompletenessRequirements;
  }

  @Override
  protected ClusterModel getResult() throws Exception {
    return _kafkaCruiseControl.clusterModel(_startMs, _endMs, _modelCompletenessRequirements, _future.operationProgress());
  }
}
