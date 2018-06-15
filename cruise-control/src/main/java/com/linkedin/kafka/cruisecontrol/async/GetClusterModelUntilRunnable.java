/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;

/**
 * The async runnable for {@link KafkaCruiseControl#clusterModel(long, ModelCompletenessRequirements,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean)}
 */
class GetClusterModelUntilRunnable extends OperationRunnable<ClusterModel> {
  private final long _time;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  private final boolean _allowCapacityEstimation;

  GetClusterModelUntilRunnable(KafkaCruiseControl kafkaCruiseControl,
                               OperationFuture<ClusterModel> future,
                               long time,
                               ModelCompletenessRequirements modelCompletenessRequirements,
                               boolean allowCapacityEstimation) {
    super(kafkaCruiseControl, future);
    _time = time;
    _modelCompletenessRequirements = modelCompletenessRequirements;
    _allowCapacityEstimation = allowCapacityEstimation;
  }

  @Override
  protected ClusterModel getResult() throws Exception {
    return _kafkaCruiseControl.clusterModel(_time,
                                            _modelCompletenessRequirements,
                                            _future.operationProgress(),
                                            _allowCapacityEstimation);
  }
}
