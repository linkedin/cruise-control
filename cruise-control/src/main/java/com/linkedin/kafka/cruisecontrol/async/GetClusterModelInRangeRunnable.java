/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

/**
 * The async runnable for {@link KafkaCruiseControl#clusterModel(long, long, Double,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean)}
 */
class GetClusterModelInRangeRunnable extends OperationRunnable<ClusterModel> {
  private final long _startMs;
  private final long _endMs;
  private final Double _minValidPartitionRatio;
  private final boolean _allowCapacityEstimation;

  GetClusterModelInRangeRunnable(KafkaCruiseControl kafkaCruiseControl,
                                 OperationFuture<ClusterModel> future,
                                 long startMs,
                                 long endMs,
                                 Double minValidPartitionRatio,
                                 boolean allowCapacityEstimation) {
    super(kafkaCruiseControl, future);
    _startMs = startMs;
    _endMs = endMs;
    _minValidPartitionRatio = minValidPartitionRatio;
    _allowCapacityEstimation = allowCapacityEstimation;
  }

  @Override
  protected ClusterModel getResult() throws Exception {
    return _kafkaCruiseControl.clusterModel(_startMs,
                                            _endMs,
                                            _minValidPartitionRatio,
                                            _future.operationProgress(),
                                            _allowCapacityEstimation);
  }
}
