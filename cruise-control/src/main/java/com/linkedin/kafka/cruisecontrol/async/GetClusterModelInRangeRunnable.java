/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.PartitionLoadState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

/**
 * The async runnable for {@link KafkaCruiseControl#clusterModel(long, long, Double,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean)}
 */
class GetClusterModelInRangeRunnable extends OperationRunnable {
  private final PartitionLoadParameters _parameters;

  GetClusterModelInRangeRunnable(KafkaCruiseControl kafkaCruiseControl,
                                 OperationFuture future,
                                 PartitionLoadParameters parameters) {
    super(kafkaCruiseControl, future);
    _parameters = parameters;
  }

  @Override
  protected PartitionLoadState getResult() throws Exception {
    ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(_parameters.startMs(),
                                                                 _parameters.endMs(),
                                                                 _parameters.minValidPartitionRatio(),
                                                                 _future.operationProgress(),
                                                                 _parameters.allowCapacityEstimation());
    int topicNameLength = clusterModel.topics().stream().mapToInt(String::length).max().orElse(20) + 5;
    return new PartitionLoadState(clusterModel.replicasSortedByUtilization(_parameters.resource()),
                                  _parameters.wantMaxLoad(),
                                  _parameters.entries(),
                                  _parameters.partitionUpperBoundary(),
                                  _parameters.partitionLowerBoundary(),
                                  _parameters.topic(),
                                  topicNameLength);
  }
}
