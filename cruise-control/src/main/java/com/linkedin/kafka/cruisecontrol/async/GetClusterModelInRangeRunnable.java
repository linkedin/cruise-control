/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.PartitionLoadState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The async runnable for {@link KafkaCruiseControl#clusterModel(long, long, Double,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean)}
 */
class GetClusterModelInRangeRunnable extends OperationRunnable {
  private final PartitionLoadParameters _parameters;
  private final KafkaCruiseControlConfig _config;

  GetClusterModelInRangeRunnable(KafkaCruiseControl kafkaCruiseControl,
                                 OperationFuture future,
                                 PartitionLoadParameters parameters,
                                 KafkaCruiseControlConfig config) {
    super(kafkaCruiseControl, future);
    _parameters = parameters;
    _config = config;
  }

  @Override
  protected PartitionLoadState getResult() throws Exception {
    _kafkaCruiseControl.sanityCheckBrokerPresence(_parameters.brokerIds());

    ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(_parameters.startMs(),
                                                                 _parameters.endMs(),
                                                                 _parameters.minValidPartitionRatio(),
                                                                 _future.operationProgress(),
                                                                 _parameters.allowCapacityEstimation());
    int topicNameLength = clusterModel.topics().stream().mapToInt(String::length).max().orElse(20) + 5;
    List<Partition> partitionList = clusterModel.replicasSortedByUtilization(_parameters.resource(),
                                                                             _parameters.wantMaxLoad(),
                                                                             _parameters.wantAvgLoad());
    if (!_parameters.brokerIds().isEmpty()) {
      partitionList = partitionList.stream()
                                   .filter(partition -> partition.partitionBrokers().stream().anyMatch(
                                             broker -> _parameters.brokerIds().contains(broker.id())))
                                   .collect(Collectors.toList());
    }
    return new PartitionLoadState(partitionList,
                                  _parameters.wantMaxLoad(),
                                  _parameters.wantAvgLoad(),
                                  _parameters.entries(),
                                  _parameters.partitionUpperBoundary(),
                                  _parameters.partitionLowerBoundary(),
                                  _parameters.topic(),
                                  topicNameLength,
                                  _config);
  }
}
