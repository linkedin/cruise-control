/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;


/**
 * The async runnable to get the {@link com.linkedin.kafka.cruisecontrol.model.ClusterModel.BrokerStats} for the cluster 
 * model.
 * 
 * @see KafkaCruiseControl#clusterModel(long, ModelCompletenessRequirements, 
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress) 
 */
class GetBrokerStatsRunnable extends OperationRunnable<ClusterModel.BrokerStats> {
  private final long _time;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  
  GetBrokerStatsRunnable(KafkaCruiseControl kafkaCruiseControl,
                         OperationFuture<ClusterModel.BrokerStats> future,
                         long time,
                         ModelCompletenessRequirements modelCompletenessRequirements) {
    super(kafkaCruiseControl, future);
    _time = time;
    _modelCompletenessRequirements = modelCompletenessRequirements;
  }

  @Override
  protected ClusterModel.BrokerStats getResult() throws Exception {
    return _kafkaCruiseControl.clusterModel(_time, _modelCompletenessRequirements, _future.operationProgress()).brokerStats();
  }
}
