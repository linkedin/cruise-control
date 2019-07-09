/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ClusterLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;


/**
 * The async runnable to get the {@link BrokerStats} for the cluster model.
 *
 * @see KafkaCruiseControl#clusterModel(long, long, ModelCompletenessRequirements,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean)
 */
class GetBrokerStatsRunnable extends OperationRunnable {
  private final long _start;
  private final long _end;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  private final boolean _allowCapacityEstimation;
  private final KafkaCruiseControlConfig _config;

  GetBrokerStatsRunnable(KafkaCruiseControl kafkaCruiseControl,
                         OperationFuture future,
                         ClusterLoadParameters parameters,
                         KafkaCruiseControlConfig config) {
    super(kafkaCruiseControl, future);
    _start = parameters.startMs() != null ? parameters.startMs() : -1;
    _end = parameters.endMs() != null ? parameters.endMs() : parameters.time();
    _modelCompletenessRequirements = parameters.requirements();
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _config = config;
  }

  @Override
  protected BrokerStats getResult() throws Exception {
    // Check whether the cached broker stats is still valid.
    BrokerStats cachedBrokerStats = _kafkaCruiseControl.cachedBrokerLoadStats(_allowCapacityEstimation);
    if (cachedBrokerStats != null) {
      return cachedBrokerStats;
    }
    return _kafkaCruiseControl.clusterModel(_start,
                                            _end,
                                            _modelCompletenessRequirements,
                                            _future.operationProgress(),
                                            _allowCapacityEstimation).brokerStats(_config);
  }
}
