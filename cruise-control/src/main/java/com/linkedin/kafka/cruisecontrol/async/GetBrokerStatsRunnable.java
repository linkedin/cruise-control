/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ClusterLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;


/**
 * The async runnable to get the {@link BrokerStats} for the cluster model.
 *
 * see {@link KafkaCruiseControl#clusterModel(long, ModelCompletenessRequirements,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean)}
 * and {@link KafkaCruiseControl#clusterModel(long, long, Double,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean)}
 */
class GetBrokerStatsRunnable extends OperationRunnable {
  private final long _start;
  private final long _end;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  private final boolean _allowCapacityEstimation;
  private final KafkaCruiseControlConfig _config;
  private final boolean _isCapacityStats;

  GetBrokerStatsRunnable(KafkaCruiseControl kafkaCruiseControl,
                         OperationFuture future,
                         ClusterLoadParameters parameters,
                         KafkaCruiseControlConfig config) {
    super(kafkaCruiseControl, future);
    _start = parameters.startMs();
    _end = parameters.endMs();
    _modelCompletenessRequirements = parameters.requirements();
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _config = config;
    _isCapacityStats = parameters.capacity();
  }

  @Override
  protected BrokerStats getResult() throws Exception {
    if (_isCapacityStats) {
        return _kafkaCruiseControl.clusterModel(_time,
                                                _modelCompletenessRequirements,
                                                _future.operationProgress(),
                                                _allowCapacityEstimation)
                                  .brokerCapacityStats(_config);
    }

    // Check whether the cached broker stats is still valid.
    BrokerStats cachedBrokerStats = _kafkaCruiseControl.cachedBrokerLoadStats(_allowCapacityEstimation);
    if (cachedBrokerStats != null) {
      return cachedBrokerStats;
    }
    if (_start != DEFAULT_START_TIME_FOR_CLUSTER_MODEL) {
      return _kafkaCruiseControl.clusterModel(_start,
                                              _end,
                                              _modelCompletenessRequirements.minMonitoredPartitionsPercentage(),
                                              _future.operationProgress(),
                                              _allowCapacityEstimation).brokerStats(_config);
    } else {
      return _kafkaCruiseControl.clusterModel(_end,
                                              _modelCompletenessRequirements,
                                              _future.operationProgress(),
                                              _allowCapacityEstimation).brokerStats(_config);
    }
  }
}
