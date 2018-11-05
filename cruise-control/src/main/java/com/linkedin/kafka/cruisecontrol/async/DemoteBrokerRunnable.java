/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaOptimizationResult;
import java.util.Collection;
import javax.servlet.http.HttpServletRequest;


/**
 * The async runnable for {@link KafkaCruiseControl#demoteBrokers(Collection, boolean,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean, Integer, HttpServletRequest)}
 */
public class DemoteBrokerRunnable extends OperationRunnable<KafkaOptimizationResult> {
  private final Collection<Integer> _brokerIds;
  private final boolean _dryRun;
  private final boolean _allowCapacityEstimation;
  private final Integer _concurrentLeaderMovements;
  private final HttpServletRequest _request;

  DemoteBrokerRunnable(KafkaCruiseControl kafkaCruiseControl,
                       OperationFuture<KafkaOptimizationResult> future,
                       Collection<Integer> brokerIds,
                       boolean dryRun,
                       boolean allowCapacityEstimation,
                       Integer concurrentLeaderMovements,
                       HttpServletRequest request) {
    super(kafkaCruiseControl, future);
    _brokerIds = brokerIds;
    _dryRun = dryRun;
    _allowCapacityEstimation = allowCapacityEstimation;
    _concurrentLeaderMovements = concurrentLeaderMovements;
    _request = request;
  }

  @Override
  protected KafkaOptimizationResult getResult() throws Exception {
    return new KafkaOptimizationResult(_kafkaCruiseControl.demoteBrokers(_brokerIds,
                                                                         _dryRun,
                                                                         _future.operationProgress(),
                                                                         _allowCapacityEstimation,
                                                                         _concurrentLeaderMovements,
                                                                         _request));
  }
}
