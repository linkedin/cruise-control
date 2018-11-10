/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.KafkaOptimizationResult;
import java.util.Collection;
import javax.servlet.http.HttpServletRequest;


/**
 * The async runnable for {@link KafkaCruiseControl#demoteBrokers(Collection, boolean,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean, Integer, HttpServletRequest)}
 */
public class DemoteBrokerRunnable extends OperationRunnable {
  private final Collection<Integer> _brokerIds;
  private final boolean _dryRun;
  private final boolean _allowCapacityEstimation;
  private final Integer _concurrentLeaderMovements;
  private final HttpServletRequest _request;

  DemoteBrokerRunnable(KafkaCruiseControl kafkaCruiseControl,
                       OperationFuture future,
                       HttpServletRequest request,
                       DemoteBrokerParameters parameters) {
    super(kafkaCruiseControl, future);
    _brokerIds = parameters.brokerIds();
    _dryRun = parameters.dryRun();
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _concurrentLeaderMovements = parameters.concurrentLeaderMovements();
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
