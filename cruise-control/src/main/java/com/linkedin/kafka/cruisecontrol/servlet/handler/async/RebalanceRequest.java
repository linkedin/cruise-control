/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RebalanceRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REBALANCE_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


public class RebalanceRequest extends AbstractAsyncRequest {
  protected RebalanceParameters _parameters;

  public RebalanceRequest() {
    super();
  }

  @Override
  protected OperationFuture handle(String uuid) {
    OperationFuture future = new OperationFuture("Rebalance");
    pending(future.operationProgress());
    _asyncKafkaCruiseControl.sessionExecutor().submit(new RebalanceRunnable(_asyncKafkaCruiseControl, future, _parameters, uuid));
    return future;
  }

  @Override
  public RebalanceParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return RebalanceRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (RebalanceParameters) validateNotNull(configs.get(REBALANCE_PARAMETER_OBJECT_CONFIG),
            "Parameter configuration is missing from the request.");
  }
}
