/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;


public class RebalanceRequest extends AbstractAsyncRequest {
  private final RebalanceParameters _parameters;

  public RebalanceRequest(KafkaCruiseControlServlet servlet, RebalanceParameters parameters) {
    super(servlet);
    _parameters = parameters;
  }

  @Override
  protected OperationFuture handle(String uuid) {
    return _asyncKafkaCruiseControl.rebalance(_parameters, uuid);
  }

  @Override
  public RebalanceParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return RebalanceRequest.class.getSimpleName();
  }
}
