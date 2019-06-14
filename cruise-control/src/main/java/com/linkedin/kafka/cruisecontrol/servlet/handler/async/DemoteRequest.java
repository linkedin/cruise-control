/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;


public class DemoteRequest extends AbstractAsyncRequest {
  private final DemoteBrokerParameters _parameters;

  public DemoteRequest(KafkaCruiseControlServlet servlet, DemoteBrokerParameters parameters) {
    super(servlet);
    _parameters = parameters;
  }

  @Override
  protected OperationFuture handle(String uuid) {
    return _asyncKafkaCruiseControl.demoteBrokers(uuid, _parameters);
  }

  @Override
  public DemoteBrokerParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return DemoteRequest.class.getSimpleName();
  }
}
