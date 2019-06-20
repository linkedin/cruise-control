/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveBrokerParameters;


public class RemoveBrokerRequest extends AbstractAsyncRequest {
  private final RemoveBrokerParameters _parameters;

  public RemoveBrokerRequest(KafkaCruiseControlServlet servlet, RemoveBrokerParameters parameters) {
    super(servlet);
    _parameters = parameters;
  }

  @Override
  protected OperationFuture handle(String uuid) {
    return _asyncKafkaCruiseControl.decommissionBrokers(_parameters, uuid);
  }

  @Override
  public RemoveBrokerParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return RemoveBrokerRequest.class.getSimpleName();
  }
}
