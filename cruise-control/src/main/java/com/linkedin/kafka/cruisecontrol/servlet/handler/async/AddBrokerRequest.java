/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddBrokerParameters;


public class AddBrokerRequest extends AbstractAsyncRequest {
  private final AddBrokerParameters _parameters;

  public AddBrokerRequest(KafkaCruiseControlServlet servlet, AddBrokerParameters parameters) {
    super(servlet);
    _parameters = parameters;
  }

  @Override
  protected OperationFuture handle(String uuid) {
    return _asyncKafkaCruiseControl.addBrokers(_parameters, uuid);
  }

  @Override
  public AddBrokerParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return AddBrokerRequest.class.getSimpleName();
  }
}
