/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlStateParameters;


public class CruiseControlStateRequest extends AbstractAsyncRequest {
  private final CruiseControlStateParameters _parameters;

  public CruiseControlStateRequest(KafkaCruiseControlServlet servlet, CruiseControlStateParameters parameters) {
    super(servlet);
    _parameters = parameters;
  }

  @Override
  protected OperationFuture handle(String uuid) {
    return _asyncKafkaCruiseControl.state(_parameters);
  }

  @Override
  public CruiseControlStateParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return CruiseControlStateRequest.class.getSimpleName();
  }
}
