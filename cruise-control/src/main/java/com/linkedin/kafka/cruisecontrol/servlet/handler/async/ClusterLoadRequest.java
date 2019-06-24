/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ClusterLoadParameters;


public class ClusterLoadRequest extends AbstractAsyncRequest {
  private final ClusterLoadParameters _parameters;

  public ClusterLoadRequest(KafkaCruiseControlServlet servlet, ClusterLoadParameters parameters) {
    super(servlet);
    _parameters = parameters;
  }

  @Override
  protected OperationFuture handle(String uuid) {
    return _asyncKafkaCruiseControl.getBrokerStats(_parameters);
  }

  @Override
  public ClusterLoadParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return ClusterLoadRequest.class.getSimpleName();
  }
}
