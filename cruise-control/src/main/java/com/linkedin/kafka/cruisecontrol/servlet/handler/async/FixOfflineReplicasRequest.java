/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.FixOfflineReplicasParameters;


public class FixOfflineReplicasRequest extends AbstractAsyncRequest {
  private final FixOfflineReplicasParameters _parameters;

  public FixOfflineReplicasRequest(KafkaCruiseControlServlet servlet, FixOfflineReplicasParameters parameters) {
    super(servlet);
    _parameters = parameters;
  }

  @Override
  protected OperationFuture handle(String uuid) {
    return _asyncKafkaCruiseControl.fixOfflineReplicas(_parameters, uuid);
  }

  @Override
  public FixOfflineReplicasParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return FixOfflineReplicasRequest.class.getSimpleName();
  }
}