/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;


public class PartitionLoadRequest extends AbstractAsyncRequest {
  private final PartitionLoadParameters _parameters;

  public PartitionLoadRequest(KafkaCruiseControlServlet servlet, PartitionLoadParameters parameters) {
    super(servlet);
    _parameters = parameters;
  }

  @Override
  protected OperationFuture handle(String uuid) {
    return _asyncKafkaCruiseControl.partitionLoadState(_parameters);
  }

  @Override
  public PartitionLoadParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return PartitionLoadRequest.class.getSimpleName();
  }
}
