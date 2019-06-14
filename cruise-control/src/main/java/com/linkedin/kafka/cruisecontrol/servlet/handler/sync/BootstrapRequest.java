/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.BootstrapParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.BootstrapResult;


public class BootstrapRequest extends AbstractSyncRequest {
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final BootstrapParameters _parameters;

  public BootstrapRequest(KafkaCruiseControlServlet servlet, BootstrapParameters parameters) {
    super(servlet);
    _kafkaCruiseControl = servlet.asyncKafkaCruiseControl();
    _parameters = parameters;
  }

  @Override
  protected BootstrapResult handle() {
    Long startMs = _parameters.startMs();
    Long endMs = _parameters.endMs();
    boolean clearMetrics = _parameters.clearMetrics();
    _kafkaCruiseControl.bootstrap(startMs, endMs, clearMetrics);

    return new BootstrapResult(_kafkaCruiseControl.config());
  }

  @Override
  public BootstrapParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return BootstrapRequest.class.getSimpleName();
  }
}
