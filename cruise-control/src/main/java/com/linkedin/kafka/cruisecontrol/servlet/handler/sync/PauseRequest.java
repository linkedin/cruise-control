/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PauseResumeParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.PauseSamplingResult;


public class PauseRequest extends AbstractSyncRequest {
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final PauseResumeParameters _parameters;

  public PauseRequest(KafkaCruiseControlServlet servlet, PauseResumeParameters parameters) {
    super(servlet);
    _kafkaCruiseControl = servlet.asyncKafkaCruiseControl();
    _parameters = parameters;
  }

  @Override
  protected PauseSamplingResult handle() {
    _kafkaCruiseControl.pauseMetricSampling(_parameters.reason());
    return new PauseSamplingResult(_kafkaCruiseControl.config());
  }

  @Override
  public PauseResumeParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return PauseRequest.class.getSimpleName();
  }
}
