/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PauseResumeParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.ResumeSamplingResult;


public class ResumeRequest extends AbstractSyncRequest {
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final PauseResumeParameters _parameters;

  public ResumeRequest(KafkaCruiseControlServlet servlet, PauseResumeParameters parameters) {
    super(servlet);
    _kafkaCruiseControl = servlet.asyncKafkaCruiseControl();
    _parameters = parameters;
  }

  @Override
  protected ResumeSamplingResult handle() {
    _kafkaCruiseControl.resumeMetricSampling(_parameters.reason());
    return new ResumeSamplingResult(_kafkaCruiseControl.config());
  }

  @Override
  public PauseResumeParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return ResumeRequest.class.getSimpleName();
  }
}
