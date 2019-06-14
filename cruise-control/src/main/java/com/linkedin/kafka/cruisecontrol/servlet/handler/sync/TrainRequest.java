/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TrainParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.TrainResult;


public class TrainRequest extends AbstractSyncRequest {
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final TrainParameters _parameters;

  public TrainRequest(KafkaCruiseControlServlet servlet, TrainParameters parameters) {
    super(servlet);
    _kafkaCruiseControl = servlet.asyncKafkaCruiseControl();
    _parameters = parameters;
  }

  @Override
  protected TrainResult handle() {
    _kafkaCruiseControl.train(_parameters.startMs(), _parameters.endMs());
    return new TrainResult(_kafkaCruiseControl.config());
  }

  @Override
  public TrainParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return TrainRequest.class.getSimpleName();
  }
}
