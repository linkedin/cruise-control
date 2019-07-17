/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TrainParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.TrainResult;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.TRAIN_PARAMETER_OBJECT_CONFIG;


public class TrainRequest extends AbstractSyncRequest {
  private KafkaCruiseControl _kafkaCruiseControl;
  private TrainParameters _parameters;

  public TrainRequest() {
    super();
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

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _kafkaCruiseControl = _servlet.asyncKafkaCruiseControl();
    _parameters = (TrainParameters) configs.get(TRAIN_PARAMETER_OBJECT_CONFIG);
    if (_parameters == null) {
      throw new IllegalArgumentException("Parameter configuration is missing from the request.");
    }
  }
}
