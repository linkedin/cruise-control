/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.StopProposalParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.StopProposalResult;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.STOP_PROPOSAL_PARAMETER_OBJECT_CONFIG;


public class StopProposalRequest extends AbstractSyncRequest {
  protected KafkaCruiseControl _kafkaCruiseControl;
  protected StopProposalParameters _parameters;

  public StopProposalRequest() {
    super();
  }

  @Override
  protected StopProposalResult handle() {
    _kafkaCruiseControl.userTriggeredStopExecution(_parameters.forceExecutionStop());
    return new StopProposalResult(_kafkaCruiseControl.config());
  }

  @Override
  public StopProposalParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return StopProposalRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _kafkaCruiseControl = _servlet.asyncKafkaCruiseControl();
    _parameters = (StopProposalParameters) configs.get(STOP_PROPOSAL_PARAMETER_OBJECT_CONFIG);
    if (_parameters == null) {
      throw new IllegalArgumentException("Parameter configuration is missing from the request.");
    }
  }
}
