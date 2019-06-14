/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.StopProposalParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.StopProposalResult;


public class StopProposalRequest extends AbstractSyncRequest {
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final StopProposalParameters _parameters;


  public StopProposalRequest(KafkaCruiseControlServlet servlet, StopProposalParameters parameters) {
    super(servlet);
    _kafkaCruiseControl = servlet.asyncKafkaCruiseControl();
    _parameters = parameters;
  }

  @Override
  protected StopProposalResult handle() {
    _kafkaCruiseControl.userTriggeredStopExecution();
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
}
