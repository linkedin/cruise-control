/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;


public class ProposalsRequest extends AbstractAsyncRequest {
  private final ProposalsParameters _parameters;

  public ProposalsRequest(KafkaCruiseControlServlet servlet, ProposalsParameters parameters) {
    super(servlet);
    _parameters = parameters;
  }

  @Override
  protected OperationFuture handle(String uuid) {
    return _asyncKafkaCruiseControl.getProposals(_parameters);
  }

  @Override
  public ProposalsParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return ProposalsRequest.class.getSimpleName();
  }
}
