/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PROPOSALS_PARAMETER_OBJECT_CONFIG;


public class ProposalsRequest extends AbstractAsyncRequest {
  private ProposalsParameters _parameters;

  public ProposalsRequest() {
    super();
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

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (ProposalsParameters) configs.get(PROPOSALS_PARAMETER_OBJECT_CONFIG);
    if (_parameters == null) {
      throw new IllegalArgumentException("Parameter configuration is missing from the request.");
    }
  }
}
