/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REBALANCE_PARAMETER_OBJECT_CONFIG;


public class RebalanceRequest extends AbstractAsyncRequest {
  protected RebalanceParameters _parameters;

  public RebalanceRequest() {
    super();
  }

  @Override
  protected OperationFuture handle(String uuid) {
    return _asyncKafkaCruiseControl.rebalance(_parameters, uuid);
  }

  @Override
  public RebalanceParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return RebalanceRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (RebalanceParameters) configs.get(REBALANCE_PARAMETER_OBJECT_CONFIG);
    if (_parameters == null) {
      throw new IllegalArgumentException("Parameter configuration is missing from the request.");
    }
  }
}
