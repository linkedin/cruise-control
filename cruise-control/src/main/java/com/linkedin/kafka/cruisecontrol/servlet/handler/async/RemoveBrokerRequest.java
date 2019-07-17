/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveBrokerParameters;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REMOVE_BROKER_PARAMETER_OBJECT_CONFIG;


public class RemoveBrokerRequest extends AbstractAsyncRequest {
  private RemoveBrokerParameters _parameters;

  public RemoveBrokerRequest() {
    super();
  }

  @Override
  protected OperationFuture handle(String uuid) {
    return _asyncKafkaCruiseControl.decommissionBrokers(_parameters, uuid);
  }

  @Override
  public RemoveBrokerParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return RemoveBrokerRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (RemoveBrokerParameters) configs.get(REMOVE_BROKER_PARAMETER_OBJECT_CONFIG);
    if (_parameters == null) {
      throw new IllegalArgumentException("Parameter configuration is missing from the request.");
    }
  }
}
