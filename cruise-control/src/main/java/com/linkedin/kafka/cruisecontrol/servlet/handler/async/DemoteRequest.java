/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEMOTE_BROKER_PARAMETER_OBJECT_CONFIG;


public class DemoteRequest extends AbstractAsyncRequest {
  protected DemoteBrokerParameters _parameters;

  public DemoteRequest() {
    super();
  }

  @Override
  protected OperationFuture handle(String uuid) {
    return _asyncKafkaCruiseControl.demoteBrokers(uuid, _parameters);
  }

  @Override
  public DemoteBrokerParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return DemoteRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (DemoteBrokerParameters) configs.get(DEMOTE_BROKER_PARAMETER_OBJECT_CONFIG);
    if (_parameters == null) {
      throw new IllegalArgumentException("Parameter configuration is missing from the request.");
    }
  }
}
