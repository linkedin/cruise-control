/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.FixOfflineReplicasParameters;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.FIX_OFFLINE_REPLICAS_PARAMETER_OBJECT_CONFIG;


public class FixOfflineReplicasRequest extends AbstractAsyncRequest {
  private FixOfflineReplicasParameters _parameters;

  public FixOfflineReplicasRequest() {
    super();
  }

  @Override
  protected OperationFuture handle(String uuid) {
    return _asyncKafkaCruiseControl.fixOfflineReplicas(_parameters, uuid);
  }

  @Override
  public FixOfflineReplicasParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return FixOfflineReplicasRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (FixOfflineReplicasParameters) configs.get(FIX_OFFLINE_REPLICAS_PARAMETER_OBJECT_CONFIG);
    if (_parameters == null) {
      throw new IllegalArgumentException("Parameter configuration is missing from the request.");
    }
  }
}