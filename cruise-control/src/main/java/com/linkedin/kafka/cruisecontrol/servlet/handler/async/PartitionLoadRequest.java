/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PARTITION_LOAD_PARAMETER_OBJECT_CONFIG;


public class PartitionLoadRequest extends AbstractAsyncRequest {
  private PartitionLoadParameters _parameters;

  public PartitionLoadRequest() {
    super();
  }

  @Override
  protected OperationFuture handle(String uuid) {
    return _asyncKafkaCruiseControl.partitionLoadState(_parameters);
  }

  @Override
  public PartitionLoadParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return PartitionLoadRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (PartitionLoadParameters) configs.get(PARTITION_LOAD_PARAMETER_OBJECT_CONFIG);
    if (_parameters == null) {
      throw new IllegalArgumentException("Parameter configuration is missing from the request.");
    }
  }
}
