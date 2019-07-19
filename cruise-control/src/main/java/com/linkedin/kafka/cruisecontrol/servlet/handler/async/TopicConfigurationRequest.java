/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicConfigurationParameters;


public class TopicConfigurationRequest extends AbstractAsyncRequest {
  private final TopicConfigurationParameters _parameters;

  public TopicConfigurationRequest(KafkaCruiseControlServlet servlet, TopicConfigurationParameters parameters) {
    super(servlet);
    _parameters = parameters;
  }

  @Override
  protected OperationFuture handle(String uuid) {
    return _asyncKafkaCruiseControl.updateTopicConfiguration(_parameters, uuid);
  }

  @Override
  public TopicConfigurationParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return TopicConfigurationRequest.class.getSimpleName();
  }
}
