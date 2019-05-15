/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UpdateTopicConfigurationParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.UpdateTopicConfigurationResult;
import java.util.regex.Pattern;

/**
 * The async runnable for {@link KafkaCruiseControl#updateTopicConfiguration(Pattern, int, boolean, String)}
 */
public class UpdateTopicConfigurationRunnable extends OperationRunnable {
  private final Pattern _topic;
  private final int _replicationFactor;
  private final boolean _skipRackAwarenessCheck;
  private final String _uuid;
  private final KafkaCruiseControlConfig _config;

  UpdateTopicConfigurationRunnable(KafkaCruiseControl kafkaCruiseControl,
                                   OperationFuture future,
                                   String uuid,
                                   UpdateTopicConfigurationParameters parameters,
                                   KafkaCruiseControlConfig config) {
    super(kafkaCruiseControl, future);
    _uuid = uuid;
    _topic = parameters.topic();
    _replicationFactor = parameters.replicationFactor();
    _skipRackAwarenessCheck = parameters.skipRackAwarenessCheck();
    _config = config;
  }

  @Override
  protected UpdateTopicConfigurationResult getResult() {
    return new UpdateTopicConfigurationResult(_kafkaCruiseControl.updateTopicConfiguration(_topic,
                                                                                           _replicationFactor,
                                                                                           _skipRackAwarenessCheck,
                                                                                           _uuid),
                                              _replicationFactor,
                                              _config);
  }
}
