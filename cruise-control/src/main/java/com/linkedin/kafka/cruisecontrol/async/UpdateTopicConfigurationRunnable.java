/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicConfigurationParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.TopicConfigurationResult;
import java.util.regex.Pattern;

/**
 * The async runnable for {@link KafkaCruiseControl#updateTopicConfiguration(Pattern, short, boolean, String)}
 */
public class UpdateTopicConfigurationRunnable extends OperationRunnable {
  private final Pattern _topic;
  private final short _replicationFactor;
  private final boolean _skipRackAwarenessCheck;
  private final String _uuid;
  private final KafkaCruiseControlConfig _config;

  UpdateTopicConfigurationRunnable(KafkaCruiseControl kafkaCruiseControl,
                                   OperationFuture future,
                                   String uuid,
                                   TopicConfigurationParameters parameters,
                                   KafkaCruiseControlConfig config) {
    super(kafkaCruiseControl, future);
    _uuid = uuid;
    _topic = parameters.topic();
    _replicationFactor = parameters.replicationFactor();
    _skipRackAwarenessCheck = parameters.skipRackAwarenessCheck();
    _config = config;
  }

  @Override
  protected TopicConfigurationResult getResult() {
    return new TopicConfigurationResult(_kafkaCruiseControl.updateTopicConfiguration(_topic,
                                                                                     _replicationFactor,
                                                                                     _skipRackAwarenessCheck,
                                                                                     _uuid),
                                        _replicationFactor,
                                        _config);
  }
}
