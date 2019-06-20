/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.TopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.KafkaClusterState;
import java.util.Map;
import org.apache.kafka.common.Cluster;


public class KafkaClusterStateRequest extends AbstractSyncRequest {
  private final Cluster _kafkaCluster;
  private final KafkaCruiseControlConfig _config;
  private final KafkaClusterStateParameters _parameters;
  private final TopicConfigProvider _topicConfigProvider;

  public KafkaClusterStateRequest(KafkaCruiseControlServlet servlet, KafkaClusterStateParameters parameters) {
    super(servlet);
    _kafkaCluster = servlet.asyncKafkaCruiseControl().kafkaCluster();
    _topicConfigProvider = servlet.asyncKafkaCruiseControl().topicConfigProvider();
    _config = servlet.asyncKafkaCruiseControl().config();
    _parameters = parameters;
  }

  @Override
  protected KafkaClusterState handle() {
    Map<String, Object> adminClientConfigs = KafkaCruiseControlUtils.parseAdminClientConfigs(_config);
    return new KafkaClusterState(_kafkaCluster, _topicConfigProvider, adminClientConfigs, _config);
  }

  @Override
  public KafkaClusterStateParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return KafkaClusterStateRequest.class.getSimpleName();
  }
}
