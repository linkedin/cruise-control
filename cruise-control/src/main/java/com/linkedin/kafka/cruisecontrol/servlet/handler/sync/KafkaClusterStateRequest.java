/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.TopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.KafkaClusterState;
import java.util.Map;
import org.apache.kafka.common.Cluster;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.KAFKA_CLUSTER_STATE_PARAMETER_OBJECT_CONFIG;


public class KafkaClusterStateRequest extends AbstractSyncRequest {
  protected Cluster _kafkaCluster;
  protected KafkaCruiseControlConfig _config;
  protected KafkaClusterStateParameters _parameters;
  protected TopicConfigProvider _topicConfigProvider;

  public KafkaClusterStateRequest() {
    super();
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

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _kafkaCluster = _servlet.asyncKafkaCruiseControl().kafkaCluster();
    _topicConfigProvider = _servlet.asyncKafkaCruiseControl().topicConfigProvider();
    _config = _servlet.asyncKafkaCruiseControl().config();
    _parameters = (KafkaClusterStateParameters) configs.get(KAFKA_CLUSTER_STATE_PARAMETER_OBJECT_CONFIG);
    if (_parameters == null) {
      throw new IllegalArgumentException("Parameter configuration is missing from the request.");
    }
  }
}
