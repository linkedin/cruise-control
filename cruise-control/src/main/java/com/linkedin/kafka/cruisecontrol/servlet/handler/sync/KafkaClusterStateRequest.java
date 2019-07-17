/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.KafkaClusterState;
import java.util.Map;
import org.apache.kafka.common.Cluster;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.KAFKA_CLUSTER_STATE_PARAMETER_OBJECT_CONFIG;


public class KafkaClusterStateRequest extends AbstractSyncRequest {
  private Cluster _kafkaCluster;
  private KafkaCruiseControlConfig _config;
  private KafkaClusterStateParameters _parameters;

  public KafkaClusterStateRequest() {
    super();
  }

  @Override
  protected KafkaClusterState handle() {
    return new KafkaClusterState(_kafkaCluster, _config);
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
    _config = _servlet.asyncKafkaCruiseControl().config();
    _parameters = (KafkaClusterStateParameters) configs.get(KAFKA_CLUSTER_STATE_PARAMETER_OBJECT_CONFIG);
    if (_parameters == null) {
      throw new IllegalArgumentException("Parameter configuration is missing from the request.");
    }
  }
}
