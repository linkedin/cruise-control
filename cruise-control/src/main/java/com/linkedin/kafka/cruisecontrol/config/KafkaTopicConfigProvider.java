/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import kafka.server.ConfigType;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import scala.jdk.javaapi.CollectionConverters;

/**
 * The Kafka topic config provider implementation based on files. The format of the file is JSON, listing properties:
 * <pre>
 *   {
 *     "min.insync.replicas": 1,
 *     "an.example.cluster.config": false
 *   }
 * </pre>
 *
 */
public class KafkaTopicConfigProvider implements TopicConfigProvider {
  public static final String CLUSTER_CONFIGS_FILE = "cluster.configs.file";
  public static final String ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_GROUP = "KafkaTopicConfigProvider";
  public static final String ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_TYPE = "GetAllActiveTopicConfigs";
  private String _connectString;
  private boolean _zkSecurityEnabled;
  private static Properties _clusterConfigs;

  @Override
  public Properties clusterConfigs() {
    return _clusterConfigs;
  }

  @Override
  public Properties topicConfigs(String topic) {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(_connectString,
                                                                              ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_GROUP,
                                                                              ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_TYPE,
                                                                              _zkSecurityEnabled);
    try {
      AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);
      return adminZkClient.fetchEntityConfig(ConfigType.Topic(), topic);
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Override
  public Map<String, Properties> allTopicConfigs() {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(_connectString,
                                                                              ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_GROUP,
                                                                              ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_TYPE,
                                                                              _zkSecurityEnabled);
    try {
      AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);
      return CollectionConverters.asJava(adminZkClient.getAllTopicConfigs());
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  private void loadClusterConfigs(String clusterConfigsFile) throws FileNotFoundException {
    JsonReader reader = new JsonReader(new InputStreamReader(new FileInputStream(clusterConfigsFile), StandardCharsets.UTF_8));
    try {
      Gson gson = new Gson();
      _clusterConfigs = gson.fromJson(reader, Properties.class);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        // let it go.
      }
    }
  }

  @Override
  public void configure(Map<String, ?> configs) {
    _connectString = (String) configs.get(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG);
    _zkSecurityEnabled = (Boolean) configs.get(ExecutorConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG);
    String configFile = KafkaCruiseControlUtils.getRequiredConfig(configs, CLUSTER_CONFIGS_FILE);
    try {
      loadClusterConfigs(configFile);
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void close() {
    // nothing to do.
  }
}
