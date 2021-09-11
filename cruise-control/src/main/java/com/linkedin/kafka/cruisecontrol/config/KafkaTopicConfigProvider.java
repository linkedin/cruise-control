/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import kafka.server.ConfigType;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import scala.collection.JavaConverters;


/**
 * The Kafka topic config provider implementation based on files. The format of the file is JSON, listing properties:
 * <pre>
 *   {
 *     "min.insync.replicas": 1,
 *     "an.example.cluster.config": false
 *   }
 * </pre>
 *
 * @deprecated This class uses the Zookeeper based admin client that will be removed in Kafka 3.0. Therefore this class has been
 * deprecated and will be removed in a future Cruise Control release. A new {@link TopicConfigProvider} implementation
 * using the Kafka Admin Client has been created ({@link KafkaAdminTopicConfigProvider}) and can be set using the
 * {@code topic.config.provider.class} configuration setting.
 *
 */
@Deprecated
public class KafkaTopicConfigProvider extends JsonFileTopicConfigProvider {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicConfigProvider.class);
  public static final String ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_GROUP = "KafkaTopicConfigProvider";
  public static final String ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_TYPE = "GetAllActiveTopicConfigs";
  protected String _connectString;
  protected boolean _zkSecurityEnabled;
  protected Properties _clusterConfigs;

  @Override
  public Properties clusterConfigs() {
    return _clusterConfigs;
  }

  @Override
  public Properties topicConfigs(String topic) {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(
      _connectString,
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
  public Map<String, Properties> topicConfigs(Set<String> topics) {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(_connectString,
                                                                              ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_GROUP,
                                                                              ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_TYPE,
                                                                              _zkSecurityEnabled);
    Map<String, Properties> topicConfigs = new HashMap<>();
    try {
      AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);

      for (String topic : topics) {
        try {
          Properties topicConfig = adminZkClient.fetchEntityConfig(ConfigType.Topic(), topic);
          topicConfigs.put(topic, topicConfig);
        } catch (Exception e) {
          LOG.warn("Unable to retrieve config for topic '{}'", topic, e);
        }
      }
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }

    return topicConfigs;
  }

  @Override
  public Map<String, Properties> allTopicConfigs() {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(_connectString,
                                                                              ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_GROUP,
                                                                              ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_TYPE,
                                                                              _zkSecurityEnabled);
    try {
      AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);
      return JavaConverters.mapAsJavaMap(adminZkClient.getAllTopicConfigs());
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Override
  public void configure(Map<String, ?> configs) {
    _connectString = (String) configs.get(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG);
    _zkSecurityEnabled = (Boolean) configs.get(ExecutorConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG);
    _clusterConfigs = loadClusterConfigs(configs);
  }

  @Override
  public void close() {
    // nothing to do.
  }
}
