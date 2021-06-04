/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import kafka.server.ConfigType;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import scala.collection.JavaConversions;
import java.util.Map;
import java.util.Properties;


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
  public static final String ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_GROUP = "KafkaTopicConfigProvider";
  public static final String ZK_KAFKA_TOPIC_CONFIG_PROVIDER_METRIC_TYPE = "GetAllActiveTopicConfigs";
  private String _connectString;
  private boolean _zkSecurityEnabled;
  private Properties _clusterConfigs;

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
      return JavaConversions.mapAsJavaMap(adminZkClient.getAllTopicConfigs());
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Override
  public void configure(Map<String, ?> configs) {
    _connectString = (String) configs.get(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG);
    _zkSecurityEnabled = (Boolean) configs.get(ExecutorConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG);
    _clusterConfigs = loadClusterConfigs(configs, CLUSTER_CONFIGS_FILE);
  }

  @Override
  public void close() {
    // nothing to do.
  }
}
