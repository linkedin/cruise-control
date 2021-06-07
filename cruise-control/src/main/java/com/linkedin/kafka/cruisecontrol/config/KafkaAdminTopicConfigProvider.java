/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


/**
 * The Kafka topic config provider implementation based on using the Kafka Admin Client for topic level configurations
 * and files for cluster level configurations. The format of the file is JSON, listing properties:
 * <pre>
 *   {
 *     "min.insync.replicas": 1,
 *     "an.example.cluster.config": false
 *   }
 * </pre>
 *
 */
public class KafkaAdminTopicConfigProvider extends JsonFileTopicConfigProvider {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminTopicConfigProvider.class);

  private Properties _clusterConfigs;
  private AdminClient _adminClient;

  @Override
  public Properties clusterConfigs() {
    return _clusterConfigs;
  }

  /**
   * Fetches the configuration for the requested topic. If an error is encountered the details will be logged and an
   * empty Properties instance will be returned.
   *
   * @param topic Topic name for which the topic-level configurations are required.
   * @return Properties instance containing the topic configuration.
   */
  @Override
  public Properties topicConfigs(String topic) {
    Config topicConfig = null;
    ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    try {
      LOG.debug("Requesting details for topic '{}'", topic);
      topicConfig = _adminClient
              .describeConfigs(Collections.singletonList(topicResource))
              .all()
              .get()
              .get(topicResource);
    } catch (ExecutionException ee) {
      if (org.apache.kafka.common.errors.TimeoutException.class == ee.getCause().getClass()) {
        LOG.warn("Failed to retrieve configuration for topic '{}' due to describeConfigs request time out. Check for Kafka-side issues"
                + " and consider increasing the configured timeout.", topic);
      } else {
        // e.g. could be UnknownTopicOrPartitionException due to topic deletion or InvalidTopicException
        LOG.warn("Cannot retrieve configuration for topic '{}'.", topic, ee);
      }
    } catch (InterruptedException ie) {
      LOG.debug("Interrupted while getting configuration for topic '{}'.", topic, ie);
    }

    if (topicConfig != null) {
      return convertTopicConfigToProperties(topicConfig);
    } else {
      LOG.warn("The configuration for topic '{}' could not be retrieved, returning empty Properties instance.", topic);
      return new Properties();
    }
  }

  /**
   * Fetches the configuration for all the topics on the Kafka cluster. If an error is encountered when retrieving the
   * topic names then the error details will be logged and an empty Map instance will be returned.
   *
   * @return A Map from topic name string to Properties instance containing that topic's configuration.
   */
  @Override
  public Map<String, Properties> allTopicConfigs() {

    // Request a map of futures for the config of each topic on the Kafka cluster
    LOG.debug("Requesting configurations for all topics");
    Set<String> topicNames = null;
    try {
      topicNames = _adminClient.listTopics().names().get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.warn("Unable to obtain list of all topic names from the Kafka Cluster");
    }

    if (topicNames == null) {
      return new HashMap<>();
    } else {
      return topicConfigs(topicNames);
    }
  }

  /**
   * Fetches the configuration for the requested topics. If an error is encountered, for each topic, the details will be
   * logged and the entry for that topic will be omitted from the returned map.
   *
   * @param topics The set of topic names for which the topic-level configurations are required.
   * @return A Map from topic name string to Properties instance containing that topic's configuration.
   */
  @Override
  public Map<String, Properties> topicConfigs(Set<String> topics) {

    Map<ConfigResource, KafkaFuture<Config>> topicConfigs;
    topicConfigs = _adminClient.describeConfigs(
            topics.stream().map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name)).collect(Collectors.toList())
    ).values();

    Map<String, Properties> propsMap = new HashMap<>();
    if (topicConfigs != null) {
      for (Map.Entry<ConfigResource, KafkaFuture<Config>> entry : topicConfigs.entrySet()) {
        try {
          Config config = entry.getValue().get();
          propsMap.put(entry.getKey().name(), convertTopicConfigToProperties(config));
        } catch (ExecutionException ee) {
          if (org.apache.kafka.common.errors.TimeoutException.class == ee.getCause().getClass()) {
            LOG.warn("Failed to retrieve config for topic '{}' due to describeConfigs request time out. Check for Kafka-side issues"
                            + " and consider increasing the configured timeout.", entry.getKey().name());
          } else {
            // e.g. could be UnknownTopicOrPartitionException due to topic deletion or InvalidTopicException
            LOG.debug("Cannot retrieve config for topic {}.", entry.getKey().name(), ee);
          }
        } catch (InterruptedException ie) {
          LOG.debug("Interrupted while getting config for topic {}.", entry.getKey().name(), ie);
        }
      }
    }

    return propsMap;
  }

  private static Properties convertTopicConfigToProperties(Config config) {
    Properties props = new Properties();
    for (ConfigEntry entry : config.entries()) {
      props.put(entry.name(), entry.value());
    }
    return props;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    _adminClient = (AdminClient) validateNotNull(
            configs.get(LoadMonitor.KAFKA_ADMIN_CLIENT_OBJECT_CONFIG),
            () -> String.format("Missing %s when creating Kafka Admin Client based Topic Config Provider",
                    LoadMonitor.KAFKA_ADMIN_CLIENT_OBJECT_CONFIG));
    _clusterConfigs = loadClusterConfigs(configs, CLUSTER_CONFIGS_FILE);
  }

  @Override
  public void close() {
    //no-op
  }
}
