/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
public class KafkaAdminTopicConfigProvider implements TopicConfigProvider {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminTopicConfigProvider.class);

  public static final String CLUSTER_CONFIGS_FILE = "cluster.configs.file";
  private static Properties _clusterConfigs;
  private AdminClient _adminClient;
  private long _adminTimeoutMs;

  @Override
  public Properties clusterConfigs() {
    return _clusterConfigs;
  }

  @Override
  public Properties topicConfigs(String topic) {
    Config topicConfig = null;
    ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    try {
      LOG.debug("Requesting details for topic '{}'", topic);
      topicConfig = _adminClient
              .describeConfigs(Collections.singletonList(topicResource))
              .all()
              .get(_adminTimeoutMs, TimeUnit.MILLISECONDS)
              .get(topicResource);
    } catch (InterruptedException e) {
      LOG.error("The request for the configuration of topic '{}' was interrupted", topic);
      e.printStackTrace();
    } catch (ExecutionException e) {
      LOG.error("The request for the configuration of topic '{}' failed", topic);
      e.printStackTrace();
    } catch (TimeoutException e) {
      LOG.error("The request for the configuration of topic '{}' timed out", topic);
      e.printStackTrace();
    }

    if (topicConfig != null) {
      return convertTopicConfigToProperties(topicConfig);
    } else {
      LOG.error("The configuration for topic '{}' could not be retrieved", topic);
      return new Properties();
    }
  }

  @Override
  public Map<String, Properties> allTopicConfigs() {
    Map<ConfigResource, Config> topicConfigs = null;
    try {
      LOG.debug("Requesting configurations for all topics");
      topicConfigs = _adminClient
              .listTopics()
              .names()
              .thenApply(
                      topicNameSet -> _adminClient.describeConfigs(
                              topicNameSet.stream().map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name)).collect(Collectors.toList())
                      ).all()
              )
              .get(_adminTimeoutMs, TimeUnit.MILLISECONDS)
              .get(_adminTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.error("The request for the configuration of all topics was interrupted");
      e.printStackTrace();
    } catch (ExecutionException e) {
      LOG.error("The request for the configuration of all topics failed");
      e.printStackTrace();
    } catch (TimeoutException e) {
      LOG.error("The request for the configuration of all topics timed out");
      e.printStackTrace();
    }

    Map<String, Properties> propsMap = new HashMap<>();
    if (topicConfigs != null) {
      LOG.debug("Converting {} Topic Configs into Properties", topicConfigs.size());
      for (Map.Entry<ConfigResource, Config> entry : topicConfigs.entrySet()) {
        propsMap.put(entry.getKey().name(), convertTopicConfigToProperties(entry.getValue()));
      }
      LOG.debug("Topic Config conversion complete");
    } else {
      LOG.error("Topic configurations for all topics on the cluster could not be retrieved");
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

    KafkaCruiseControlConfig ccConfig = new KafkaCruiseControlConfig(configs);
    _adminTimeoutMs = ccConfig.getConfiguredInstance(ExecutorConfig.ADMIN_CLIENT_REQUEST_TIMEOUT_MS_CONFIG, Integer.class);
    _adminClient = KafkaCruiseControlUtils.createAdminClient(KafkaCruiseControlUtils.parseAdminClientConfigs(ccConfig));

    String configFile = KafkaCruiseControlUtils.getRequiredConfig(configs, CLUSTER_CONFIGS_FILE);
    try {
      loadClusterConfigs(configFile);
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void close() {
    _adminClient.close(Duration.ofMillis(KafkaCruiseControlUtils.ADMIN_CLIENT_CLOSE_TIMEOUT_MS));
  }
}
