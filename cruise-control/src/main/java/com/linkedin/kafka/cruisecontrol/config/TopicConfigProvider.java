/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import java.util.Map;
import java.util.Properties;


public interface TopicConfigProvider extends CruiseControlConfigurable, AutoCloseable {

  /**
   * Get cluster-level configs that applies to a topic if no topic-level config exist for it.
   */
  Properties clusterConfigs();

  /**
   * Get topic-level configs for the requested topic.
   * @param topic Topic for which the topic-level configs are requested.
   */
  Properties topicConfigs(String topic);

  /**
   * Get topic-level configs for all topics.
   */
  Map<String, Properties> allTopicConfigs();
}
