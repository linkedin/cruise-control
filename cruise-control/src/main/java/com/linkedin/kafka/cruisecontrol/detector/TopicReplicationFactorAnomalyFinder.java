/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.common.config.ConfigDef;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import static com.linkedin.cruisecontrol.common.config.ConfigDef.Type.CLASS;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfigUtils.getConfiguredInstance;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;


/**
 * The class will check whether there are topics having partition(s) with replication factor different than the desired value.
 * Required configurations for this class.
 * <ul>
 *   <li>{@link #SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG}: The config for the target replication factor of topics.
 *   <li>{@link #TOPIC_EXCLUDED_FROM_REPLICATION_FACTOR_CHECK}: The config to specify topics excluded from the anomaly checking.
 *   The value is treated as a regular expression, default value is set to
 *   {@link #DEFAULT_TOPIC_EXCLUDED_FROM_REPLICATION_FACTOR_CHECK}.
 *   <li>{@link #TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS_CONFIG}: The config for the topic anomaly class name,
 *   default value is set to {@link #DEFAULT_TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS}.
 * </ul>
 */
public class TopicReplicationFactorAnomalyFinder implements TopicAnomalyFinder {
  public static final String SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG = "self.healing.target.topic.replication.factor";
  public static final String TOPIC_EXCLUDED_FROM_REPLICATION_FACTOR_CHECK = "topic.excluded.from.replication.factor.check";
  public static final String DEFAULT_TOPIC_EXCLUDED_FROM_REPLICATION_FACTOR_CHECK = "";
  public static final String TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS_CONFIG = "topic.replication.topic.anomaly.class";
  public static final Class<?> DEFAULT_TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS = TopicReplicationFactorAnomaly.class;
  public static final String TOPICS_WITH_BAD_REPLICATION_FACTOR_CONFIG = "topics.with.bad.replication.factor";
  private KafkaCruiseControl _kafkaCruiseControl;
  private int _targetReplicationFactor;
  private Pattern _topicExcludedFromCheck;
  private Class<?> _topicReplicationTopicAnomalyClass;

  @Override
  public Set<TopicAnomaly> topicAnomalies() {
    Cluster cluster = _kafkaCruiseControl.kafkaCluster();
    Set<String> topicsWithBadReplicationFactor = new HashSet<>();
    for (String topic : cluster.topics()) {
      if (_topicExcludedFromCheck.matcher(topic).matches()) {
        continue;
      }
      for (PartitionInfo partition : cluster.partitionsForTopic(topic)) {
        if (partition.replicas().length != _targetReplicationFactor) {
          topicsWithBadReplicationFactor.add(topic);
          break;
        }
      }
    }
    if (!topicsWithBadReplicationFactor.isEmpty()) {
      return Collections.singleton(createTopicReplicationFactorAnomaly(topicsWithBadReplicationFactor,
                                                                       _targetReplicationFactor));
    }
    return Collections.emptySet();
  }

  private TopicAnomaly createTopicReplicationFactorAnomaly(Set<String> topicsWithBadReplicationFactor,
                                                           int targetReplicationFactor) {
    Map<String, Object> configs = new HashMap<>(4);
    configs.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl);
    configs.put(TOPICS_WITH_BAD_REPLICATION_FACTOR_CONFIG, topicsWithBadReplicationFactor);
    configs.put(SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG, targetReplicationFactor);
    configs.put(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, _kafkaCruiseControl.timeMs());
    return getConfiguredInstance(_topicReplicationTopicAnomalyClass, TopicAnomaly.class, configs);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    _kafkaCruiseControl = (KafkaCruiseControl) configs.get(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
    if (_kafkaCruiseControl == null) {
      throw new IllegalArgumentException("Topic replication factor anomaly finder is missing " + KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
    }
    Short targetReplicationFactor = (Short) configs.get(SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG);
    if (targetReplicationFactor == null) {
      throw new IllegalArgumentException("Topic replication factor anomaly finder is missing " + SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG);
    }
    _targetReplicationFactor = targetReplicationFactor;
    String topicExcludedFromCheck = (String) configs.get(TOPIC_EXCLUDED_FROM_REPLICATION_FACTOR_CHECK);
    _topicExcludedFromCheck = Pattern.compile(topicExcludedFromCheck == null ? DEFAULT_TOPIC_EXCLUDED_FROM_REPLICATION_FACTOR_CHECK
                                                                             : topicExcludedFromCheck);
    String topicReplicationTopicAnomalyClass = (String) configs.get(TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS_CONFIG);
    if (topicReplicationTopicAnomalyClass == null) {
      _topicReplicationTopicAnomalyClass = DEFAULT_TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS;
    } else {
      _topicReplicationTopicAnomalyClass = (Class<?>) ConfigDef.parseType(TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS_CONFIG,
                                                                          topicReplicationTopicAnomalyClass,
                                                                          CLASS);
      if (_topicReplicationTopicAnomalyClass == null || !TopicAnomaly.class.isAssignableFrom(_topicReplicationTopicAnomalyClass)) {
          throw new IllegalArgumentException(String.format("Invalid %s is provided to replication factor anomaly finder, provided %s",
              TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS_CONFIG, _topicReplicationTopicAnomalyClass));
      }
    }
  }
}
