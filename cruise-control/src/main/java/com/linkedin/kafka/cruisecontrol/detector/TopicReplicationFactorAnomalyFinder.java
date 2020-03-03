/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.common.config.ConfigDef;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *   <li>{@link #TOPIC_REPLICATION_FACTOR_MARGIN_CONFIG}: The config for the topic replication factor margin over minISR, i.e.
 *   a topic is taken as unfixable if the target replication factor is less than its configured minISR value plus this margin.
 *   Default value is set to {@link #DEFAULT_TOPIC_REPLICATION_FACTOR_MARGIN}.
 *   <li>{@link #TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS_CONFIG}: The config for TTL time in millisecond of cached topic minISR
 *   records, default value is set to {@link #DEFAULT_TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS}.
 * </ul>
 */
public class TopicReplicationFactorAnomalyFinder implements TopicAnomalyFinder {
  private static final Logger LOG = LoggerFactory.getLogger(TopicAnomalyFinder.class);
  public static final String SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG = "self.healing.target.topic.replication.factor";
  public static final String TOPIC_EXCLUDED_FROM_REPLICATION_FACTOR_CHECK = "topic.excluded.from.replication.factor.check";
  public static final String DEFAULT_TOPIC_EXCLUDED_FROM_REPLICATION_FACTOR_CHECK = "";
  public static final String TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS_CONFIG = "topic.replication.topic.anomaly.class";
  public static final Class<?> DEFAULT_TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS = TopicReplicationFactorAnomaly.class;
  public static final String TOPICS_WITH_BAD_REPLICATION_FACTOR_BY_FIXABILITY_CONFIG = "topics.with.bad.replication.factor.by.fixability";
  public static final String TOPIC_REPLICATION_FACTOR_MARGIN_CONFIG = "topic.replication.factor.margin";
  public static final short DEFAULT_TOPIC_REPLICATION_FACTOR_MARGIN = 1;
  public static final String TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS_CONFIG = "topic.min.isr.record.retention.time.ms";
  public static final long DEFAULT_TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS = 12 * 60 * 60 * 1000;
  private static final long DESCRIBE_TOPIC_CONFIG_TIMEOUT_MS = 100000L;
  private KafkaCruiseControl _kafkaCruiseControl;
  private short _targetReplicationFactor;
  private Pattern _topicExcludedFromCheck;
  private Class<?> _topicReplicationTopicAnomalyClass;
  private AdminClient _adminClient;
  private short _topicReplicationFactorMargin;
  private long _topicMinISRRecordRetentionTimeMs;
  private Map<String, TopicMinISREntry> _cachedTopicMinISR;

  @Override
  public Set<TopicAnomaly> topicAnomalies() {
    LOG.info("Start to detect topic replication factor anomaly.");
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
    refreshTopicMinISRCache();
    if (!topicsWithBadReplicationFactor.isEmpty()) {
      maybeRetrieveAndCacheTopicMinISR(topicsWithBadReplicationFactor);
      Map<Boolean, Set<String>> topicsByFixability = populateTopicFixability(topicsWithBadReplicationFactor);
      return Collections.singleton(createTopicReplicationFactorAnomaly(topicsByFixability,
                                                                       _targetReplicationFactor));
    }
    return Collections.emptySet();
  }

  /**
   * Retrieve topic minISR config information if it is not cached locally.
   * @param topicsToCheck Set of topics to check.
   */
  private void maybeRetrieveAndCacheTopicMinISR(Set<String> topicsToCheck) {
    Set<ConfigResource> topicResourcesToCheck = new HashSet<>(topicsToCheck.size());
    topicsToCheck.stream().filter(t -> !_cachedTopicMinISR.containsKey(t))
                          .forEach(t -> topicResourcesToCheck.add(new ConfigResource(ConfigResource.Type.TOPIC, t)));
    if (topicResourcesToCheck.isEmpty()) {
      return;
    }
    for (Map.Entry<ConfigResource, KafkaFuture<Config>> entry : _adminClient.describeConfigs(topicResourcesToCheck).values().entrySet()) {
      try {
        short topicMinISR = Short.parseShort(entry.getValue().get(DESCRIBE_TOPIC_CONFIG_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                                                  .get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value());
        _cachedTopicMinISR.put(entry.getKey().name(), new TopicMinISREntry(topicMinISR, System.currentTimeMillis()));
      } catch (TimeoutException | InterruptedException | ExecutionException e) {
        LOG.warn("Skip attempt to fix replication factor of topic {} due to unable to retrieve its minISR config.",
                 entry.getKey().name());
      }
    }
  }

  /**
   * Scan through topics with bad replication factor to check whether the topic is fixable or not.
   * One topic is fixable if the target replication factor is no less than the topic's minISR config plus topic replication
   * factor margin.
   *
   * @param topicsWithBadReplicationFactor Set of topics with bad replication factor.
   * @return Topics with bad replication factor by fixability.
   */
  private Map<Boolean, Set<String>> populateTopicFixability(Set<String> topicsWithBadReplicationFactor) {
    Map<Boolean, Set<String>> topicsByFixability = new HashMap<>(2);
    for (String topic : topicsWithBadReplicationFactor) {
      if (_cachedTopicMinISR.containsKey(topic)) {
        short topicMinISR = _cachedTopicMinISR.get(topic).minISR();
        if (_targetReplicationFactor < topicMinISR + _topicReplicationFactorMargin) {
          topicsByFixability.putIfAbsent(false, new HashSet<>());
          topicsByFixability.get(false).add(topic);
        } else {
          topicsByFixability.putIfAbsent(true, new HashSet<>());
          topicsByFixability.get(true).add(topic);
        }
      }
    }
    return topicsByFixability;
  }

  /**
   * Invalidate stale topic minISR record from local cache.
   */
  private void refreshTopicMinISRCache() {
    long currentTimeMs = System.currentTimeMillis();
    Iterator<Map.Entry<String, TopicMinISREntry>> cacheIterator = _cachedTopicMinISR.entrySet().iterator();
    while (cacheIterator.hasNext()) {
      Map.Entry<String, TopicMinISREntry> entry = cacheIterator.next();
      if (entry.getValue().createTimeMs() + _topicMinISRRecordRetentionTimeMs < currentTimeMs) {
        cacheIterator.remove();
      } else {
        break;
      }
    }
  }

  private TopicAnomaly createTopicReplicationFactorAnomaly(Map<Boolean, Set<String>> topicsByFixability,
                                                           short targetReplicationFactor) {
    Map<String, Object> configs = new HashMap<>(4);
    configs.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl);
    configs.put(TOPICS_WITH_BAD_REPLICATION_FACTOR_BY_FIXABILITY_CONFIG, topicsByFixability);
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
    try {
      _targetReplicationFactor = Short.parseShort((String) configs.get(SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG));
      if (_targetReplicationFactor <= 0) {
        throw new IllegalArgumentException(String.format("%s config of replication factor anomaly finder should be set to positive,"
            + " provided %d.", SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG, _targetReplicationFactor));
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG +
                                         " is missing or misconfigured for topic replication factor anomaly finder.");
    }
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
    try {
      _topicReplicationFactorMargin = Short.parseShort((String) configs.get(TOPIC_REPLICATION_FACTOR_MARGIN_CONFIG));
      if (_topicReplicationFactorMargin < 0) {
        throw new IllegalArgumentException(String.format("%s config of replication factor anomaly finder should not be set to negative,"
            + " provided %d.", TOPIC_REPLICATION_FACTOR_MARGIN_CONFIG, _topicReplicationFactorMargin));
      }
    } catch (NumberFormatException e) {
      _topicReplicationFactorMargin = DEFAULT_TOPIC_REPLICATION_FACTOR_MARGIN;
    }
    try {
      _topicMinISRRecordRetentionTimeMs = Long.parseLong((String) configs.get(TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS_CONFIG));
      if (_topicMinISRRecordRetentionTimeMs <= 0) {
        throw new IllegalArgumentException(String.format("%s config of replication factor anomaly finder should be set to positive,"
            + " provided %d.", TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS_CONFIG, _topicMinISRRecordRetentionTimeMs));
      }
    } catch (NumberFormatException e) {
      _topicMinISRRecordRetentionTimeMs = DEFAULT_TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS;
    }

    KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
    _adminClient = KafkaCruiseControlUtils.createAdminClient(KafkaCruiseControlUtils.parseAdminClientConfigs(config));
    _cachedTopicMinISR = new LinkedHashMap<>();
  }

  /**
   * A class to encapsulate the retrieved topic minISR config information.
   */
  private static class TopicMinISREntry {
    private final short _minISR;
    private final long _createTimeMs;

    TopicMinISREntry(short minISR, long createTimeMs) {
      _minISR = minISR;
      _createTimeMs = createTimeMs;
    }

    short minISR() {
      return _minISR;
    }

    long createTimeMs() {
      return _createTimeMs;
    }
  }
}
