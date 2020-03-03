/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.UpdateTopicConfigurationRunnable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.buildTopicRegex;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.extractKafkaCruiseControlObjectFromConfig;
import static com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder.SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder.TOPICS_WITH_BAD_REPLICATION_FACTOR_BY_FIXABILITY_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.TOPIC_ANOMALY;


/**
 * Topics which contain at least one partition with replication factor not equal to the config value of
 * {@link com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder#SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG}
 */
public class TopicReplicationFactorAnomaly extends TopicAnomaly {
  protected Short _targetReplicationFactor;
  protected Map<Boolean, Set<String>> _topicsWithBadReplicationFactorByFixability;
  protected UpdateTopicConfigurationRunnable _updateTopicConfigurationRunnable;

  @Override
  public boolean fix() throws Exception {
    if (_updateTopicConfigurationRunnable == null) {
      return false;
    }
    _optimizationResult = _updateTopicConfigurationRunnable.getResult();
    boolean hasProposalsToFix = hasProposalsToFix();
    // Ensure that only the relevant response is cached to avoid memory pressure.
    _optimizationResult.discardIrrelevantAndCacheJsonAndPlaintext();
    return hasProposalsToFix;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    KafkaCruiseControl kafkaCruiseControl = extractKafkaCruiseControlObjectFromConfig(configs, KafkaAnomalyType.TOPIC_ANOMALY);
    KafkaCruiseControlConfig config = kafkaCruiseControl.config();
    _topicsWithBadReplicationFactorByFixability = (Map<Boolean, Set<String>>) configs.get(TOPICS_WITH_BAD_REPLICATION_FACTOR_BY_FIXABILITY_CONFIG);
    if (_topicsWithBadReplicationFactorByFixability == null || _topicsWithBadReplicationFactorByFixability.isEmpty()) {
      throw new IllegalArgumentException(String.format("Missing %s for topic replication factor anomaly.",
                                                       TOPICS_WITH_BAD_REPLICATION_FACTOR_BY_FIXABILITY_CONFIG));
    }
    _targetReplicationFactor = (Short) configs.get(SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG);
    if (_targetReplicationFactor == null) {
      throw new IllegalArgumentException(String.format("Missing %s for topic replication factor anomaly.",
                                                       SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG));
    }
    boolean allowCapacityEstimation = config.getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
    boolean excludeRecentlyDemotedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
    boolean excludeRecentlyRemovedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
    if (_topicsWithBadReplicationFactorByFixability.get(true) != null && !_topicsWithBadReplicationFactorByFixability.get(true).isEmpty()) {
      Pattern topicRegex = buildTopicRegex(_topicsWithBadReplicationFactorByFixability.get(true));
      _updateTopicConfigurationRunnable = new UpdateTopicConfigurationRunnable(kafkaCruiseControl,
                                                                               Collections.singletonMap(_targetReplicationFactor,
                                                                                                        topicRegex),
                                                                               getSelfHealingGoalNames(config),
                                                                               allowCapacityEstimation,
                                                                               excludeRecentlyDemotedBrokers,
                                                                               excludeRecentlyRemovedBrokers,
                                                                               _anomalyId.toString(),
                                                                               reasonSupplier());
    }
  }

  @Override
  public Supplier<String> reasonSupplier() {
    return () -> String.format("Self healing for %s: %s", TOPIC_ANOMALY, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{Detected following topics which have at least one partition with replication factor other than ")
      .append(_targetReplicationFactor)
      .append(" : {fixable : [");
    StringJoiner joiner = new StringJoiner(",");
    _topicsWithBadReplicationFactorByFixability.getOrDefault(true, Collections.emptySet()).forEach(joiner::add);
    sb.append(joiner.toString());
    joiner = new StringJoiner(",");
    _topicsWithBadReplicationFactorByFixability.getOrDefault(false, Collections.emptySet()).forEach(joiner::add);
    sb.append(joiner.toString());
    sb.append("]}}");
    return sb.toString();
  }
}
