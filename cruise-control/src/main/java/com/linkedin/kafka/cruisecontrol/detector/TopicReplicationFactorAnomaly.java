/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.UpdateTopicConfigurationRunnable;
import java.util.HashMap;
import java.util.HashSet;
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
import static com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder.TOPICS_WITH_BAD_REPLICATION_FACTOR_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.TOPIC_ANOMALY;


/**
 * Topics which have at least one partition detected to violate replication factor requirement. For more detail about detection
 * process and criteria , check {@link com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder}.
 */
public class TopicReplicationFactorAnomaly extends TopicAnomaly {
  protected Set<TopicReplicationFactorAnomalyEntry> _topicsWithBadReplicationFactor;
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
    _topicsWithBadReplicationFactor = (Set<TopicReplicationFactorAnomalyEntry>) configs.get(TOPICS_WITH_BAD_REPLICATION_FACTOR_CONFIG);
    if (_topicsWithBadReplicationFactor == null || _topicsWithBadReplicationFactor.isEmpty()) {
      throw new IllegalArgumentException(String.format("Missing %s for topic replication factor anomaly.",
                                                       TOPICS_WITH_BAD_REPLICATION_FACTOR_CONFIG));
    }
    boolean allowCapacityEstimation = config.getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
    boolean excludeRecentlyDemotedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
    boolean excludeRecentlyRemovedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
    Map<Short, Pattern> topicPatternByReplicationFactor = populateTopicPatternByReplicationFactor(_topicsWithBadReplicationFactor);
    _updateTopicConfigurationRunnable = new UpdateTopicConfigurationRunnable(kafkaCruiseControl,
                                                                             topicPatternByReplicationFactor,
                                                                             getSelfHealingGoalNames(config),
                                                                             allowCapacityEstimation,
                                                                             excludeRecentlyDemotedBrokers,
                                                                             excludeRecentlyRemovedBrokers,
                                                                             _anomalyId.toString(),
                                                                             reasonSupplier());
  }

  protected Map<Short, Pattern> populateTopicPatternByReplicationFactor(Set<TopicReplicationFactorAnomalyEntry> topicsWithBadReplicationFactor) {
    Map<Short, Set<String>> topicsByReplicationFactor = new HashMap<>();
    for (TopicReplicationFactorAnomalyEntry entry : topicsWithBadReplicationFactor) {
      topicsByReplicationFactor.putIfAbsent(entry.getTargetReplicationFactor(), new HashSet<>());
      topicsByReplicationFactor.get(entry.getTargetReplicationFactor()).add(entry.getTopicName());
    }
    Map<Short, Pattern> topicPatternByReplicationFactor = new HashMap<>(topicsByReplicationFactor.size());
    topicsByReplicationFactor.forEach((k, v) -> topicPatternByReplicationFactor.put(k, buildTopicRegex(v)));
    return topicPatternByReplicationFactor;
  }

  @Override
  public Supplier<String> reasonSupplier() {
    return () -> String.format("Self healing for %s: %s", TOPIC_ANOMALY, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{Detected following topics having replication factor violations: [{");
    StringJoiner joiner = new StringJoiner("}, {");
    _topicsWithBadReplicationFactor.forEach(e -> joiner.add(e.toString()));
    sb.append(joiner.toString())
      .append("}]}");
    return sb.toString();
  }

  public static class TopicReplicationFactorAnomalyEntry {
    private final String _topicName;
    private final int _partitionCount;
    private final int _violatedPartitionCount;
    private final short _targetReplicationFactor;
    private final short _minIsrConfig;

    public TopicReplicationFactorAnomalyEntry(String topicName, int partitionCount, int violatedPartitionCount,
                                              short targetReplicationFactor, short minIsrConfig) {
      _topicName = topicName;
      _partitionCount = partitionCount;
      _violatedPartitionCount = violatedPartitionCount;
      _targetReplicationFactor = targetReplicationFactor;
      _minIsrConfig = minIsrConfig;
    }

    public String getTopicName() {
      return _topicName;
    }

    public short getTargetReplicationFactor() {
      return _targetReplicationFactor;
    }

    @Override
    public String toString() {
      return String.format("Topic: %s, total partition count %d, violated partition count %d, target replication factor %d, minISR: %d",
                           _topicName, _partitionCount, _violatedPartitionCount, _targetReplicationFactor, _minIsrConfig);
    }
  }
}
