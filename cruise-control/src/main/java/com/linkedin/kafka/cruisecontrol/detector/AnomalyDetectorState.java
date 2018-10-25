/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class AnomalyDetectorState {
  // Maximum number of anomalies to keep in the anomaly detector state.
  private final static int NUM_RECENT_ANOMALIES = 10;
  private static final String START_MS = "startMs";
  private static final String VIOLATED_GOALS = "violatedGoals";
  private static final String FAILED_BROKERS = "failedBrokers";
  private static final String DESCRIPTION = "description";
  private static final String SELF_HEALING_ENABLED = "selfHealingEnabled";
  private static final String SELF_HEALING_DISABLED = "selfHealingDisabled";
  private static final String RECENT_GOAL_VIOLATIONS = "recentGoalViolations";
  private static final String RECENT_BROKER_FAILURES = "recentBrokerFailures";
  private static final String RECENT_METRIC_ANOMALIES = "recentMetricAnomalies";
  // Recent anomalies with record time by the anomaly type.
  private final Map<AnomalyType, LinkedHashMap<Long, Anomaly>> _recentAnomaliesByType;
  private final Map<AnomalyType, Boolean> _selfHealingEnabled;

  public AnomalyDetectorState(Map<AnomalyType, Boolean> selfHealingEnabled) {
    _recentAnomaliesByType = new HashMap<>(AnomalyType.cachedValues().size());
    for (AnomalyType anomalyType : AnomalyType.cachedValues()) {
      _recentAnomaliesByType.put(anomalyType, new LinkedHashMap<Long, Anomaly>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, Anomaly> eldest) {
          return this.size() > NUM_RECENT_ANOMALIES;
        }
      });
    }
    _selfHealingEnabled = selfHealingEnabled;
  }

  public void addAnomaly(AnomalyType anomalyType, Anomaly anomaly) {
    _recentAnomaliesByType.get(anomalyType).put(System.currentTimeMillis(), anomaly);
  }

  public Map<AnomalyType, Boolean> selfHealingEnabled() {
    return Collections.unmodifiableMap(_selfHealingEnabled);
  }

  public void setSelfHealingFor(AnomalyType anomalyType, boolean isSelfHealingEnabled) {
    _selfHealingEnabled.put(anomalyType, isSelfHealingEnabled);
  }

  private Set<Map<String, Object>> recentGoalViolations() {
    LinkedHashMap<Long, Anomaly> goalViolationsByTime = _recentAnomaliesByType.get(AnomalyType.GOAL_VIOLATION);
    Set<Map<String, Object>> recentAnomalies = new HashSet<>(NUM_RECENT_ANOMALIES);
    for (Map.Entry<Long, Anomaly> entry: goalViolationsByTime.entrySet()) {
      GoalViolations goalViolations = (GoalViolations) entry.getValue();
      Set<String> violatedGoals = goalViolations.violations().stream().map(GoalViolations.Violation::goalName)
                                                .collect(Collectors.toSet());
      Map<String, Object> anomalyDetails = new HashMap<>(2);
      anomalyDetails.put(START_MS, entry.getKey());
      anomalyDetails.put(VIOLATED_GOALS, violatedGoals);
      recentAnomalies.add(anomalyDetails);
    }
    return recentAnomalies;
  }

  private Set<Map<String, Object>> recentBrokerFailures() {
    LinkedHashMap<Long, Anomaly> brokerFailuresByTime = _recentAnomaliesByType.get(AnomalyType.BROKER_FAILURE);
    Set<Map<String, Object>> recentAnomalies = new HashSet<>(NUM_RECENT_ANOMALIES);
    for (Map.Entry<Long, Anomaly> entry : brokerFailuresByTime.entrySet()) {
      Map<String, Object> anomalyDetails = new HashMap<>(2);
      anomalyDetails.put(START_MS, entry.getKey());
      anomalyDetails.put(FAILED_BROKERS, ((BrokerFailures) entry.getValue()).failedBrokers());
      recentAnomalies.add(anomalyDetails);
    }
    return recentAnomalies;
  }

  private Set<Map<String, Object>> recentMetricAnomalies() {
    LinkedHashMap<Long, Anomaly> metricAnomaliesByTime = _recentAnomaliesByType.get(AnomalyType.METRIC_ANOMALY);
    Set<Map<String, Object>> recentAnomalies = new HashSet<>(NUM_RECENT_ANOMALIES);
    for (Map.Entry<Long, Anomaly> entry: metricAnomaliesByTime.entrySet()) {
      Map<String, Object> anomalyDetails = new HashMap<>(2);
      KafkaMetricAnomaly metricAnomaly = (KafkaMetricAnomaly) entry.getValue();
      anomalyDetails.put(START_MS, entry.getKey());
      anomalyDetails.put(DESCRIPTION, metricAnomaly.description());
      recentAnomalies.add(anomalyDetails);
    }
    return recentAnomalies;
  }

  public Map<String, Object> getJsonStructure() {
    Map<String, Object> anomalyDetectorState = new HashMap<>(_recentAnomaliesByType.size() + 2);
    Set<String> selfHealingEnabled = new HashSet<>(AnomalyType.cachedValues().size());
    Set<String> selfHealingDisabled = new HashSet<>(AnomalyType.cachedValues().size());
    _selfHealingEnabled.forEach((key, value) -> {
      if (value) {
        selfHealingEnabled.add(key.name());
      } else {
        selfHealingDisabled.add(key.name());
      }
    });
    anomalyDetectorState.put(SELF_HEALING_ENABLED, selfHealingEnabled);
    anomalyDetectorState.put(SELF_HEALING_DISABLED, selfHealingDisabled);
    anomalyDetectorState.put(RECENT_GOAL_VIOLATIONS, recentGoalViolations());
    anomalyDetectorState.put(RECENT_BROKER_FAILURES, recentBrokerFailures());
    anomalyDetectorState.put(RECENT_METRIC_ANOMALIES, recentMetricAnomalies());

    return anomalyDetectorState;
  }

  @Override
  public String toString() {
    Set<String> selfHealingEnabled = new HashSet<>(AnomalyType.cachedValues().size());
    Set<String> selfHealingDisabled = new HashSet<>(AnomalyType.cachedValues().size());
    _selfHealingEnabled.forEach((key, value) -> {
      if (value) {
        selfHealingEnabled.add(key.name());
      } else {
        selfHealingDisabled.add(key.name());
      }
    });

    return String.format("%s:%s%n%s:%s%n%s:%s%n%s:%s%n%s:%s%n",
                         SELF_HEALING_ENABLED, selfHealingEnabled,
                         SELF_HEALING_DISABLED, selfHealingDisabled,
                         RECENT_GOAL_VIOLATIONS, recentGoalViolations(),
                         RECENT_BROKER_FAILURES, recentBrokerFailures(),
                         RECENT_METRIC_ANOMALIES, recentMetricAnomalies());
  }
}
