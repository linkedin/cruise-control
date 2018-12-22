/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;


public class AnomalyDetectorState {
  private static final String DATA_FORMAT = "YYYY-MM-dd_HH:mm:ss z";
  private static final String TIME_ZONE = "UTC";
  private static final String DETECTION_MS = "detectionMs";
  private static final String DETECTION_DATE = "detectionDate";
  private static final String FIXABLE_VIOLATED_GOALS = "fixableViolatedGoals";
  private static final String UNFIXABLE_VIOLATED_GOALS = "unfixableViolatedGoals";
  private static final String FAILED_BROKERS_BY_TIME_MS = "failedBrokersByTimeMs";
  private static final String DESCRIPTION = "description";
  private static final String SELF_HEALING_ENABLED = "selfHealingEnabled";
  private static final String SELF_HEALING_DISABLED = "selfHealingDisabled";
  private static final String RECENT_GOAL_VIOLATIONS = "recentGoalViolations";
  private static final String RECENT_BROKER_FAILURES = "recentBrokerFailures";
  private static final String RECENT_METRIC_ANOMALIES = "recentMetricAnomalies";
  // Recent anomalies with record time by the anomaly type.
  private final Map<AnomalyType, Map<Long, Anomaly>> _recentAnomaliesByType;
  private final Map<AnomalyType, Boolean> _selfHealingEnabled;
  // Maximum number of anomalies to keep in the anomaly detector state.
  private final int _numCachedRecentAnomalyStates;

  public AnomalyDetectorState(Map<AnomalyType, Boolean> selfHealingEnabled, int numCachedRecentAnomalyStates) {
    _numCachedRecentAnomalyStates = numCachedRecentAnomalyStates;
    _recentAnomaliesByType = new HashMap<>(AnomalyType.cachedValues().size());
    for (AnomalyType anomalyType : AnomalyType.cachedValues()) {
      _recentAnomaliesByType.put(anomalyType, new LinkedHashMap<Long, Anomaly>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, Anomaly> eldest) {
          return this.size() > _numCachedRecentAnomalyStates;
        }
      });
    }
    _selfHealingEnabled = selfHealingEnabled;
  }

  public void addAnomaly(AnomalyType anomalyType, Anomaly anomaly) {
    _recentAnomaliesByType.get(anomalyType).put(System.currentTimeMillis(), anomaly);
  }

  public synchronized Map<AnomalyType, Boolean> selfHealingEnabled() {
    return _selfHealingEnabled;
  }

  public synchronized boolean setSelfHealingFor(AnomalyType anomalyType, boolean isSelfHealingEnabled) {
    Boolean oldValue = _selfHealingEnabled.put(anomalyType, isSelfHealingEnabled);
    return oldValue != null && oldValue;
  }

  private static String getDateFormat(long timeMs) {
    Date date = new Date(timeMs);
    DateFormat formatter = new SimpleDateFormat(DATA_FORMAT);
    formatter.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
    return formatter.format(date);
  }

  private Set<Map<String, Object>> recentGoalViolations(boolean useDateFormat) {
    Map<Long, Anomaly> goalViolationsByTime = _recentAnomaliesByType.get(AnomalyType.GOAL_VIOLATION);
    Set<Map<String, Object>> recentAnomalies = new HashSet<>(_numCachedRecentAnomalyStates);
    for (Map.Entry<Long, Anomaly> entry: goalViolationsByTime.entrySet()) {
      GoalViolations goalViolations = (GoalViolations) entry.getValue();
      Map<Boolean, List<String>> violatedGoalsByFixability = goalViolations.violatedGoalsByFixability();
      Map<String, Object> anomalyDetails = new HashMap<>(3);
      anomalyDetails.put(useDateFormat ? DETECTION_DATE : DETECTION_MS,
          useDateFormat ? getDateFormat(entry.getKey()) : entry.getKey());
      anomalyDetails.put(FIXABLE_VIOLATED_GOALS, violatedGoalsByFixability.getOrDefault(true, Collections.emptyList()));
      anomalyDetails.put(UNFIXABLE_VIOLATED_GOALS, violatedGoalsByFixability.getOrDefault(false, Collections.emptyList()));
      recentAnomalies.add(anomalyDetails);
    }
    return recentAnomalies;
  }

  private Set<Map<String, Object>> recentBrokerFailures(boolean useDateFormat) {
    Map<Long, Anomaly> brokerFailuresByTime = _recentAnomaliesByType.get(AnomalyType.BROKER_FAILURE);
    Set<Map<String, Object>> recentAnomalies = new HashSet<>(_numCachedRecentAnomalyStates);
    for (Map.Entry<Long, Anomaly> entry : brokerFailuresByTime.entrySet()) {
      Map<String, Object> anomalyDetails = new HashMap<>(2);
      anomalyDetails.put(useDateFormat ? DETECTION_DATE : DETECTION_MS,
          useDateFormat ? getDateFormat(entry.getKey()) : entry.getKey());
      anomalyDetails.put(FAILED_BROKERS_BY_TIME_MS, ((BrokerFailures) entry.getValue()).failedBrokers());
      recentAnomalies.add(anomalyDetails);
    }
    return recentAnomalies;
  }

  private Set<Map<String, Object>> recentMetricAnomalies(boolean useDateFormat) {
    Map<Long, Anomaly> metricAnomaliesByTime = _recentAnomaliesByType.get(AnomalyType.METRIC_ANOMALY);
    Set<Map<String, Object>> recentAnomalies = new HashSet<>(_numCachedRecentAnomalyStates);
    for (Map.Entry<Long, Anomaly> entry: metricAnomaliesByTime.entrySet()) {
      Map<String, Object> anomalyDetails = new HashMap<>(2);
      KafkaMetricAnomaly metricAnomaly = (KafkaMetricAnomaly) entry.getValue();
      anomalyDetails.put(useDateFormat ? DETECTION_DATE : DETECTION_MS,
          useDateFormat ? getDateFormat(entry.getKey()) : entry.getKey());
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
    anomalyDetectorState.put(RECENT_GOAL_VIOLATIONS, recentGoalViolations(false));
    anomalyDetectorState.put(RECENT_BROKER_FAILURES, recentBrokerFailures(false));
    anomalyDetectorState.put(RECENT_METRIC_ANOMALIES, recentMetricAnomalies(false));

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

    return String.format("{%s:%s, %s:%s, %s:%s, %s:%s, %s:%s}%n",
                         SELF_HEALING_ENABLED, selfHealingEnabled,
                         SELF_HEALING_DISABLED, selfHealingDisabled,
                         RECENT_GOAL_VIOLATIONS, recentGoalViolations(true),
                         RECENT_BROKER_FAILURES, recentBrokerFailures(true),
                         RECENT_METRIC_ANOMALIES, recentMetricAnomalies(true));
  }
}
