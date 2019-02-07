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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getAnomalyType;


public class AnomalyDetectorState {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectorState.class);
  private static final String DATA_FORMAT = "YYYY-MM-dd_HH:mm:ss z";
  private static final String TIME_ZONE = "UTC";
  private static final String DETECTION_MS = "detectionMs";
  private static final String DETECTION_DATE = "detectionDate";
  private static final String ANOMALY_ID = "anomalyId";
  private static final String STATUS = "status";
  private static final String STATUS_UPDATE_MS = "statusUpdateMs";
  private static final String STATUS_UPDATE_DATE = "statusUpdateDate";
  private static final String FIXABLE_VIOLATED_GOALS = "fixableViolatedGoals";
  private static final String UNFIXABLE_VIOLATED_GOALS = "unfixableViolatedGoals";
  private static final String FAILED_BROKERS_BY_TIME_MS = "failedBrokersByTimeMs";
  private static final String DESCRIPTION = "description";
  private static final String SELF_HEALING_ENABLED = "selfHealingEnabled";
  private static final String SELF_HEALING_DISABLED = "selfHealingDisabled";
  private static final String RECENT_GOAL_VIOLATIONS = "recentGoalViolations";
  private static final String RECENT_BROKER_FAILURES = "recentBrokerFailures";
  private static final String RECENT_METRIC_ANOMALIES = "recentMetricAnomalies";

  // Recent anomalies with anomaly state by the anomaly type.
  private final Map<AnomalyType, Map<String, AnomalyState>> _recentAnomaliesByType;
  private final Map<AnomalyType, Boolean> _selfHealingEnabled;
  // Maximum number of anomalies to keep in the anomaly detector state.
  private final int _numCachedRecentAnomalyStates;

  public AnomalyDetectorState(Map<AnomalyType, Boolean> selfHealingEnabled, int numCachedRecentAnomalyStates) {
    _numCachedRecentAnomalyStates = numCachedRecentAnomalyStates;
    _recentAnomaliesByType = new HashMap<>(AnomalyType.cachedValues().size());
    for (AnomalyType anomalyType : AnomalyType.cachedValues()) {
      _recentAnomaliesByType.put(anomalyType, new LinkedHashMap<String, AnomalyState>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, AnomalyState> eldest) {
          return this.size() > _numCachedRecentAnomalyStates;
        }
      });
    }
    _selfHealingEnabled = selfHealingEnabled;
  }

  /**
   * Add detected anomaly to the anomaly detector state.
   *
   * @param anomalyType Type of the detected anomaly.
   * @param anomaly The detected anomaly.
   */
  void addAnomalyDetection(AnomalyType anomalyType, Anomaly anomaly) {
    _recentAnomaliesByType.get(anomalyType).put(anomaly.anomalyId(), new AnomalyState(anomaly));
  }

  /**
   * Update state regarding how the anomaly has been handled.
   *
   * @param anomaly The anomaly to handle.
   * @param status A status information regarding how the anomaly was handled.
   */
  void onAnomalyHandle(Anomaly anomaly, AnomalyState.Status status) {
    AnomalyType anomalyType = getAnomalyType(anomaly);
    String anomalyId = anomaly.anomalyId();

    AnomalyState recentAnomalyState = _recentAnomaliesByType.get(anomalyType).get(anomalyId);
    if (recentAnomalyState != null) {
      recentAnomalyState.setStatus(status);
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Anomaly (type: {}, anomalyId: {}) is no longer in the anomaly detector state cache.", anomalyType, anomalyId);
    }
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

  private static void populateCommonDetails(AnomalyState anomalyState, Map<String, Object> anomalyDetails, boolean useDateFormat) {
    anomalyDetails.put(useDateFormat ? DETECTION_DATE : DETECTION_MS,
                       useDateFormat ? getDateFormat(anomalyState.detectionMs()) : anomalyState.detectionMs());
    anomalyDetails.put(STATUS, anomalyState.status());
    anomalyDetails.put(ANOMALY_ID, anomalyState.anomalyId());
    anomalyDetails.put(useDateFormat ? STATUS_UPDATE_DATE : STATUS_UPDATE_MS,
                       useDateFormat ? getDateFormat(anomalyState.statusUpdateMs()) : anomalyState.statusUpdateMs());
  }

  private Set<Map<String, Object>> recentGoalViolations(boolean useDateFormat) {
    Map<String, AnomalyState> goalViolationsByUuid = _recentAnomaliesByType.get(AnomalyType.GOAL_VIOLATION);
    Set<Map<String, Object>> recentAnomalies = new HashSet<>(_numCachedRecentAnomalyStates);
    for (Map.Entry<String, AnomalyState> entry: goalViolationsByUuid.entrySet()) {
      AnomalyState anomalyState = entry.getValue();
      GoalViolations goalViolations = (GoalViolations) anomalyState.anomaly();
      Map<Boolean, List<String>> violatedGoalsByFixability = goalViolations.violatedGoalsByFixability();
      Map<String, Object> anomalyDetails = new HashMap<>(6);
      anomalyDetails.put(FIXABLE_VIOLATED_GOALS, violatedGoalsByFixability.getOrDefault(true, Collections.emptyList()));
      anomalyDetails.put(UNFIXABLE_VIOLATED_GOALS, violatedGoalsByFixability.getOrDefault(false, Collections.emptyList()));
      populateCommonDetails(anomalyState, anomalyDetails, useDateFormat);
      recentAnomalies.add(anomalyDetails);
    }
    return recentAnomalies;
  }

  private Set<Map<String, Object>> recentBrokerFailures(boolean useDateFormat) {
    Map<String, AnomalyState> brokerFailuresByUuid = _recentAnomaliesByType.get(AnomalyType.BROKER_FAILURE);
    Set<Map<String, Object>> recentAnomalies = new HashSet<>(_numCachedRecentAnomalyStates);
    for (Map.Entry<String, AnomalyState> entry : brokerFailuresByUuid.entrySet()) {
      AnomalyState anomalyState = entry.getValue();
      Map<String, Object> anomalyDetails = new HashMap<>(5);
      anomalyDetails.put(FAILED_BROKERS_BY_TIME_MS, ((BrokerFailures) anomalyState.anomaly()).failedBrokers());
      populateCommonDetails(anomalyState, anomalyDetails, useDateFormat);
      recentAnomalies.add(anomalyDetails);
    }
    return recentAnomalies;
  }

  private Set<Map<String, Object>> recentMetricAnomalies(boolean useDateFormat) {
    Map<String, AnomalyState> metricAnomaliesByUuid = _recentAnomaliesByType.get(AnomalyType.METRIC_ANOMALY);
    Set<Map<String, Object>> recentAnomalies = new HashSet<>(_numCachedRecentAnomalyStates);
    for (Map.Entry<String, AnomalyState> entry: metricAnomaliesByUuid.entrySet()) {
      AnomalyState anomalyState = entry.getValue();
      Map<String, Object> anomalyDetails = new HashMap<>(5);
      KafkaMetricAnomaly metricAnomaly = (KafkaMetricAnomaly) anomalyState.anomaly();
      anomalyDetails.put(DESCRIPTION, metricAnomaly.description());
      populateCommonDetails(anomalyState, anomalyDetails, useDateFormat);
      recentAnomalies.add(anomalyDetails);
    }
    return recentAnomalies;
  }

  private Map<Boolean, Set<String>> getSelfHealingByEnableStatus() {
    Map<Boolean, Set<String>> selfHealingByEnableStatus = new HashMap<>(2);
    selfHealingByEnableStatus.put(true, new HashSet<>(AnomalyType.cachedValues().size()));
    selfHealingByEnableStatus.put(false, new HashSet<>(AnomalyType.cachedValues().size()));
    _selfHealingEnabled.forEach((key, value) -> {
      selfHealingByEnableStatus.get(value).add(key.name());
    });
    return selfHealingByEnableStatus;
  }

  public Map<String, Object> getJsonStructure() {
    Map<String, Object> anomalyDetectorState = new HashMap<>(_recentAnomaliesByType.size() + 2);
    Map<Boolean, Set<String>> selfHealingByEnableStatus = getSelfHealingByEnableStatus();
    anomalyDetectorState.put(SELF_HEALING_ENABLED, selfHealingByEnableStatus.get(true));
    anomalyDetectorState.put(SELF_HEALING_DISABLED, selfHealingByEnableStatus.get(false));
    anomalyDetectorState.put(RECENT_GOAL_VIOLATIONS, recentGoalViolations(false));
    anomalyDetectorState.put(RECENT_BROKER_FAILURES, recentBrokerFailures(false));
    anomalyDetectorState.put(RECENT_METRIC_ANOMALIES, recentMetricAnomalies(false));

    return anomalyDetectorState;
  }

  @Override
  public String toString() {
    Map<Boolean, Set<String>> selfHealingByEnableStatus = getSelfHealingByEnableStatus();
    return String.format("{%s:%s, %s:%s, %s:%s, %s:%s, %s:%s}%n",
                         SELF_HEALING_ENABLED, selfHealingByEnableStatus.get(true),
                         SELF_HEALING_DISABLED, selfHealingByEnableStatus.get(false),
                         RECENT_GOAL_VIOLATIONS, recentGoalViolations(true),
                         RECENT_BROKER_FAILURES, recentBrokerFailures(true),
                         RECENT_METRIC_ANOMALIES, recentMetricAnomalies(true));
  }
}
