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
import static com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType.GOAL_VIOLATION;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType.METRIC_ANOMALY;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType.BROKER_FAILURE;

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
  private static final String FAILED_DISKS_BY_TIME_MS = "failedDisksByTimeMs";
  private static final String DESCRIPTION = "description";
  private static final String SELF_HEALING_ENABLED = "selfHealingEnabled";
  private static final String SELF_HEALING_DISABLED = "selfHealingDisabled";
  private static final String SELF_HEALING_ENABLED_RATIO = "selfHealingEnabledRatio";
  private static final String RECENT_GOAL_VIOLATIONS = "recentGoalViolations";
  private static final String RECENT_BROKER_FAILURES = "recentBrokerFailures";
  private static final String RECENT_METRIC_ANOMALIES = "recentMetricAnomalies";
  private static final String RECENT_DISK_FAILURES = "recentDiskFailures";
  private static final String ONGOING_SELF_HEALING_ANOMALY = "ongoingSelfHealingAnomaly";
  private static final String OPTIMIZATION_RESULT = "optimizationResult";

  // Recent anomalies with anomaly state by the anomaly type.
  private final Map<AnomalyType, Map<String, AnomalyState>> _recentAnomaliesByType;
  private Anomaly _ongoingSelfHealingAnomaly;
  private final Map<AnomalyType, Boolean> _selfHealingEnabled;
  private Map<String, Float> _selfHealingEnabledRatio;
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
    _selfHealingEnabledRatio = null;
    _ongoingSelfHealingAnomaly = null;
  }

  public void setSelfHealingEnabledRatio(Map<AnomalyType, Float> selfHealingEnabledRatio) {
    if (selfHealingEnabledRatio == null) {
      throw new IllegalArgumentException("Attempt to set selfHealingEnabledRatio with null.");
    }
    _selfHealingEnabledRatio = new HashMap<>(selfHealingEnabledRatio.size());
    selfHealingEnabledRatio.forEach((key, value) -> _selfHealingEnabledRatio.put(key.name(), value));
  }

  private Map<String, Float> selfHealingEnabledRatio() {
    return _selfHealingEnabledRatio == null ? Collections.emptyMap() : _selfHealingEnabledRatio;
  }

  /**
   * Update anomaly status once associated self-healing operation has finished.
   *
   * @param anomalyId Unique id of anomaly which triggered self-healing operation.
   */
  public synchronized void markSelfHealingFinished(String anomalyId) {
    if (_ongoingSelfHealingAnomaly == null || !_ongoingSelfHealingAnomaly.anomalyId().equals(anomalyId)) {
      throw new IllegalStateException(String.format("Anomaly %s is not marked as %s state in AnomalyDetector.",
                                                    anomalyId, AnomalyState.Status.FIX_STARTED));
    }
    _ongoingSelfHealingAnomaly = null;
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
  synchronized void onAnomalyHandle(Anomaly anomaly, AnomalyState.Status status) {
    AnomalyType anomalyType = getAnomalyType(anomaly);
    String anomalyId = anomaly.anomalyId();

    if (status == AnomalyState.Status.FIX_STARTED) {
      _ongoingSelfHealingAnomaly = anomaly;
    }

    AnomalyState recentAnomalyState = _recentAnomaliesByType.get(anomalyType).get(anomalyId);
    if (recentAnomalyState != null) {
      recentAnomalyState.setStatus(status);
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Anomaly (type: {}, anomalyId: {}) is no longer in the anomaly detector state cache.", anomalyType, anomalyId);
    }
  }

  public synchronized void setSelfHealingFor(AnomalyType anomalyType, boolean isSelfHealingEnabled) {
    _selfHealingEnabled.put(anomalyType, isSelfHealingEnabled);
  }

  private static String getDateFormat(long timeMs) {
    Date date = new Date(timeMs);
    DateFormat formatter = new SimpleDateFormat(DATA_FORMAT);
    formatter.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
    return formatter.format(date);
  }

  private static Map<String, Object> populateAnomalyDetails(AnomalyState anomalyState,
                                                            AnomalyType anomalyType,
                                                            boolean hasFixStarted,
                                                            boolean isJson) {
    // Goal violation has one more field than other anomaly types.
    Map<String, Object> anomalyDetails = new HashMap<>((hasFixStarted ? 6 : 5) + (anomalyType == GOAL_VIOLATION ? 1 : 0));
    anomalyDetails.put(isJson ? DETECTION_MS : DETECTION_DATE,
                       isJson ? anomalyState.detectionMs() : getDateFormat(anomalyState.detectionMs()));
    anomalyDetails.put(STATUS, anomalyState.status());
    anomalyDetails.put(ANOMALY_ID, anomalyState.anomalyId());
    anomalyDetails.put(isJson ? STATUS_UPDATE_MS : STATUS_UPDATE_DATE,
                       isJson ? anomalyState.statusUpdateMs() : getDateFormat(anomalyState.statusUpdateMs()));
    switch (anomalyType) {
      case GOAL_VIOLATION:
        GoalViolations goalViolations = (GoalViolations) anomalyState.anomaly();
        Map<Boolean, List<String>> violatedGoalsByFixability = goalViolations.violatedGoalsByFixability();
        anomalyDetails.put(FIXABLE_VIOLATED_GOALS, violatedGoalsByFixability.getOrDefault(true, Collections.emptyList()));
        anomalyDetails.put(UNFIXABLE_VIOLATED_GOALS, violatedGoalsByFixability.getOrDefault(false, Collections.emptyList()));
        if (hasFixStarted) {
          anomalyDetails.put(OPTIMIZATION_RESULT, goalViolations.optimizationResult(isJson));
        }
        break;
      case BROKER_FAILURE:
        BrokerFailures brokerFailures = (BrokerFailures) anomalyState.anomaly();
        anomalyDetails.put(FAILED_BROKERS_BY_TIME_MS, brokerFailures.failedBrokers());
        if (hasFixStarted) {
          anomalyDetails.put(OPTIMIZATION_RESULT, brokerFailures.optimizationResult(isJson));
        }
        break;
      case DISK_FAILURE:
        DiskFailures diskFailures = (DiskFailures) anomalyState.anomaly();
        anomalyDetails.put(FAILED_DISKS_BY_TIME_MS, diskFailures.failedDisks());
        if (hasFixStarted) {
          anomalyDetails.put(OPTIMIZATION_RESULT, diskFailures.optimizationResult(isJson));
        }
        break;
      case METRIC_ANOMALY:
        KafkaMetricAnomaly metricAnomaly = (KafkaMetricAnomaly) anomalyState.anomaly();
        anomalyDetails.put(DESCRIPTION, metricAnomaly.description());
        if (hasFixStarted) {
          anomalyDetails.put(OPTIMIZATION_RESULT, metricAnomaly.optimizationResult(isJson));
        }
        break;
      default:
        throw new IllegalStateException("Unrecognized anomaly type " + anomalyType);
    }
    return anomalyDetails;
  }

  private Set<Map<String, Object>> recentAnomalies(AnomalyType anomalyType, boolean isJson) {
    Map<String, AnomalyState> anomaliesById = _recentAnomaliesByType.get(anomalyType);
    Set<Map<String, Object>> recentAnomalies = new HashSet<>(_numCachedRecentAnomalyStates);
    for (Map.Entry<String, AnomalyState> entry: anomaliesById.entrySet()) {
      recentAnomalies.add(populateAnomalyDetails(entry.getValue(), anomalyType, false, isJson));
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

  public synchronized Map<String, Object> getJsonStructure() {
    Map<String, Object> anomalyDetectorState = new HashMap<>(_recentAnomaliesByType.size() + (_ongoingSelfHealingAnomaly == null ? 3 : 4));
    Map<Boolean, Set<String>> selfHealingByEnableStatus = getSelfHealingByEnableStatus();
    anomalyDetectorState.put(SELF_HEALING_ENABLED, selfHealingByEnableStatus.get(true));
    anomalyDetectorState.put(SELF_HEALING_DISABLED, selfHealingByEnableStatus.get(false));
    anomalyDetectorState.put(SELF_HEALING_ENABLED_RATIO, selfHealingEnabledRatio());
    anomalyDetectorState.put(RECENT_GOAL_VIOLATIONS, recentAnomalies(GOAL_VIOLATION, true));
    anomalyDetectorState.put(RECENT_BROKER_FAILURES, recentAnomalies(BROKER_FAILURE, true));
    anomalyDetectorState.put(RECENT_METRIC_ANOMALIES, recentAnomalies(METRIC_ANOMALY, true));
    anomalyDetectorState.put(RECENT_DISK_FAILURES, recentAnomalies(AnomalyType.DISK_FAILURE, true));
    if (_ongoingSelfHealingAnomaly != null) {
      anomalyDetectorState.put(ONGOING_SELF_HEALING_ANOMALY, _ongoingSelfHealingAnomaly.anomalyId());
    }
    return anomalyDetectorState;
  }

  @Override
  public synchronized String toString() {
    Map<Boolean, Set<String>> selfHealingByEnableStatus = getSelfHealingByEnableStatus();
    return String.format("{%s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%s}%n",
                         SELF_HEALING_ENABLED, selfHealingByEnableStatus.get(true),
                         SELF_HEALING_DISABLED, selfHealingByEnableStatus.get(false),
                         SELF_HEALING_ENABLED_RATIO, selfHealingEnabledRatio(),
                         RECENT_GOAL_VIOLATIONS, recentAnomalies(GOAL_VIOLATION, false),
                         RECENT_BROKER_FAILURES, recentAnomalies(BROKER_FAILURE, false),
                         RECENT_METRIC_ANOMALIES, recentAnomalies(METRIC_ANOMALY, false),
                         RECENT_DISK_FAILURES, recentAnomalies(AnomalyType.DISK_FAILURE, false),
                         ONGOING_SELF_HEALING_ANOMALY, _ongoingSelfHealingAnomaly == null ? "None" : _ongoingSelfHealingAnomaly.anomalyId());
  }
}
