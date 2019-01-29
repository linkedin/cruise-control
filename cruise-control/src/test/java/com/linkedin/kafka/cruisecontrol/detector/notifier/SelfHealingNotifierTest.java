/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import com.linkedin.kafka.cruisecontrol.detector.GoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomaly;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for SelfHealingNotifier.
 */
public class SelfHealingNotifierTest {
  private static final boolean EXCLUDE_RECENTLY_DEMOTED_BROKERS = true;
  private static final boolean EXCLUDE_RECENTLY_REMOVED_BROKERS = true;

  @Test
  public void testOnBrokerFailure() {
    final long failureTime1 = 200L;
    final long failureTime2 = 400L;
    final long startTime = 500L;
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    Time mockTime = new MockTime(0, startTime, TimeUnit.NANOSECONDS.convert(startTime, TimeUnit.MILLISECONDS));
    TestingBrokerFailureAutoFixNotifier anomalyNotifier = new TestingBrokerFailureAutoFixNotifier(mockTime);
    anomalyNotifier.configure(Collections.singletonMap(SelfHealingNotifier.SELF_HEALING_BROKER_FAILURE_ENABLED_CONFIG, "true"));

    Map<Integer, Long> failedBrokers = new HashMap<>();
    failedBrokers.put(1, failureTime1);
    failedBrokers.put(2, failureTime2);
    boolean allowCapacityEstimation = true;
    AnomalyNotificationResult result = anomalyNotifier.onBrokerFailure(new BrokerFailures(mockKafkaCruiseControl,
                                                                                          failedBrokers,
                                                                                          allowCapacityEstimation,
                                                                                          EXCLUDE_RECENTLY_DEMOTED_BROKERS,
                                                                                          EXCLUDE_RECENTLY_REMOVED_BROKERS));
    assertEquals(AnomalyNotificationResult.Action.CHECK, result.action());
    assertEquals(SelfHealingNotifier.DEFAULT_ALERT_THRESHOLD_MS + failureTime1 - mockTime.milliseconds(),
                 result.delay());
    assertFalse(anomalyNotifier._alertCalled.get(AnomalyType.BROKER_FAILURE));

    // Sleep to 1 ms before alert.
    mockTime.sleep(result.delay() - 1);
    result = anomalyNotifier.onBrokerFailure(new BrokerFailures(mockKafkaCruiseControl,
                                                                failedBrokers,
                                                                allowCapacityEstimation,
                                                                EXCLUDE_RECENTLY_DEMOTED_BROKERS,
                                                                EXCLUDE_RECENTLY_REMOVED_BROKERS));
    assertEquals(AnomalyNotificationResult.Action.CHECK, result.action());
    assertEquals(1, result.delay());
    assertFalse(anomalyNotifier._alertCalled.get(AnomalyType.BROKER_FAILURE));

    // Sleep 1 ms
    mockTime.sleep(1);
    anomalyNotifier.resetAlert(AnomalyType.BROKER_FAILURE);
    result = anomalyNotifier.onBrokerFailure(new BrokerFailures(mockKafkaCruiseControl,
                                                                failedBrokers,
                                                                allowCapacityEstimation,
                                                                EXCLUDE_RECENTLY_DEMOTED_BROKERS,
                                                                EXCLUDE_RECENTLY_REMOVED_BROKERS));
    assertEquals(AnomalyNotificationResult.Action.CHECK, result.action());
    assertEquals(SelfHealingNotifier.DEFAULT_AUTO_FIX_THRESHOLD_MS + failureTime1 - mockTime.milliseconds(),
                 result.delay());
    assertTrue(anomalyNotifier._alertCalled.get(AnomalyType.BROKER_FAILURE));

    // Sleep to 1 ms before alert.
    mockTime.sleep(result.delay() - 1);
    anomalyNotifier.resetAlert(AnomalyType.BROKER_FAILURE);
    result = anomalyNotifier.onBrokerFailure(new BrokerFailures(mockKafkaCruiseControl,
                                                                failedBrokers,
                                                                allowCapacityEstimation,
                                                                EXCLUDE_RECENTLY_DEMOTED_BROKERS,
                                                                EXCLUDE_RECENTLY_REMOVED_BROKERS));
    assertEquals(AnomalyNotificationResult.Action.CHECK, result.action());
    assertEquals(1, result.delay());
    assertTrue(anomalyNotifier._alertCalled.get(AnomalyType.BROKER_FAILURE));
    assertFalse(anomalyNotifier._autoFixTriggered.get(AnomalyType.BROKER_FAILURE));

    // Sleep 1 ms
    mockTime.sleep(1);
    anomalyNotifier.resetAlert(AnomalyType.BROKER_FAILURE);
    result = anomalyNotifier.onBrokerFailure(new BrokerFailures(mockKafkaCruiseControl,
                                                                failedBrokers,
                                                                allowCapacityEstimation,
                                                                EXCLUDE_RECENTLY_DEMOTED_BROKERS,
                                                                EXCLUDE_RECENTLY_REMOVED_BROKERS));
    assertEquals(AnomalyNotificationResult.Action.FIX, result.action());
    assertEquals(-1L, result.delay());
    assertTrue(anomalyNotifier._alertCalled.get(AnomalyType.BROKER_FAILURE));
    assertTrue(anomalyNotifier._autoFixTriggered.get(AnomalyType.BROKER_FAILURE));
    assertFalse(anomalyNotifier._alertCalled.get(AnomalyType.GOAL_VIOLATION));
    assertFalse(anomalyNotifier._alertCalled.get(AnomalyType.METRIC_ANOMALY));
  }

  @Test
  public void testSelfHealingDisabled() {
    final long startTime = 500L;
    Time mockTime = new MockTime(startTime);
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    TestingBrokerFailureAutoFixNotifier anomalyNotifier = new TestingBrokerFailureAutoFixNotifier(mockTime);

    Map<String, String> selfHealingExplicitlyDisabled = new HashMap<>(4);
    selfHealingExplicitlyDisabled.put(SelfHealingNotifier.SELF_HEALING_BROKER_FAILURE_ENABLED_CONFIG, "false");
    selfHealingExplicitlyDisabled.put(SelfHealingNotifier.SELF_HEALING_GOAL_VIOLATION_ENABLED_CONFIG, "false");
    selfHealingExplicitlyDisabled.put(SelfHealingNotifier.SELF_HEALING_METRIC_ANOMALY_ENABLED_CONFIG, "false");
    // Set to verify the overriding of specific config over general config
    selfHealingExplicitlyDisabled.put(SelfHealingNotifier.SELF_HEALING_ENABLED_CONFIG, "true");
    anomalyNotifier.configure(selfHealingExplicitlyDisabled);

    // (1) Broker Failure
    final long failureTime1 = 200L;
    final long failureTime2 = 400L;
    Map<Integer, Long> failedBrokers = new HashMap<>();
    failedBrokers.put(1, failureTime1);
    failedBrokers.put(2, failureTime2);

    mockTime.sleep(SelfHealingNotifier.DEFAULT_AUTO_FIX_THRESHOLD_MS + failureTime1);
    anomalyNotifier.resetAlert(AnomalyType.BROKER_FAILURE);
    AnomalyNotificationResult result = anomalyNotifier.onBrokerFailure(
        new BrokerFailures(mockKafkaCruiseControl,
                           failedBrokers,
                           true,
                           EXCLUDE_RECENTLY_DEMOTED_BROKERS,
                           EXCLUDE_RECENTLY_REMOVED_BROKERS));
    assertEquals(AnomalyNotificationResult.Action.IGNORE, result.action());
    assertTrue(anomalyNotifier._alertCalled.get(AnomalyType.BROKER_FAILURE));
    assertFalse(anomalyNotifier._autoFixTriggered.get(AnomalyType.BROKER_FAILURE));

    // (2) Goal Violation
    anomalyNotifier.resetAlert(AnomalyType.GOAL_VIOLATION);
    result = anomalyNotifier.onGoalViolation(new GoalViolations(mockKafkaCruiseControl,
                                                                true,
                                                                EXCLUDE_RECENTLY_DEMOTED_BROKERS,
                                                                EXCLUDE_RECENTLY_REMOVED_BROKERS));
    assertEquals(AnomalyNotificationResult.Action.IGNORE, result.action());
    assertTrue(anomalyNotifier._alertCalled.get(AnomalyType.GOAL_VIOLATION));
    assertFalse(anomalyNotifier._autoFixTriggered.get(AnomalyType.GOAL_VIOLATION));

    // (3) Metric Anomaly
    anomalyNotifier.resetAlert(AnomalyType.METRIC_ANOMALY);
    result = anomalyNotifier.onMetricAnomaly(new KafkaMetricAnomaly(mockKafkaCruiseControl, "", null, null, null));
    assertEquals(AnomalyNotificationResult.Action.IGNORE, result.action());
    assertTrue(anomalyNotifier._alertCalled.get(AnomalyType.METRIC_ANOMALY));
    assertFalse(anomalyNotifier._autoFixTriggered.get(AnomalyType.METRIC_ANOMALY));
  }

  private static class TestingBrokerFailureAutoFixNotifier extends SelfHealingNotifier {
    final Map<AnomalyType, Boolean> _alertCalled;
    final Map<AnomalyType, Boolean> _autoFixTriggered;

    TestingBrokerFailureAutoFixNotifier(Time time) {
      super(time);
      _alertCalled = new HashMap<>(AnomalyType.cachedValues().size());
      _autoFixTriggered = new HashMap<>(AnomalyType.cachedValues().size());
      for (AnomalyType alertType : AnomalyType.cachedValues()) {
        _alertCalled.put(alertType, false);
        _autoFixTriggered.put(alertType, false);
      }
    }

    @Override
    public void alert(Object anomaly, boolean autoFixTriggered, long selfHealingStartTime, AnomalyType anomalyType) {
      _alertCalled.put(anomalyType, true);
      _autoFixTriggered.put(anomalyType, autoFixTriggered);
    }

    void resetAlert(AnomalyType anomalyType) {
      _autoFixTriggered.put(anomalyType, false);
      _alertCalled.put(anomalyType, false);
    }
  }
}
