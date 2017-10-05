/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for SelfHealingNotifier.
 */
public class SelfHealingNotifierTest {

  @Test
  public void testOnBrokerFailure() {
    final long failureTime1 = 200L;
    final long failureTime2 = 400L;
    final long startTime = 500L;
    Time mockTime = new MockTime(0, startTime, TimeUnit.NANOSECONDS.convert(startTime, TimeUnit.MILLISECONDS));
    TestingBrokerFailureAutoFixNotifier anomalyNotifier = new TestingBrokerFailureAutoFixNotifier(mockTime);
    anomalyNotifier.configure(Collections.emptyMap());

    Map<Integer, Long> failedBrokers = new HashMap<>();
    failedBrokers.put(1, failureTime1);
    failedBrokers.put(2, failureTime2);
    AnomalyNotificationResult result = anomalyNotifier.onBrokerFailure(new BrokerFailures(failedBrokers));
    assertEquals(AnomalyNotificationResult.Action.CHECK, result.action());
    assertEquals(SelfHealingNotifier.DEFAULT_ALERT_THRESHOLD_MS + failureTime1 - mockTime.milliseconds(),
                 result.delay());
    assertFalse(anomalyNotifier.alertCalled);

    // Sleep to 1 ms before alert.
    mockTime.sleep(result.delay() - 1);
    result = anomalyNotifier.onBrokerFailure(new BrokerFailures(failedBrokers));
    assertEquals(AnomalyNotificationResult.Action.CHECK, result.action());
    assertEquals(1, result.delay());
    assertFalse(anomalyNotifier.alertCalled);

    // Sleep 1 ms
    mockTime.sleep(1);
    anomalyNotifier.resetAlert();
    result = anomalyNotifier.onBrokerFailure(new BrokerFailures(failedBrokers));
    assertEquals(AnomalyNotificationResult.Action.CHECK, result.action());
    assertEquals(SelfHealingNotifier.DEFAULT_AUTO_FIX_THRESHOLD_MS + failureTime1 - mockTime.milliseconds(),
                 result.delay());
    assertTrue(anomalyNotifier.alertCalled);

    // Sleep to 1 ms before alert.
    mockTime.sleep(result.delay() - 1);
    anomalyNotifier.resetAlert();
    result = anomalyNotifier.onBrokerFailure(new BrokerFailures(failedBrokers));
    assertEquals(AnomalyNotificationResult.Action.CHECK, result.action());
    assertEquals(1, result.delay());
    assertTrue(anomalyNotifier.alertCalled);
    assertFalse(anomalyNotifier.autoFixTriggered);

    // Sleep 1 ms
    mockTime.sleep(1);
    anomalyNotifier.resetAlert();
    result = anomalyNotifier.onBrokerFailure(new BrokerFailures(failedBrokers));
    assertEquals(AnomalyNotificationResult.Action.FIX, result.action());
    assertEquals(-1L, result.delay());
    assertTrue(anomalyNotifier.alertCalled);
    assertTrue(anomalyNotifier.autoFixTriggered);
  }

  @Test
  public void testSelfHealingDisabled() {
    final long failureTime1 = 200L;
    final long failureTime2 = 400L;
    final long startTime = 500L;
    Time mockTime = new MockTime(startTime);
    TestingBrokerFailureAutoFixNotifier anomalyNotifier = new TestingBrokerFailureAutoFixNotifier(mockTime);
    anomalyNotifier.configure(Collections.singletonMap(SelfHealingNotifier.SELF_HEALING_ENABLED_CONFIG, "false"));

    Map<Integer, Long> failedBrokers = new HashMap<>();
    failedBrokers.put(1, failureTime1);
    failedBrokers.put(2, failureTime2);

    mockTime.sleep(SelfHealingNotifier.DEFAULT_AUTO_FIX_THRESHOLD_MS + failureTime1);
    anomalyNotifier.resetAlert();
    AnomalyNotificationResult result = anomalyNotifier.onBrokerFailure(new BrokerFailures(failedBrokers));
    assertEquals(AnomalyNotificationResult.Action.IGNORE, result.action());
    assertTrue(anomalyNotifier.alertCalled);
    assertFalse(anomalyNotifier.autoFixTriggered);
  }

  private static class TestingBrokerFailureAutoFixNotifier extends SelfHealingNotifier {
    boolean autoFixTriggered = false;
    boolean alertCalled = false;

    TestingBrokerFailureAutoFixNotifier(Time time) {
      super(time);
    }

    @Override
    public void alert(Object anomaly, boolean autoFixTriggered, long selfHealingStartTime) {
      alertCalled = true;
      this.autoFixTriggered = autoFixTriggered;
    }

    void resetAlert() {
      autoFixTriggered = false;
      alertCalled = false;
    }
  }
}
