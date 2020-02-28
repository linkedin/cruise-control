/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import com.linkedin.kafka.cruisecontrol.detector.DiskFailures;
import com.linkedin.kafka.cruisecontrol.detector.GoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomaly;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.BrokerFailureDetector.FAILED_BROKERS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.DiskFailureDetector.FAILED_DISKS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_BROKER_ENTITIES_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_FIXABLE_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder.SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder.TOPICS_WITH_BAD_REPLICATION_FACTOR_BY_FIXABILITY_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for SelfHealingNotifier.
 */
public class SelfHealingNotifierTest {
  public static final String BAD_LOGDIR = "logdir1";
  public static final String BAD_TOPIC = "topic1";
  public static final short SELF_HEALING_TARGET_REPLICATION_FACTOR = 3;

  @Test
  public void testOnBrokerFailure() {
    final long failureTime1 = 200L;
    final long failureTime2 = 400L;
    final long startTime = 500L;
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(props);
    EasyMock.expect(mockKafkaCruiseControl.config()).andReturn(kafkaCruiseControlConfig).atLeastOnce();
    EasyMock.replay(mockKafkaCruiseControl);
    Time mockTime = new MockTime(0, startTime, TimeUnit.NANOSECONDS.convert(startTime, TimeUnit.MILLISECONDS));
    TestingBrokerFailureAutoFixNotifier anomalyNotifier = new TestingBrokerFailureAutoFixNotifier(mockTime);
    anomalyNotifier.configure(Collections.singletonMap(SelfHealingNotifier.SELF_HEALING_BROKER_FAILURE_ENABLED_CONFIG, "true"));

    Map<Integer, Long> failedBrokers = new HashMap<>();
    failedBrokers.put(1, failureTime1);
    failedBrokers.put(2, failureTime2);
    Map<String, Object> parameterConfigOverrides = new HashMap<>(3);
    parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, mockKafkaCruiseControl);
    parameterConfigOverrides.put(FAILED_BROKERS_OBJECT_CONFIG, failedBrokers);
    parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, failureTime1);

    AnomalyNotificationResult result = anomalyNotifier.onBrokerFailure(
        kafkaCruiseControlConfig.getConfiguredInstance(AnomalyDetectorConfig.BROKER_FAILURES_CLASS_CONFIG,
                                                       BrokerFailures.class,
                                                       parameterConfigOverrides));
    assertEquals(AnomalyNotificationResult.Action.CHECK, result.action());
    assertEquals(SelfHealingNotifier.DEFAULT_ALERT_THRESHOLD_MS + failureTime1 - mockTime.milliseconds(),
                 result.delay());
    assertFalse(anomalyNotifier._alertCalled.get(KafkaAnomalyType.BROKER_FAILURE));

    // Sleep to 1 ms before alert.
    mockTime.sleep(result.delay() - 1);
    result = anomalyNotifier.onBrokerFailure(
        kafkaCruiseControlConfig.getConfiguredInstance(AnomalyDetectorConfig.BROKER_FAILURES_CLASS_CONFIG,
                                                       BrokerFailures.class,
                                                       parameterConfigOverrides));
    assertEquals(AnomalyNotificationResult.Action.CHECK, result.action());
    assertEquals(1, result.delay());
    assertFalse(anomalyNotifier._alertCalled.get(KafkaAnomalyType.BROKER_FAILURE));

    // Sleep 1 ms
    mockTime.sleep(1);
    anomalyNotifier.resetAlert(KafkaAnomalyType.BROKER_FAILURE);
    result = anomalyNotifier.onBrokerFailure(
        kafkaCruiseControlConfig.getConfiguredInstance(AnomalyDetectorConfig.BROKER_FAILURES_CLASS_CONFIG,
                                                       BrokerFailures.class,
                                                       parameterConfigOverrides));
    assertEquals(AnomalyNotificationResult.Action.CHECK, result.action());
    assertEquals(SelfHealingNotifier.DEFAULT_AUTO_FIX_THRESHOLD_MS + failureTime1 - mockTime.milliseconds(),
                 result.delay());
    assertTrue(anomalyNotifier._alertCalled.get(KafkaAnomalyType.BROKER_FAILURE));

    // Sleep to 1 ms before alert.
    mockTime.sleep(result.delay() - 1);
    anomalyNotifier.resetAlert(KafkaAnomalyType.BROKER_FAILURE);
    result = anomalyNotifier.onBrokerFailure(
        kafkaCruiseControlConfig.getConfiguredInstance(AnomalyDetectorConfig.BROKER_FAILURES_CLASS_CONFIG,
                                                       BrokerFailures.class,
                                                       parameterConfigOverrides));
    assertEquals(AnomalyNotificationResult.Action.CHECK, result.action());
    assertEquals(1, result.delay());
    assertFalse(anomalyNotifier._alertCalled.get(KafkaAnomalyType.BROKER_FAILURE));
    assertFalse(anomalyNotifier._autoFixTriggered.get(KafkaAnomalyType.BROKER_FAILURE));

    // Sleep 1 ms
    mockTime.sleep(1);
    anomalyNotifier.resetAlert(KafkaAnomalyType.BROKER_FAILURE);
    result = anomalyNotifier.onBrokerFailure(
        kafkaCruiseControlConfig.getConfiguredInstance(AnomalyDetectorConfig.BROKER_FAILURES_CLASS_CONFIG,
                                                       BrokerFailures.class,
                                                       parameterConfigOverrides));
    assertEquals(AnomalyNotificationResult.Action.FIX, result.action());
    assertEquals(-1L, result.delay());
    assertTrue(anomalyNotifier._alertCalled.get(KafkaAnomalyType.BROKER_FAILURE));
    assertTrue(anomalyNotifier._autoFixTriggered.get(KafkaAnomalyType.BROKER_FAILURE));
    assertFalse(anomalyNotifier._alertCalled.get(KafkaAnomalyType.GOAL_VIOLATION));
    assertFalse(anomalyNotifier._alertCalled.get(KafkaAnomalyType.METRIC_ANOMALY));
    assertFalse(anomalyNotifier._alertCalled.get(KafkaAnomalyType.DISK_FAILURE));
    assertFalse(anomalyNotifier._alertCalled.get(KafkaAnomalyType.TOPIC_ANOMALY));
  }

  @Test
  public void testSelfHealingDisabled() {
    final long startTime = 500L;
    Time mockTime = new MockTime(startTime);
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(props);
    EasyMock.expect(mockKafkaCruiseControl.config()).andReturn(kafkaCruiseControlConfig).atLeastOnce();
    EasyMock.replay(mockKafkaCruiseControl);
    TestingBrokerFailureAutoFixNotifier anomalyNotifier = new TestingBrokerFailureAutoFixNotifier(mockTime);

    Map<String, String> selfHealingExplicitlyDisabled = new HashMap<>(4);
    selfHealingExplicitlyDisabled.put(SelfHealingNotifier.SELF_HEALING_BROKER_FAILURE_ENABLED_CONFIG, "false");
    selfHealingExplicitlyDisabled.put(SelfHealingNotifier.SELF_HEALING_GOAL_VIOLATION_ENABLED_CONFIG, "false");
    selfHealingExplicitlyDisabled.put(SelfHealingNotifier.SELF_HEALING_METRIC_ANOMALY_ENABLED_CONFIG, "false");
    selfHealingExplicitlyDisabled.put(SelfHealingNotifier.SELF_HEALING_DISK_FAILURE_ENABLED_CONFIG, "false");
    selfHealingExplicitlyDisabled.put(SelfHealingNotifier.SELF_HEALING_TOPIC_ANOMALY_ENABLED_CONFIG, "false");
    // Set to verify the overriding of specific config over general config
    selfHealingExplicitlyDisabled.put(SelfHealingNotifier.SELF_HEALING_ENABLED_CONFIG, "true");
    anomalyNotifier.configure(selfHealingExplicitlyDisabled);

    // (1) Test broker failure anomaly can be detected by notifier.
    final long failureTime1 = 200L;
    final long failureTime2 = 400L;
    Map<Integer, Long> failedBrokers = new HashMap<>();
    failedBrokers.put(1, failureTime1);
    failedBrokers.put(2, failureTime2);
    final long anomalyDetectionTime = 200L;
    final BrokerEntity brokerWithMetricAnomaly = new BrokerEntity("local", 1);

    mockTime.sleep(SelfHealingNotifier.DEFAULT_AUTO_FIX_THRESHOLD_MS + failureTime1);
    anomalyNotifier.resetAlert(KafkaAnomalyType.BROKER_FAILURE);
    Map<String, Object> parameterConfigOverrides = new HashMap<>(8);
    parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, mockKafkaCruiseControl);
    parameterConfigOverrides.put(FAILED_BROKERS_OBJECT_CONFIG, failedBrokers);
    parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, anomalyDetectionTime);
    parameterConfigOverrides.put(METRIC_ANOMALY_FIXABLE_OBJECT_CONFIG, false);
    parameterConfigOverrides.put(METRIC_ANOMALY_BROKER_ENTITIES_OBJECT_CONFIG,
                                 Collections.singletonMap(brokerWithMetricAnomaly, anomalyDetectionTime));
    parameterConfigOverrides.put(FAILED_DISKS_OBJECT_CONFIG,
                                 Collections.singletonMap(1, Collections.singletonMap(BAD_LOGDIR, failureTime1)));
    parameterConfigOverrides.put(SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG, SELF_HEALING_TARGET_REPLICATION_FACTOR);
    parameterConfigOverrides.put(TOPICS_WITH_BAD_REPLICATION_FACTOR_BY_FIXABILITY_CONFIG,
                                 Collections.singletonMap(true, Collections.singleton(BAD_TOPIC)));
    AnomalyNotificationResult result = anomalyNotifier.onBrokerFailure(
        kafkaCruiseControlConfig.getConfiguredInstance(AnomalyDetectorConfig.BROKER_FAILURES_CLASS_CONFIG,
                                                       BrokerFailures.class,
                                                       parameterConfigOverrides));
    assertEquals(AnomalyNotificationResult.Action.IGNORE, result.action());
    assertTrue(anomalyNotifier._alertCalled.get(KafkaAnomalyType.BROKER_FAILURE));
    assertFalse(anomalyNotifier._autoFixTriggered.get(KafkaAnomalyType.BROKER_FAILURE));

    // (2) Test goal violation anomaly can be detected by notifier.
    anomalyNotifier.resetAlert(KafkaAnomalyType.GOAL_VIOLATION);
    result = anomalyNotifier.onGoalViolation(
        kafkaCruiseControlConfig.getConfiguredInstance(AnomalyDetectorConfig.GOAL_VIOLATIONS_CLASS_CONFIG,
                                                       GoalViolations.class,
                                                       parameterConfigOverrides));
    assertEquals(AnomalyNotificationResult.Action.IGNORE, result.action());
    assertTrue(anomalyNotifier._alertCalled.get(KafkaAnomalyType.GOAL_VIOLATION));
    assertFalse(anomalyNotifier._autoFixTriggered.get(KafkaAnomalyType.GOAL_VIOLATION));

    // (3) Test metric anomaly can be detected by notifier.
    anomalyNotifier.resetAlert(KafkaAnomalyType.METRIC_ANOMALY);
    result = anomalyNotifier.onMetricAnomaly(kafkaCruiseControlConfig.getConfiguredInstance(AnomalyDetectorConfig.METRIC_ANOMALY_CLASS_CONFIG,
                                                                                            KafkaMetricAnomaly.class,
                                                                                            parameterConfigOverrides));
    assertEquals(AnomalyNotificationResult.Action.IGNORE, result.action());
    assertTrue(anomalyNotifier._alertCalled.get(KafkaAnomalyType.METRIC_ANOMALY));
    assertFalse(anomalyNotifier._autoFixTriggered.get(KafkaAnomalyType.METRIC_ANOMALY));

    // (4) Test disk failure anomaly can be detected by notifier.
    anomalyNotifier.resetAlert(KafkaAnomalyType.DISK_FAILURE);
    result = anomalyNotifier.onDiskFailure(kafkaCruiseControlConfig.getConfiguredInstance(AnomalyDetectorConfig.DISK_FAILURES_CLASS_CONFIG,
                                                                                          DiskFailures.class,
                                                                                          parameterConfigOverrides));
    assertEquals(AnomalyNotificationResult.Action.IGNORE, result.action());
    assertTrue(anomalyNotifier._alertCalled.get(KafkaAnomalyType.DISK_FAILURE));
    assertFalse(anomalyNotifier._autoFixTriggered.get(KafkaAnomalyType.DISK_FAILURE));

    // (5) Test topic anomaly can be detected by notifier.
    anomalyNotifier.resetAlert(KafkaAnomalyType.TOPIC_ANOMALY);
    TopicReplicationFactorAnomaly topicReplicationFactorAnomaly = new TopicReplicationFactorAnomaly();
    topicReplicationFactorAnomaly.configure(parameterConfigOverrides);
    result = anomalyNotifier.onTopicAnomaly(topicReplicationFactorAnomaly);
    assertEquals(AnomalyNotificationResult.Action.IGNORE, result.action());
    assertTrue(anomalyNotifier._alertCalled.get(KafkaAnomalyType.TOPIC_ANOMALY));
    assertFalse(anomalyNotifier._autoFixTriggered.get(KafkaAnomalyType.TOPIC_ANOMALY));
  }

  private static class TestingBrokerFailureAutoFixNotifier extends SelfHealingNotifier {
    final Map<AnomalyType, Boolean> _alertCalled;
    final Map<AnomalyType, Boolean> _autoFixTriggered;

    TestingBrokerFailureAutoFixNotifier(Time time) {
      super(time);
      _alertCalled = new HashMap<>(KafkaAnomalyType.cachedValues().size());
      _autoFixTriggered = new HashMap<>(KafkaAnomalyType.cachedValues().size());
      for (KafkaAnomalyType alertType : KafkaAnomalyType.cachedValues()) {
        _alertCalled.put(alertType, false);
        _autoFixTriggered.put(alertType, false);
      }
    }

    @Override
    public void alert(Object anomaly, boolean autoFixTriggered, long selfHealingStartTime, AnomalyType anomalyType) {
      _alertCalled.put(anomalyType, true);
      _autoFixTriggered.put(anomalyType, autoFixTriggered);
    }

    void resetAlert(KafkaAnomalyType anomalyType) {
      _autoFixTriggered.put(anomalyType, false);
      _alertCalled.put(anomalyType, false);
    }
  }
}
