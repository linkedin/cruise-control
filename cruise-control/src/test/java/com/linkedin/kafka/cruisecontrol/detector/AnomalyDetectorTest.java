/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotifier;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorState.NUM_SELF_HEALING_STARTED;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;


/**
 * Unit test class for anomaly detector.
 */
public class AnomalyDetectorTest {
  static private final long MOCK_ANOMALY_DETECTION_INTERVAL_MS = 3000L;
  static private final long MOCK_DELAY_CHECK_MS = 1000L;
  static private final Map<AnomalyType, Float> MOCK_SELF_HEALING_ENABLED_RATIO = new HashMap<>(AnomalyType.cachedValues().size());
  static {
    for (AnomalyType anomalyType : AnomalyType.cachedValues()) {
      MOCK_SELF_HEALING_ENABLED_RATIO.put(anomalyType, 0.99f);
    }
  }


  private static void startPeriodicDetectors(ScheduledExecutorService mockDetectorScheduler,
                                             GoalViolationDetector mockGoalViolationDetector,
                                             MetricAnomalyDetector mockMetricAnomalyDetector,
                                             ScheduledExecutorService executorService) {
    // Starting periodic goal violation detection.
    EasyMock.expect(mockDetectorScheduler.scheduleAtFixedRate(EasyMock.eq(mockGoalViolationDetector),
                                                              EasyMock.anyLong(),
                                                              EasyMock.eq(MOCK_ANOMALY_DETECTION_INTERVAL_MS),
                                                              EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);

    // Starting periodic metric anomaly detection.
    EasyMock.expect(mockDetectorScheduler.scheduleAtFixedRate(EasyMock.eq(mockMetricAnomalyDetector),
                                                              EasyMock.anyLong(),
                                                              EasyMock.eq(MOCK_ANOMALY_DETECTION_INTERVAL_MS),
                                                              EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);

    // Starting anomaly handler
    EasyMock.expect(mockDetectorScheduler.submit(EasyMock.isA(AnomalyDetector.AnomalyHandlerTask.class)))
            .andDelegateTo(executorService);
  }

  @Test
  public void testDelayedCheck() throws InterruptedException {
    LinkedBlockingDeque<Anomaly> anomalies = new LinkedBlockingDeque<>();
    AnomalyNotifier mockAnomalyNotifier = EasyMock.mock(AnomalyNotifier.class);
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    MetricAnomalyDetector mockMetricAnomalyDetector = EasyMock.createNiceMock(MetricAnomalyDetector.class);
    ScheduledExecutorService mockDetectorScheduler = EasyMock.mock(ScheduledExecutorService.class);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);

    EasyMock.expect(mockAnomalyNotifier.onBrokerFailure(EasyMock.isA(BrokerFailures.class)))
            .andReturn(AnomalyNotificationResult.check(MOCK_DELAY_CHECK_MS));
    EasyMock.expect(mockAnomalyNotifier.selfHealingEnabledRatio()).andReturn(MOCK_SELF_HEALING_ENABLED_RATIO);

    startPeriodicDetectors(mockDetectorScheduler, mockGoalViolationDetector, mockMetricAnomalyDetector, executorService);
    // Schedule a delayed check
    EasyMock.expect(mockDetectorScheduler.schedule(EasyMock.isA(Runnable.class),
                                                   EasyMock.eq(MOCK_DELAY_CHECK_MS),
                                                   EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);
    shutdownDetector(mockDetectorScheduler, executorService);

    // The following state are used to test the delayed check when executor is idle.
    EasyMock.expect(mockKafkaCruiseControl.executionState()).andReturn(ExecutorState.State.NO_TASK_IN_PROGRESS);
    replayMocks(mockAnomalyNotifier, mockBrokerFailureDetector, mockGoalViolationDetector, mockMetricAnomalyDetector,
                mockDetectorScheduler, mockKafkaCruiseControl);

    AnomalyDetector anomalyDetector = new AnomalyDetector(anomalies, MOCK_ANOMALY_DETECTION_INTERVAL_MS, mockKafkaCruiseControl, mockAnomalyNotifier,
                                                          mockGoalViolationDetector, mockBrokerFailureDetector,
                                                          mockMetricAnomalyDetector, mockDetectorScheduler,
                                                          EasyMock.mock(LoadMonitor.class));

    try {
      anomalyDetector.startDetection();
      anomalies.add(new BrokerFailures(mockKafkaCruiseControl, Collections.singletonMap(0, 100L),
                                       false, true, true,
                                       Collections.emptyList()));
      while (anomalyDetector.numCheckedWithDelay() < 1) {
        // Wait for the anomaly to be checked with delay before attempting to shutdown the anomaly detector.
      }
      anomalyDetector.shutdown();
      assertEquals(0, anomalyDetector.numSelfHealingStarted());
      assertEquals(1, anomalyDetector.numCheckedWithDelay());
      assertTrue(executorService.awaitTermination(5000, TimeUnit.MILLISECONDS));
      AnomalyDetectorState anomalyDetectorState = anomalyDetector.anomalyDetectorState();
      assertEquals((long) anomalyDetectorState.metrics().get(NUM_SELF_HEALING_STARTED), 0L);
      assertEquals(anomalyDetectorState.recentAnomaliesByType().get(AnomalyType.BROKER_FAILURE).size(), 1);
      assertEquals(anomalyDetectorState.recentAnomaliesByType().get(AnomalyType.GOAL_VIOLATION).size(), 0);
      assertEquals(anomalyDetectorState.recentAnomaliesByType().get(AnomalyType.METRIC_ANOMALY).size(), 0);
    } finally {
      executorService.shutdown();
    }
    EasyMock.verify(mockAnomalyNotifier, mockDetectorScheduler, mockKafkaCruiseControl);
  }

  private static void shutdownDetector(ScheduledExecutorService mockDetectorScheduler,
                                       ScheduledExecutorService executorService) throws InterruptedException {
    mockDetectorScheduler.shutdown();
    EasyMock.expectLastCall().andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.awaitTermination(MOCK_ANOMALY_DETECTION_INTERVAL_MS, TimeUnit.MILLISECONDS)).andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.isTerminated()).andDelegateTo(executorService);
  }

  private static void replayMocks(AnomalyNotifier mockAnomalyNotifier,
                                  BrokerFailureDetector mockBrokerFailureDetector,
                                  GoalViolationDetector mockGoalViolationDetector,
                                  MetricAnomalyDetector mockMetricAnomalyDetector,
                                  ScheduledExecutorService mockDetectorScheduler,
                                  KafkaCruiseControl mockKafkaCruiseControl) {
    EasyMock.replay(mockAnomalyNotifier);
    EasyMock.replay(mockBrokerFailureDetector);
    EasyMock.replay(mockGoalViolationDetector);
    EasyMock.replay(mockMetricAnomalyDetector);
    EasyMock.replay(mockDetectorScheduler);
    EasyMock.replay(mockKafkaCruiseControl);
  }

  @Test
  public void testFix() throws InterruptedException, KafkaCruiseControlException {
    LinkedBlockingDeque<Anomaly> anomalies = new LinkedBlockingDeque<>();
    AnomalyNotifier mockAnomalyNotifier = EasyMock.mock(AnomalyNotifier.class);
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    MetricAnomalyDetector mockMetricAnomalyDetector = EasyMock.createNiceMock(MetricAnomalyDetector.class);
    ScheduledExecutorService mockDetectorScheduler = EasyMock.mock(ScheduledExecutorService.class);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);

    EasyMock.expect(mockAnomalyNotifier.onGoalViolation(EasyMock.isA(GoalViolations.class)))
            .andReturn(AnomalyNotificationResult.fix());
    EasyMock.expect(mockAnomalyNotifier.selfHealingEnabledRatio()).andReturn(MOCK_SELF_HEALING_ENABLED_RATIO);


    startPeriodicDetectors(mockDetectorScheduler, mockGoalViolationDetector, mockMetricAnomalyDetector, executorService);
    shutdownDetector(mockDetectorScheduler, executorService);

    // The following state are used to test the delayed check when executor is idle.
    EasyMock.expect(mockKafkaCruiseControl.executionState()).andReturn(ExecutorState.State.NO_TASK_IN_PROGRESS);
    EasyMock.expect(mockKafkaCruiseControl.rebalance(EasyMock.eq(Collections.emptyList()),
                                                     EasyMock.eq(false),
                                                     EasyMock.eq(null),
                                                     EasyMock.anyObject(OperationProgress.class),
                                                     EasyMock.eq(true),
                                                     EasyMock.eq(null),
                                                     EasyMock.eq(null),
                                                     EasyMock.eq(false),
                                                     EasyMock.eq(null),
                                                     EasyMock.eq(null),
                                                     EasyMock.anyString(),
                                                     EasyMock.eq(true),
                                                     EasyMock.eq(true),
                                                     EasyMock.eq(false),
                                                     EasyMock.eq(true),
                                                     EasyMock.eq(Collections.emptySet())))
            .andReturn(null);
    EasyMock.expect(mockKafkaCruiseControl.meetCompletenessRequirements(Collections.emptyList())).andReturn(true);
    EasyMock.expect(mockDetectorScheduler.schedule(EasyMock.isA(Runnable.class),
                                                   EasyMock.eq(0L),
                                                   EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);
    replayMocks(mockAnomalyNotifier, mockBrokerFailureDetector, mockGoalViolationDetector, mockMetricAnomalyDetector,
                mockDetectorScheduler, mockKafkaCruiseControl);

    AnomalyDetector anomalyDetector = new AnomalyDetector(anomalies, MOCK_ANOMALY_DETECTION_INTERVAL_MS, mockKafkaCruiseControl, mockAnomalyNotifier,
                                                          mockGoalViolationDetector, mockBrokerFailureDetector,
                                                          mockMetricAnomalyDetector, mockDetectorScheduler,
                                                          EasyMock.mock(LoadMonitor.class));

    try {
      anomalyDetector.startDetection();
      GoalViolations violations = new GoalViolations(mockKafkaCruiseControl, true,
                                                     true, true,
                                                     Collections.emptyList());
      violations.addViolation("RackAwareGoal", true);
      anomalies.add(violations);
      while (anomalyDetector.numSelfHealingStarted() < 1) {
        // Wait for the anomaly to be fixed before attempting to shutdown the anomaly detector.
      }
      anomalyDetector.shutdown();
      assertEquals(1, anomalyDetector.numSelfHealingStarted());
      assertEquals(0, anomalyDetector.numCheckedWithDelay());
      assertTrue(executorService.awaitTermination(5000, TimeUnit.MILLISECONDS));
      AnomalyDetectorState anomalyDetectorState = anomalyDetector.anomalyDetectorState();
      assertEquals((long) anomalyDetectorState.metrics().get(NUM_SELF_HEALING_STARTED), 1L);
      assertEquals(anomalyDetectorState.recentAnomaliesByType().get(AnomalyType.BROKER_FAILURE).size(), 0);
      assertEquals(anomalyDetectorState.recentAnomaliesByType().get(AnomalyType.GOAL_VIOLATION).size(), 1);
      assertEquals(anomalyDetectorState.recentAnomaliesByType().get(AnomalyType.METRIC_ANOMALY).size(), 0);
      EasyMock.verify(mockAnomalyNotifier, mockDetectorScheduler, mockKafkaCruiseControl);
    } finally {
      executorService.shutdown();
    }
  }

  @Test
  public void testExecutionInProgress() throws InterruptedException {
    LinkedBlockingDeque<Anomaly> anomalies = new LinkedBlockingDeque<>();
    AnomalyNotifier mockAnomalyNotifier = EasyMock.mock(AnomalyNotifier.class);
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    MetricAnomalyDetector mockMetricAnomalyDetector = EasyMock.createNiceMock(MetricAnomalyDetector.class);
    ScheduledExecutorService mockDetectorScheduler = EasyMock.mock(ScheduledExecutorService.class);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);

    startPeriodicDetectors(mockDetectorScheduler, mockGoalViolationDetector, mockMetricAnomalyDetector, executorService);
    shutdownDetector(mockDetectorScheduler, executorService);
    EasyMock.expect(mockAnomalyNotifier.selfHealingEnabledRatio()).andReturn(MOCK_SELF_HEALING_ENABLED_RATIO);

    // The following state are used to test the delayed check when executor is idle.
    EasyMock.expect(mockKafkaCruiseControl.executionState())
            .andReturn(ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS);

    replayMocks(mockAnomalyNotifier, mockBrokerFailureDetector, mockGoalViolationDetector, mockMetricAnomalyDetector,
                mockDetectorScheduler, mockKafkaCruiseControl);

    AnomalyDetector anomalyDetector = new AnomalyDetector(anomalies, MOCK_ANOMALY_DETECTION_INTERVAL_MS, mockKafkaCruiseControl, mockAnomalyNotifier,
                                                          mockGoalViolationDetector, mockBrokerFailureDetector,
                                                          mockMetricAnomalyDetector, mockDetectorScheduler,
                                                          EasyMock.mock(LoadMonitor.class));

    try {
      anomalyDetector.startDetection();
      anomalies.add(new GoalViolations(mockKafkaCruiseControl, true,
                                       true, true,
                                       Collections.emptyList()));
      while (!anomalies.isEmpty()) {
        // Just wait for the anomalies to be drained.
      }
      anomalyDetector.shutdown();
      assertEquals(0, anomalyDetector.numSelfHealingStarted());
      assertEquals(0, anomalyDetector.numCheckedWithDelay());
      assertTrue(executorService.awaitTermination(5000, TimeUnit.MILLISECONDS));
      AnomalyDetectorState anomalyDetectorState = anomalyDetector.anomalyDetectorState();
      assertEquals((long) anomalyDetectorState.metrics().get(NUM_SELF_HEALING_STARTED), 0L);
      assertEquals(anomalyDetectorState.recentAnomaliesByType().get(AnomalyType.BROKER_FAILURE).size(), 0);
      assertEquals(anomalyDetectorState.recentAnomaliesByType().get(AnomalyType.GOAL_VIOLATION).size(), 1);
      assertEquals(anomalyDetectorState.recentAnomaliesByType().get(AnomalyType.METRIC_ANOMALY).size(), 0);
    } finally {
      executorService.shutdown();
    }
    EasyMock.verify(mockAnomalyNotifier, mockDetectorScheduler, mockKafkaCruiseControl);
  }

  @Test
  public void testShutdown() throws InterruptedException {
    AnomalyNotifier mockAnomalyNotifier = EasyMock.createNiceMock(AnomalyNotifier.class);
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    MetricAnomalyDetector mockMetricAnomalyDetector = EasyMock.createNiceMock(MetricAnomalyDetector.class);
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.createNiceMock(KafkaCruiseControl.class);
    ScheduledExecutorService detectorScheduler =
        Executors.newScheduledThreadPool(2, new KafkaCruiseControlThreadFactory("AnomalyDetector", false, null));

    AnomalyDetector anomalyDetector = new AnomalyDetector(new LinkedBlockingDeque<>(), MOCK_ANOMALY_DETECTION_INTERVAL_MS, mockKafkaCruiseControl,
                                                          mockAnomalyNotifier, mockGoalViolationDetector,
                                                          mockBrokerFailureDetector, mockMetricAnomalyDetector,
                                                          detectorScheduler, EasyMock.mock(LoadMonitor.class));

    anomalyDetector.shutdown();
    Thread t = new Thread(anomalyDetector::shutdown);
    t.start();
    t.join(30000L);
    assertEquals(0, anomalyDetector.numSelfHealingStarted());
    assertEquals(0, anomalyDetector.numCheckedWithDelay());
    assertTrue(detectorScheduler.isTerminated());
  }
}
