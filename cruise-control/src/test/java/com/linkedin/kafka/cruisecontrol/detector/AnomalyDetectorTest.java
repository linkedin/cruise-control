/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotifier;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


/**
 * Unit test class for anomaly detector.
 */
public class AnomalyDetectorTest {
  static private final long MOCK_ANOMALY_DETECTION_INTERVAL_MS = 3000L;
  static private final long MOCK_DELAY_CHECK_MS = 1000L;

  @Test
  public void testDelayedCheck() throws InterruptedException {
    LinkedBlockingDeque<Anomaly> anomalies = new LinkedBlockingDeque<>();
    AdminClient mockAdminClient = EasyMock.createNiceMock(AdminClient.class);
    AnomalyNotifier mockAnomalyNotifier = EasyMock.mock(AnomalyNotifier.class);
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    MetricAnomalyDetector mockMetricAnomalyDetector = EasyMock.createNiceMock(MetricAnomalyDetector.class);
    DiskFailureDetector mockDiskFailureDetector = EasyMock.createNiceMock(DiskFailureDetector.class);
    ScheduledExecutorService mockDetectorScheduler = EasyMock.mock(ScheduledExecutorService.class);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);

    EasyMock.expect(mockAnomalyNotifier.onBrokerFailure(EasyMock.isA(BrokerFailures.class)))
            .andReturn(AnomalyNotificationResult.check(MOCK_DELAY_CHECK_MS));

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

    // Starting periodic disk failure detection.
    EasyMock.expect(mockDetectorScheduler.scheduleAtFixedRate(EasyMock.eq(mockDiskFailureDetector),
                                                              EasyMock.anyLong(),
                                                              EasyMock.eq(MOCK_ANOMALY_DETECTION_INTERVAL_MS),
                                                              EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);

    // Starting anomaly handler
    EasyMock.expect(mockDetectorScheduler.submit(EasyMock.isA(AnomalyDetector.AnomalyHandlerTask.class)))
            .andDelegateTo(executorService);
    // Schedule a delayed check
    EasyMock.expect(mockDetectorScheduler.schedule(EasyMock.isA(Runnable.class),
                                                   EasyMock.eq(MOCK_DELAY_CHECK_MS),
                                                   EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);
    mockDetectorScheduler.shutdown();
    EasyMock.expectLastCall().andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.awaitTermination(MOCK_ANOMALY_DETECTION_INTERVAL_MS, TimeUnit.MILLISECONDS))
            .andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.isTerminated()).andDelegateTo(executorService);

    // The following state are used to test the delayed check when executor is idle.
    EasyMock.expect(mockKafkaCruiseControl.state(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new CruiseControlState(ExecutorState.noTaskInProgress(null, null), null, null,
                                              null, null));
    EasyMock.replay(mockAnomalyNotifier);
    EasyMock.replay(mockBrokerFailureDetector);
    EasyMock.replay(mockGoalViolationDetector);
    EasyMock.replay(mockMetricAnomalyDetector);
    EasyMock.replay(mockDetectorScheduler);
    EasyMock.replay(mockKafkaCruiseControl);

    AnomalyDetector anomalyDetector = new AnomalyDetector(anomalies, mockAdminClient, MOCK_ANOMALY_DETECTION_INTERVAL_MS, mockKafkaCruiseControl,
                                                          mockAnomalyNotifier, mockGoalViolationDetector, mockBrokerFailureDetector,
                                                          mockMetricAnomalyDetector, mockDiskFailureDetector, mockDetectorScheduler,
                                                          EasyMock.mock(LoadMonitor.class));

    try {
      anomalyDetector.startDetection();
      anomalies.add(new BrokerFailures(mockKafkaCruiseControl, Collections.singletonMap(0, 100L),
                                       false, true, true,
                                       Collections.emptyList()));
      while (!anomalies.isEmpty()) {
        // just wait for the anomalies to be drained.
      }
      anomalyDetector.shutdown();
      assertTrue(executorService.awaitTermination(5000, TimeUnit.MILLISECONDS));
      EasyMock.verify(mockAnomalyNotifier, mockDetectorScheduler, mockKafkaCruiseControl);
    } finally {
      executorService.shutdown();
    }
  }

  @Test
  public void testFixGoalViolation() throws InterruptedException, KafkaCruiseControlException {
    testFixAnomaly(AnomalyType.GOAL_VIOLATION);
  }

  @Test
  public void testFixDiskFailure() throws InterruptedException, KafkaCruiseControlException {
    testFixAnomaly(AnomalyType.DISK_FAILURE);
  }

  private void testFixAnomaly(AnomalyType anomalyType) throws InterruptedException, KafkaCruiseControlException {
    LinkedBlockingDeque<Anomaly> anomalies = new LinkedBlockingDeque<>();
    AdminClient mockAdminClient = EasyMock.createNiceMock(AdminClient.class);
    AnomalyNotifier mockAnomalyNotifier = EasyMock.mock(AnomalyNotifier.class);
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    MetricAnomalyDetector mockMetricAnomalyDetector = EasyMock.createNiceMock(MetricAnomalyDetector.class);
    DiskFailureDetector mockDiskFailureDetector = EasyMock.createNiceMock(DiskFailureDetector.class);
    ScheduledExecutorService mockDetectorScheduler = EasyMock.mock(ScheduledExecutorService.class);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);

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

    // Starting periodic disk failure detection.
    EasyMock.expect(mockDetectorScheduler.scheduleAtFixedRate(EasyMock.eq(mockDiskFailureDetector),
                                                              EasyMock.anyLong(),
                                                              EasyMock.eq(MOCK_ANOMALY_DETECTION_INTERVAL_MS),
                                                              EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);

    // Starting anomaly handler
    EasyMock.expect(mockDetectorScheduler.submit(EasyMock.isA(AnomalyDetector.AnomalyHandlerTask.class)))
            .andDelegateTo(executorService);

    mockDetectorScheduler.shutdown();
    EasyMock.expectLastCall().andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.awaitTermination(MOCK_ANOMALY_DETECTION_INTERVAL_MS, TimeUnit.MILLISECONDS))
            .andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.isTerminated()).andDelegateTo(executorService);

    // The following state are used to test the delayed check when executor is idle.
    EasyMock.expect(mockKafkaCruiseControl.state(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new CruiseControlState(ExecutorState.noTaskInProgress(null, null), null, null,
                                              null, null));

    if (anomalyType == AnomalyType.GOAL_VIOLATION) {
      EasyMock.expect(mockAnomalyNotifier.onGoalViolation(EasyMock.isA(GoalViolations.class))).andReturn(AnomalyNotificationResult.fix());
      EasyMock.expect(mockKafkaCruiseControl.rebalance(EasyMock.eq(Collections.emptyList()),
                                                       EasyMock.eq(false),
                                                       EasyMock.eq(null),
                                                       EasyMock.anyObject(OperationProgress.class),
                                                       EasyMock.eq(true),
                                                       EasyMock.eq(null),
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
                                                       EasyMock.eq(Collections.emptySet()),
                                                       EasyMock.eq(false)))
              .andReturn(null);
    } else if (anomalyType == AnomalyType.DISK_FAILURE) {
      EasyMock.expect(mockAnomalyNotifier.onDiskFailure(EasyMock.isA(DiskFailures.class))).andReturn(AnomalyNotificationResult.fix());
      EasyMock.expect(mockKafkaCruiseControl.fixOfflineReplicas(EasyMock.eq(false),
                                                                EasyMock.eq(Collections.emptyList()),
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
                                                                EasyMock.eq(true)))
              .andReturn(null);
    }
    EasyMock.expect(mockKafkaCruiseControl.meetCompletenessRequirements(Collections.emptyList())).andReturn(true);

    EasyMock.replay(mockAnomalyNotifier);
    EasyMock.replay(mockBrokerFailureDetector);
    EasyMock.replay(mockGoalViolationDetector);
    EasyMock.replay(mockMetricAnomalyDetector);
    EasyMock.replay(mockDetectorScheduler);
    EasyMock.replay(mockKafkaCruiseControl);

    AnomalyDetector anomalyDetector = new AnomalyDetector(anomalies, mockAdminClient, MOCK_ANOMALY_DETECTION_INTERVAL_MS, mockKafkaCruiseControl,
                                                          mockAnomalyNotifier, mockGoalViolationDetector, mockBrokerFailureDetector,
                                                          mockMetricAnomalyDetector, mockDiskFailureDetector, mockDetectorScheduler,
                                                          EasyMock.mock(LoadMonitor.class));

    try {
      anomalyDetector.startDetection();
      if (anomalyType == AnomalyType.GOAL_VIOLATION) {
        GoalViolations violations = new GoalViolations(mockKafkaCruiseControl, true,
                                                       true, true,
                                                       Collections.emptyList());
        violations.addViolation("RackAwareGoal", true);
        anomalies.add(violations);
      } else if (anomalyType == AnomalyType.DISK_FAILURE) {
        Map<Integer, Map<String, Long>> failedDisksByBroker = Collections.singletonMap(0, Collections.singletonMap("tmp", 0L));
        DiskFailures diskFailures = new DiskFailures(mockKafkaCruiseControl, failedDisksByBroker, true,
                                                     true, true,
                                                     Collections.emptyList());
        anomalies.add(diskFailures);
      }
      while (!anomalies.isEmpty()) {
        // Just wait for the anomalies to be drained.
      }
      anomalyDetector.shutdown();
      assertTrue(executorService.awaitTermination(5000, TimeUnit.MILLISECONDS));
    } finally {
      executorService.shutdown();
    }
    EasyMock.verify(mockAnomalyNotifier, mockDetectorScheduler, mockKafkaCruiseControl);
  }

  @Test
  public void testExecutionInProgress() throws InterruptedException {
    LinkedBlockingDeque<Anomaly> anomalies = new LinkedBlockingDeque<>();
    AdminClient mockAdminClient = EasyMock.createNiceMock(AdminClient.class);
    AnomalyNotifier mockAnomalyNotifier = EasyMock.mock(AnomalyNotifier.class);
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    MetricAnomalyDetector mockMetricAnomalyDetector = EasyMock.createNiceMock(MetricAnomalyDetector.class);
    DiskFailureDetector mockDiskFailureDetector = EasyMock.createNiceMock(DiskFailureDetector.class);
    ScheduledExecutorService mockDetectorScheduler = EasyMock.mock(ScheduledExecutorService.class);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);

    // Starting periodic goal violation detection.
    EasyMock.expect(mockDetectorScheduler.scheduleAtFixedRate(EasyMock.eq(mockGoalViolationDetector),
                                                              EasyMock.anyLong(),
                                                              EasyMock.eq(MOCK_ANOMALY_DETECTION_INTERVAL_MS),
                                                              EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);

    // Starting periodic goal violation detection.
    EasyMock.expect(mockDetectorScheduler.scheduleAtFixedRate(EasyMock.eq(mockMetricAnomalyDetector),
                                                              EasyMock.anyLong(),
                                                              EasyMock.eq(MOCK_ANOMALY_DETECTION_INTERVAL_MS),
                                                              EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);

    // Starting periodic disk failure detection.
    EasyMock.expect(mockDetectorScheduler.scheduleAtFixedRate(EasyMock.eq(mockDiskFailureDetector),
                                                              EasyMock.anyLong(),
                                                              EasyMock.eq(MOCK_ANOMALY_DETECTION_INTERVAL_MS),
                                                              EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);

    // Starting anomaly handler
    EasyMock.expect(mockDetectorScheduler.submit(EasyMock.isA(AnomalyDetector.AnomalyHandlerTask.class)))
            .andDelegateTo(executorService);
    // For detector shutdown.
    mockDetectorScheduler.shutdown();
    EasyMock.expectLastCall().andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.awaitTermination(MOCK_ANOMALY_DETECTION_INTERVAL_MS, TimeUnit.MILLISECONDS))
            .andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.isTerminated()).andDelegateTo(executorService);

    // The following state are used to test the delayed check when executor is busy.
    EasyMock.expect(mockKafkaCruiseControl.state(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new CruiseControlState(
                ExecutorState.operationInProgress(ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                  null,
                                                  1,
                                                  1,
                                                  1,
                                                  null,
                                                  null,
                                                  null),
                null, null, null, null));

    EasyMock.replay(mockAnomalyNotifier);
    EasyMock.replay(mockBrokerFailureDetector);
    EasyMock.replay(mockGoalViolationDetector);
    EasyMock.replay(mockMetricAnomalyDetector);
    EasyMock.replay(mockDetectorScheduler);
    EasyMock.replay(mockKafkaCruiseControl);

    AnomalyDetector anomalyDetector = new AnomalyDetector(anomalies, mockAdminClient, MOCK_ANOMALY_DETECTION_INTERVAL_MS, mockKafkaCruiseControl,
                                                          mockAnomalyNotifier, mockGoalViolationDetector, mockBrokerFailureDetector,
                                                          mockMetricAnomalyDetector, mockDiskFailureDetector, mockDetectorScheduler,
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
      assertTrue(executorService.awaitTermination(5000, TimeUnit.MILLISECONDS));

      EasyMock.verify(mockAnomalyNotifier, mockDetectorScheduler, mockKafkaCruiseControl);
    } finally {
      executorService.shutdown();
    }
  }

  @Test
  public void testShutdown() throws InterruptedException {
    AnomalyNotifier mockAnomalyNotifier = EasyMock.createNiceMock(AnomalyNotifier.class);
    AdminClient mockAdminClient = EasyMock.createNiceMock(AdminClient.class);
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    MetricAnomalyDetector mockMetricAnomalyDetector = EasyMock.createNiceMock(MetricAnomalyDetector.class);
    DiskFailureDetector mockDiskFailureDetector = EasyMock.createNiceMock(DiskFailureDetector.class);
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.createNiceMock(KafkaCruiseControl.class);
    ScheduledExecutorService detectorScheduler =
        Executors.newScheduledThreadPool(2, new KafkaCruiseControlThreadFactory("AnomalyDetector", false, null));

    AnomalyDetector anomalyDetector = new AnomalyDetector(new LinkedBlockingDeque<>(), mockAdminClient, MOCK_ANOMALY_DETECTION_INTERVAL_MS,
                                                          mockKafkaCruiseControl, mockAnomalyNotifier, mockGoalViolationDetector,
                                                          mockBrokerFailureDetector, mockMetricAnomalyDetector, mockDiskFailureDetector,
                                                          detectorScheduler, EasyMock.mock(LoadMonitor.class));

    anomalyDetector.shutdown();
    Thread t = new Thread(anomalyDetector::shutdown);
    t.start();
    t.join(30000L);
    assertTrue(detectorScheduler.isTerminated());
  }
}
