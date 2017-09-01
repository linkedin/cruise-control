/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlState;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotifier;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


/**
 * Unit test class for anomaly detector.
 */
public class AnomalyDetectorTest {

  @Test
  public void testDelayedCheck() throws InterruptedException, KafkaCruiseControlException {
    LinkedBlockingDeque<Anomaly> anomalies = new LinkedBlockingDeque<>();
    AnomalyNotifier mockAnomalyNotifier = EasyMock.mock(AnomalyNotifier.class);
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    ScheduledExecutorService mockDetectorScheduler = EasyMock.mock(ScheduledExecutorService.class);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);

    EasyMock.expect(mockAnomalyNotifier.onBrokerFailure(EasyMock.isA(BrokerFailures.class)))
           .andReturn(AnomalyNotificationResult.check(1000L));

    // Starting periodic goal violation detection.
    EasyMock.expect(mockDetectorScheduler.scheduleAtFixedRate(EasyMock.eq(mockGoalViolationDetector),
                                                              EasyMock.anyLong(),
                                                              EasyMock.eq(3000L),
                                                              EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);

    // Starting anomaly handler
    EasyMock.expect(mockDetectorScheduler.submit(EasyMock.isA(AnomalyDetector.AnomalyHandlerTask.class)))
            .andDelegateTo(executorService);
    // Schedule a delayed check
    EasyMock.expect(mockDetectorScheduler.schedule(EasyMock.isA(Runnable.class),
                                                   EasyMock.eq(1000L),
                                                   EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);
    mockDetectorScheduler.shutdown();
    EasyMock.expectLastCall().andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.awaitTermination(3000L, TimeUnit.MILLISECONDS)).andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.isTerminated()).andDelegateTo(executorService);

    // The following state are used to test the delayed check when executor is busy.
    EasyMock.expect(mockKafkaCruiseControl.state())
            .andReturn(new KafkaCruiseControlState(ExecutorState.noTaskInProgress(), null, null));

    EasyMock.replay(mockAnomalyNotifier);
    EasyMock.replay(mockBrokerFailureDetector);
    EasyMock.replay(mockGoalViolationDetector);
    EasyMock.replay(mockDetectorScheduler);
    EasyMock.replay(mockKafkaCruiseControl);

    AnomalyDetector anomalyDetector = new AnomalyDetector(anomalies, 3000L, mockKafkaCruiseControl, mockAnomalyNotifier,
                                                          mockGoalViolationDetector, mockBrokerFailureDetector,
                                                          mockDetectorScheduler);

    try {
      anomalyDetector.startDetection();
      anomalies.add(new BrokerFailures(Collections.singletonMap(0, 100L)));
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
  public void testFix() throws InterruptedException, KafkaCruiseControlException {
    LinkedBlockingDeque<Anomaly> anomalies = new LinkedBlockingDeque<>();
    AnomalyNotifier mockAnomalyNotifier = EasyMock.mock(AnomalyNotifier.class);
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    ScheduledExecutorService mockDetectorScheduler = EasyMock.mock(ScheduledExecutorService.class);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);

    EasyMock.expect(mockAnomalyNotifier.onGoalViolation(EasyMock.isA(GoalViolations.class)))
            .andReturn(AnomalyNotificationResult.fix());

    // Starting periodic goal violation detection.
    EasyMock.expect(mockDetectorScheduler.scheduleAtFixedRate(EasyMock.eq(mockGoalViolationDetector),
                                                              EasyMock.anyLong(),
                                                              EasyMock.eq(3000L),
                                                              EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);

    // Starting anomaly handler
    EasyMock.expect(mockDetectorScheduler.submit(EasyMock.isA(AnomalyDetector.AnomalyHandlerTask.class)))
            .andDelegateTo(executorService);

    mockDetectorScheduler.shutdown();
    EasyMock.expectLastCall().andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.awaitTermination(3000L, TimeUnit.MILLISECONDS)).andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.isTerminated()).andDelegateTo(executorService);

    // The following state are used to test the delayed check when executor is busy.
    EasyMock.expect(mockKafkaCruiseControl.state())
            .andReturn(new KafkaCruiseControlState(ExecutorState.noTaskInProgress(), null, null));
    EasyMock.expect(mockKafkaCruiseControl.rebalance(Collections.emptyList(), false, null))
            .andReturn(null);

    EasyMock.replay(mockAnomalyNotifier);
    EasyMock.replay(mockBrokerFailureDetector);
    EasyMock.replay(mockGoalViolationDetector);
    EasyMock.replay(mockDetectorScheduler);
    EasyMock.replay(mockKafkaCruiseControl);

    AnomalyDetector anomalyDetector = new AnomalyDetector(anomalies, 3000L, mockKafkaCruiseControl, mockAnomalyNotifier,
                                                          mockGoalViolationDetector, mockBrokerFailureDetector,
                                                          mockDetectorScheduler);

    try {
      anomalyDetector.startDetection();
      anomalies.add(new GoalViolations());
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
  public void testExecutionInProgress() throws InterruptedException, KafkaCruiseControlException {
    LinkedBlockingDeque<Anomaly> anomalies = new LinkedBlockingDeque<>();
    AnomalyNotifier mockAnomalyNotifier = EasyMock.mock(AnomalyNotifier.class);
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    ScheduledExecutorService mockDetectorScheduler = EasyMock.mock(ScheduledExecutorService.class);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);

    // Starting periodic goal violation detection.
    EasyMock.expect(mockDetectorScheduler.scheduleAtFixedRate(EasyMock.eq(mockGoalViolationDetector),
                                                              EasyMock.anyLong(),
                                                              EasyMock.eq(3000L),
                                                              EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);

    // Starting anomaly handler
    EasyMock.expect(mockDetectorScheduler.submit(EasyMock.isA(AnomalyDetector.AnomalyHandlerTask.class)))
            .andDelegateTo(executorService);
    // For detector shutdown.
    mockDetectorScheduler.shutdown();
    EasyMock.expectLastCall().andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.awaitTermination(3000L, TimeUnit.MILLISECONDS)).andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.isTerminated()).andDelegateTo(executorService);

    // The following state are used to test the delayed check when executor is busy.
    EasyMock.expect(mockKafkaCruiseControl.state())
            .andReturn(new KafkaCruiseControlState(
                ExecutorState.replicaMovementInProgress(1, Collections.emptySet(), Collections.emptySet(),
                                                        1, 1),
                null, null));

    EasyMock.replay(mockAnomalyNotifier);
    EasyMock.replay(mockBrokerFailureDetector);
    EasyMock.replay(mockGoalViolationDetector);
    EasyMock.replay(mockDetectorScheduler);
    EasyMock.replay(mockKafkaCruiseControl);

    AnomalyDetector anomalyDetector = new AnomalyDetector(anomalies, 3000L, mockKafkaCruiseControl, mockAnomalyNotifier,
                                                          mockGoalViolationDetector, mockBrokerFailureDetector,
                                                          mockDetectorScheduler);

    try {
      anomalyDetector.startDetection();
      anomalies.add(new GoalViolations());
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
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.createNiceMock(KafkaCruiseControl.class);
    ScheduledExecutorService detectorScheduler =
        Executors.newScheduledThreadPool(2, new KafkaCruiseControlThreadFactory("AnomalyDetector", false, null));

    AnomalyDetector anomalyDetector = new AnomalyDetector(new LinkedBlockingDeque<>(), 3000L, mockKafkaCruiseControl,
                                                          mockAnomalyNotifier, mockGoalViolationDetector,
                                                          mockBrokerFailureDetector, detectorScheduler);

    anomalyDetector.shutdown();
    Thread t = new Thread(anomalyDetector::shutdown);
    t.start();
    t.join(30000L);
    assertTrue(detectorScheduler.isTerminated());
  }



}
