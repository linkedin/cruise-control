/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorManager;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.CLIENT_REQUEST_TIMEOUT_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertNotNull;


public class KafkaCruiseControlTest extends CruiseControlIntegrationTestHarness {

  @Before
  public void setup() throws Exception {
    super.start();
  }

  @After
  public void teardown() {
    super.stop();
  }

  @Test
  public void testSanityCheckDryRun() throws InterruptedException, ExecutionException, TimeoutException {
    Time time = EasyMock.mock(Time.class);
    AnomalyDetectorManager anomalyDetectorManager = EasyMock.mock(AnomalyDetectorManager.class);
    Executor executor = EasyMock.strictMock(Executor.class);
    LoadMonitor loadMonitor = EasyMock.mock(LoadMonitor.class);
    ExecutorService goalOptimizerExecutor = EasyMock.mock(ExecutorService.class);
    GoalOptimizer goalOptimizer = EasyMock.mock(GoalOptimizer.class);

    // For sanityCheckDryRun(false, true) and sanityCheckDryRun(false, false) (see #1 and #2 below).
    EasyMock.expect(executor.hasOngoingExecution()).andReturn(true).times(2);
    // For sanityCheckDryRun(false, XXX) (see #3 below)
    EasyMock.expect(executor.hasOngoingExecution()).andReturn(false).once();
    EasyMock.expect(executor.hasOngoingPartitionReassignments()).andReturn(true);
    // For sanityCheckDryRun(false, XXX) (see #4 below)
    EasyMock.expect(executor.hasOngoingExecution()).andReturn(false).once();
    EasyMock.expect(executor.hasOngoingPartitionReassignments()).andReturn(false);
    EasyMock.expect(executor.hasOngoingLeaderElection()).andReturn(true);
    // For sanityCheckDryRun(false, XXX) (see #5 below)
    EasyMock.expect(executor.hasOngoingExecution()).andReturn(false).once();
    EasyMock.expect(executor.hasOngoingPartitionReassignments()).andReturn(false);
    EasyMock.expect(executor.hasOngoingLeaderElection()).andReturn(false);

    EasyMock.replay(time, anomalyDetectorManager, executor, loadMonitor, goalOptimizerExecutor, goalOptimizer);
    KafkaCruiseControl kafkaCruiseControl = new KafkaCruiseControl(_config, time, anomalyDetectorManager, executor,
                                                                   loadMonitor, goalOptimizerExecutor, goalOptimizer);

    // Expect no failure (dryrun = true) regardless of ongoing executions.
    kafkaCruiseControl.sanityCheckDryRun(true, false);
    kafkaCruiseControl.sanityCheckDryRun(true, true);
    // 1. Expect no failure (dryrun = false), if there is ongoing execution started by CC, it must be requested to stop.
    kafkaCruiseControl.sanityCheckDryRun(false, true);
    // 2. Expect failure (dryrun = false), if there is ongoing execution started by CC, not requested to stop.
    assertThrows(IllegalStateException.class, () -> kafkaCruiseControl.sanityCheckDryRun(false, false));
    // 3. Expect failure (dryrun = false), there is no execution started by CC, but ongoing replica reassignment, request to stop is irrelevant.
    assertThrows(IllegalStateException.class, () -> kafkaCruiseControl.sanityCheckDryRun(false, false));
    // 4. Expect failure (dryrun = false), there is no execution started by CC, but ongoing leader election, request to stop is irrelevant.
    assertThrows(IllegalStateException.class, () -> kafkaCruiseControl.sanityCheckDryRun(false, false));
    // 5. Expect failure (dryrun = false), there is no execution started by CC or other tools, request to stop is irrelevant.
    kafkaCruiseControl.sanityCheckDryRun(false, false);

    EasyMock.verify(time, anomalyDetectorManager, executor, loadMonitor, goalOptimizerExecutor, goalOptimizer);

    // Verify initialization and functioning of Admin Client
    AdminClient adminClient = kafkaCruiseControl.adminClient();
    assertNotNull(adminClient);
    assertEquals(clusterSize(), adminClient.describeCluster().nodes().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).size());
  }
}
