/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampleStore;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.PartitionEntity;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.easymock.EasyMock.anyLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Unit test for LoadMonitor
 */
public class LoadMonitorTest {
  private static final String TOPIC_0 = "topic0";
  private static final String TOPIC_1 = "topic1";
  private static final int P0 = 0;
  private static final int P1 = 1;
  private static final TopicPartition T0P0 = new TopicPartition(TOPIC_0, P0);
  private static final TopicPartition T0P1 = new TopicPartition(TOPIC_0, P1);
  private static final TopicPartition T1P0 = new TopicPartition(TOPIC_1, P0);
  private static final TopicPartition T1P1 = new TopicPartition(TOPIC_1, P1);
  private static final PartitionEntity PE_T0P0 = new PartitionEntity(T0P0);
  private static final PartitionEntity PE_T0P1 = new PartitionEntity(T0P1);
  private static final PartitionEntity PE_T1P0 = new PartitionEntity(T1P0);
  private static final PartitionEntity PE_T1P1 = new PartitionEntity(T1P1);
  private static final MetricDef METRIC_DEF = KafkaMetricDef.commonMetricDef();

  private static final int NUM_WINDOWS = 2;
  private static final int MIN_SAMPLES_PER_WINDOW = 4;
  private static final long WINDOW_MS = 1000;
  private static final String DEFAULT_CLEANUP_POLICY = "delete";

  private final Time _time = new MockTime(0);

  @Test
  public void testStateWithOnlyActiveSnapshotWindow() {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    // populate the metrics aggregator.
    // four samples for each partition
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P0, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P1, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P0, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P1, 0, WINDOW_MS, METRIC_DEF);

    MetadataClient.ClusterAndGeneration clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    LoadMonitorState state = loadMonitor.state(new OperationProgress(), clusterAndGeneration);
    // The load monitor only has an active window. There is no stable window.
    assertEquals(0, state.numValidPartitions());
    assertEquals(0, state.numValidWindows());
    assertTrue(state.monitoredWindows().isEmpty());
  }

  @Test
  public void testStateWithoutEnoughSnapshotWindows() {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    // populate the metrics aggregator.
    // four samples for each partition except T1P1. T1P1 has no sample in the first window and one in the second window.
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T0P0, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T0P1, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T1P0, 0, WINDOW_MS, METRIC_DEF);

    MetadataClient.ClusterAndGeneration clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    LoadMonitorState state = loadMonitor.state(new OperationProgress(), clusterAndGeneration);
    // The load monitor has 1 stable window with 0.5 of valid partitions ratio.
    assertEquals(0, state.numValidPartitions());
    assertEquals(0, state.numValidWindows());
    assertEquals(1, state.monitoredWindows().size());
    assertEquals(0.5, state.monitoredWindows().get(WINDOW_MS), 0.0);

    // Back fill for T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, aggregator, PE_T1P1, 0, WINDOW_MS, METRIC_DEF);
    clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    state = loadMonitor.state(new OperationProgress(), clusterAndGeneration);
    // The load monitor now has one stable window with 1.0 of valid partitions ratio.
    assertEquals(0, state.numValidPartitions());
    assertEquals(1, state.numValidWindows());
    assertEquals(1, state.monitoredWindows().size());
    assertEquals(1.0, state.monitoredWindows().get(WINDOW_MS), 0.0);
  }

  @Test
  public void testStateWithInvalidSnapshotWindows() {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    // populate the metrics aggregator.
    // four samples for each partition except T1P1. T1P1 has 2 samples in the first window, and 2 samples in the
    // active window.
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P0, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P1, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T1P0, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 2, aggregator, PE_T1P1, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 2, aggregator, PE_T1P1, 2, WINDOW_MS, METRIC_DEF);

    MetadataClient.ClusterAndGeneration clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    LoadMonitorState state = loadMonitor.state(new OperationProgress(), clusterAndGeneration);
    // Both partitions for topic 0 should be valid.
    assertEquals(2, state.numValidPartitions());
    // Both topic should be valid in the first window.
    assertEquals(1, state.numValidWindows());
    // There should be 2 monitored windows.
    assertEquals(2, state.monitoredWindows().size());
    // Both topic should be valid in the first window.
    assertEquals(1.0, state.monitoredWindows().get(WINDOW_MS), 0.0);
    // Only topic 2 is valid in the second window.
    assertEquals(0.5, state.monitoredWindows().get(WINDOW_MS * 2), 0.0);

    // Back fill 3 samples for T1P1 in the second window.
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 3, aggregator, PE_T1P1, 1, WINDOW_MS, METRIC_DEF);
    clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    state = loadMonitor.state(new OperationProgress(), clusterAndGeneration);
    // All the partitions should be valid now.
    assertEquals(4, state.numValidPartitions());
    // All the windows should be valid now.
    assertEquals(2, state.numValidWindows());
    // There should be two monitored windows.
    assertEquals(2, state.monitoredWindows().size());
    // Both monitored windows should have 100% completeness.
    assertEquals(1.0, state.monitoredWindows().get(WINDOW_MS), 0.0);
    assertEquals(1.0, state.monitoredWindows().get(WINDOW_MS * 2), 0.0);
  }

  @Test
  public void testMeetCompletenessRequirements() {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    // Require at least 1 valid window with 1.0 of valid partitions ratio.
    ModelCompletenessRequirements requirements1 = new ModelCompletenessRequirements(1, 1.0, false);
    // Require at least 1 valid window with 0.5 of valid partitions ratio.
    ModelCompletenessRequirements requirements2 = new ModelCompletenessRequirements(1, 0.5, false);
    // Require at least 2 valid windows with 1.0 of valid partitions ratio.
    ModelCompletenessRequirements requirements3 = new ModelCompletenessRequirements(2, 1.0, false);
    // Require at least 2 valid windows with 0.5 of valid partitions ratio.
    ModelCompletenessRequirements requirements4 = new ModelCompletenessRequirements(2, 0.5, false);

    // populate the metrics aggregator.
    // One stable window + one active window, enough samples for each partition except T1P1.
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T0P0, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T0P1, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T1P0, 0, WINDOW_MS, METRIC_DEF);
    // The load monitor has one window with 0.5 valid partitions ratio.
    MetadataClient.ClusterAndGeneration clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    assertFalse(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements1));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements2));
    assertFalse(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements3));
    assertFalse(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements4));

    // Add more samples, two stable windows + one active window. enough samples for each partition except T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P0, 2, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P1, 2, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P0, 2, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, aggregator, PE_T1P1, 2, WINDOW_MS, METRIC_DEF);
    // The load monitor has two windows, both with 0.5 valid partitions ratio
    clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    assertFalse(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements1));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements2));
    assertFalse(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements3));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements4));

    // Back fill the first stable window for T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, aggregator, PE_T1P1, 0, WINDOW_MS, METRIC_DEF);
    // The load monitor has two windows with 1.0 and 0.5 of completeness respectively.
    clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements1));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements2));
    assertFalse(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements3));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements4));

    // Back fill all stable windows for T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 3, aggregator, PE_T1P1, 1, WINDOW_MS, METRIC_DEF);
    // The load monitor has two windows both with 1.0 of completeness.
    clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements1));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements2));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements3));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements4));
  }

  // Test the case with enough snapshot windows and valid partitions.
  @Test
  public void testBasicClusterModel() throws NotEnoughValidWindowsException {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P0, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P1, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T1P0, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T1P1, 0, WINDOW_MS, METRIC_DEF);

    ClusterModel clusterModel = loadMonitor.clusterModel(-1, Long.MAX_VALUE,
                                                         new ModelCompletenessRequirements(2, 1.0, false),
                                                         new OperationProgress());
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
  }

  // Not enough snapshot windows and some partitions are missing from all snapshot windows.
  @Test
  public void testClusterModelWithInvalidPartitionAndInsufficientSnapshotWindows()
      throws NotEnoughValidWindowsException {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    ModelCompletenessRequirements requirements1 = new ModelCompletenessRequirements(1, 1.0, false);
    ModelCompletenessRequirements requirements2 = new ModelCompletenessRequirements(1, 0.5, false);
    ModelCompletenessRequirements requirements3 = new ModelCompletenessRequirements(2, 1.0, false);
    ModelCompletenessRequirements requirements4 = new ModelCompletenessRequirements(2, 0.5, false);

    // populate the metrics aggregator.
    // two samples for each partition except T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T0P0, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T0P1, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T1P0, 0, WINDOW_MS, METRIC_DEF);

    try {
      loadMonitor.clusterModel(-1, Long.MAX_VALUE, requirements1, new OperationProgress());
      fail("Should have thrown NotEnoughValidWindowsException.");
    } catch (NotEnoughValidWindowsException nevwe) {
      // let it go
    }

    ClusterModel clusterModel = loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements2, new OperationProgress());
    assertNull(clusterModel.partition(T1P0));
    assertNull(clusterModel.partition(T1P1));
    assertEquals(1, clusterModel.partition(T0P0).leader().load().numWindows());
    assertEquals(3, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(1.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(3.0, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(3.0, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);

    try {
      loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements3, new OperationProgress());
      fail("Should have thrown NotEnoughValidWindowsException.");
    } catch (NotEnoughValidWindowsException nevwe) {
      // let it go
    }

    try {
      loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements4, new OperationProgress());
      fail("Should have thrown NotEnoughValidWindowsException.");
    } catch (NotEnoughValidWindowsException nevwe) {
      // let it go
    }
  }

  // Enough snapshot windows, some partitions are invalid in all snapshot windows.
  @Test
  public void testClusterWithInvalidPartitions() throws NotEnoughValidWindowsException {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    ModelCompletenessRequirements requirements1 = new ModelCompletenessRequirements(1, 1.0, false);
    ModelCompletenessRequirements requirements2 = new ModelCompletenessRequirements(1, 0.5, false);
    ModelCompletenessRequirements requirements3 = new ModelCompletenessRequirements(2, 1.0, false);
    ModelCompletenessRequirements requirements4 = new ModelCompletenessRequirements(2, 0.5, false);

    // populate the metrics aggregator.
    // two samples for each partition except T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P0, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P1, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T1P0, 0, WINDOW_MS, METRIC_DEF);

    try {
      loadMonitor.clusterModel(-1, Long.MAX_VALUE, requirements1, new OperationProgress());
      fail("Should have thrown NotEnoughValidWindowsException.");
    } catch (NotEnoughValidWindowsException nevwe) {
      // let it go
    }

    ClusterModel clusterModel = loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements2, new OperationProgress());
    assertNull(clusterModel.partition(T1P0));
    assertNull(clusterModel.partition(T1P1));
    assertEquals(2, clusterModel.partition(T0P0).leader().load().numWindows());
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);

    try {
      loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements3, new OperationProgress());
      fail("Should have thrown NotEnoughValidWindowsException.");
    } catch (NotEnoughValidWindowsException nevwe) {
      // let it go
    }

    clusterModel = loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements4, new OperationProgress());
    assertNull(clusterModel.partition(T1P0));
    assertNull(clusterModel.partition(T1P1));
    assertEquals(2, clusterModel.partition(T0P0).leader().load().numWindows());
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);
  }

  // Enough snapshot windows, some partitions are not available in some snapshot windows.
  @Test
  public void testClusterModelWithPartlyInvalidPartitions() throws NotEnoughValidWindowsException {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    ModelCompletenessRequirements requirements1 = new ModelCompletenessRequirements(1, 1.0, false);
    ModelCompletenessRequirements requirements2 = new ModelCompletenessRequirements(1, 0.5, false);
    ModelCompletenessRequirements requirements3 = new ModelCompletenessRequirements(2, 1.0, false);
    ModelCompletenessRequirements requirements4 = new ModelCompletenessRequirements(2, 0.5, false);

    // populate the metrics aggregator.
    // four samples for each partition except T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P0, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P1, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T1P0, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, aggregator, PE_T1P1, 1, WINDOW_MS, METRIC_DEF);

    ClusterModel clusterModel =  loadMonitor.clusterModel(-1, Long.MAX_VALUE, requirements1, new OperationProgress());
    for (TopicPartition tp : Arrays.asList(T0P0, T0P1, T1P0, T1P1)) {
      assertNotNull(clusterModel.partition(tp));
    }
    assertEquals(1, clusterModel.partition(T0P0).leader().load().numWindows());
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(11.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(23, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(23, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);

    assertEquals(10, clusterModel.partition(T1P1).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(10, clusterModel.partition(T1P1).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(20, clusterModel.partition(T1P1).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(20, clusterModel.partition(T1P1).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);

    clusterModel = loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements2, new OperationProgress());
    assertNull(clusterModel.partition(T1P0));
    assertNull(clusterModel.partition(T1P1));
    assertEquals(2, clusterModel.partition(T0P0).leader().load().numWindows());
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(13.0, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(13.0, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);

    try {
      loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements3, new OperationProgress());
      fail("Should have thrown NotEnoughValidWindowsException.");
    } catch (NotEnoughValidWindowsException nevwe) {
      // let it go
    }

    clusterModel = loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements4, new OperationProgress());
    assertNull(clusterModel.partition(T1P0));
    assertNull(clusterModel.partition(T1P1));
    assertEquals(2, clusterModel.partition(T0P0).leader().load().numWindows());
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);
  }

  @Test
  public void testClusterModelWithInvalidSnapshotWindows() throws NotEnoughValidWindowsException {
    TestContext context = prepareContext(4);
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P0, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P1, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P0, 0, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P1, 0, WINDOW_MS, METRIC_DEF);

    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P0, 3, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P1, 3, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P0, 3, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P1, 3, WINDOW_MS, METRIC_DEF);

    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P0, 4, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P1, 4, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P0, 4, WINDOW_MS, METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P1, 4, WINDOW_MS, METRIC_DEF);

    ClusterModel clusterModel = loadMonitor.clusterModel(-1, Long.MAX_VALUE,
        new ModelCompletenessRequirements(2, 0, false),
        new OperationProgress());

    assertEquals(2, clusterModel.partition(T0P0).leader().load().numWindows());
    assertEquals(16.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(2, clusterModel.partition(T0P1).leader().load().numWindows());
    assertEquals(33, clusterModel.partition(T0P1).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(2, clusterModel.partition(T1P0).leader().load().numWindows());
    assertEquals(33, clusterModel.partition(T1P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(2, clusterModel.partition(T1P1).leader().load().numWindows());
    assertEquals(33, clusterModel.partition(T1P1).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);
  }

  private TestContext prepareContext() {
    return prepareContext(NUM_WINDOWS);
  }

  private TestContext prepareContext(int numWindowToPreserve) {
    // Create mock metadata client.
    Metadata metadata = getMetadata(Arrays.asList(T0P0, T0P1, T1P0, T1P1));
    MetadataClient mockMetadataClient = EasyMock.mock(MetadataClient.class);
    EasyMock.expect(mockMetadataClient.cluster())
            .andReturn(metadata.fetch())
            .anyTimes();
    EasyMock.expect(mockMetadataClient.clusterAndGeneration())
            .andReturn(new MetadataClient.ClusterAndGeneration(metadata.fetch(), 0))
            .anyTimes();
    EasyMock.expect(mockMetadataClient.metadata()).andReturn(metadata).anyTimes();
    EasyMock.expect(mockMetadataClient.refreshMetadata())
            .andReturn(new MetadataClient.ClusterAndGeneration(metadata.fetch(), 0))
            .anyTimes();
    EasyMock.expect(mockMetadataClient.refreshMetadata(anyLong()))
            .andReturn(new MetadataClient.ClusterAndGeneration(metadata.fetch(), 0))
            .anyTimes();
    EasyMock.replay(mockMetadataClient);

    // create load monitor.
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.put(KafkaCruiseControlConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG, Integer.toString(numWindowToPreserve));
    props.put(KafkaCruiseControlConfig.MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG,
              Integer.toString(MIN_SAMPLES_PER_WINDOW));
    props.put(KafkaCruiseControlConfig.PARTITION_METRICS_WINDOW_MS_CONFIG, Long.toString(WINDOW_MS));
    props.put("cleanup.policy", DEFAULT_CLEANUP_POLICY);
    props.put(KafkaCruiseControlConfig.SAMPLE_STORE_CLASS_CONFIG, NoopSampleStore.class.getName());
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);
    LoadMonitor loadMonitor = new LoadMonitor(config, mockMetadataClient, _time, new MetricRegistry(), METRIC_DEF);

    KafkaPartitionMetricSampleAggregator aggregator = loadMonitor.partitionSampleAggregator();

    ModelParameters.init(config);
    loadMonitor.startUp();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    while (loadMonitor.state(new OperationProgress(), clusterAndGeneration).state() != LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        // let it go.
      }
    }

    return new TestContext(loadMonitor, aggregator, config, metadata);
  }

  private Metadata getMetadata(Collection<TopicPartition> partitions) {
    Node node0 = new Node(0, "localhost", 100, "rack0");
    Node node1 = new Node(1, "localhost", 100, "rack1");
    Node[] nodes = {node0, node1};
    Set<Node> allNodes = new HashSet<>();
    allNodes.add(node0);
    allNodes.add(node1);
    Set<PartitionInfo> parts = new HashSet<>();
    for (TopicPartition tp : partitions) {
      parts.add(new PartitionInfo(tp.topic(), tp.partition(), node0, nodes, nodes));
    }
    Cluster cluster = new Cluster("cluster-id", allNodes, parts, Collections.emptySet(), Collections.emptySet());
    Metadata metadata = new Metadata(10, 10, false);
    metadata.update(cluster, Collections.emptySet(), 0);
    return metadata;
  }

  private static class TestContext {
    private final LoadMonitor _loadMonitor;
    private final KafkaPartitionMetricSampleAggregator _aggregator;
    private final KafkaCruiseControlConfig _config;
    private final Metadata _metadata;

    private TestContext(LoadMonitor loadMonitor,
                        KafkaPartitionMetricSampleAggregator aggregator,
                        KafkaCruiseControlConfig config,
                        Metadata metadata) {
      _loadMonitor = loadMonitor;
      _aggregator = aggregator;
      _config = config;
      _metadata = metadata;
    }

    private LoadMonitor loadmonitor() {
      return _loadMonitor;
    }

    private KafkaPartitionMetricSampleAggregator aggregator() {
      return _aggregator;
    }

    private KafkaCruiseControlConfig config() {
      return _config;
    }

    private Metadata metadata() {
      return _metadata;
    }
  }
}
