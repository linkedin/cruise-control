/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.exception.NotEnoughSnapshotsException;
import com.linkedin.kafka.cruisecontrol.exception.NotEnoughValidSnapshotsException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Load;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampleStore;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.MetricCompletenessChecker;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;
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

  private static final int NUM_SNAPSHOT_WINDOWS = 2;
  private static final int MIN_SAMPLES_PER_SNAPSHOT_WINDOW = 4;
  private static final long SNAPSHOT_WINDOW_MS = 1000;

  private final Time _time = new MockTime(0);

  @Test
  public void testStateWithOnlyActiveSnapshotWindow() {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    MetricSampleAggregator aggregator = context.aggregator();

    // populate the metrics aggregator.
    // two samples for each partition except T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, T0P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, T0P1, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, T1P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, T1P1, 0, SNAPSHOT_WINDOW_MS);

    LoadMonitorState state = loadMonitor.state();
    assertEquals(0, state.numValidMonitoredPartitions());
    assertEquals(0, state.numValidSnapshotWindows());
    assertTrue(state.monitoredSnapshotWindows().isEmpty());
  }

  @Test
  public void testStateWithoutEnoughSnapshotWindows() {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    MetricSampleAggregator aggregator = context.aggregator();

    // populate the metrics aggregator.
    // two samples for each partition except T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, T0P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, T0P1, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, T1P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 1, aggregator, T1P1, 0, SNAPSHOT_WINDOW_MS);

    LoadMonitorState state = loadMonitor.state();
    assertEquals(0, state.numValidMonitoredPartitions());
    assertEquals(0, state.numValidSnapshotWindows());
    assertEquals(1, state.monitoredSnapshotWindows().size());
    assertEquals(0.5, state.monitoredSnapshotWindows().get(SNAPSHOT_WINDOW_MS), 0.0);

    // Back fill for T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, aggregator, T1P1, 0, SNAPSHOT_WINDOW_MS);
    state = loadMonitor.state();
    assertEquals(0, state.numValidMonitoredPartitions());
    assertEquals(1, state.numValidSnapshotWindows());
    assertEquals(1, state.monitoredSnapshotWindows().size());
    assertEquals(1.0, state.monitoredSnapshotWindows().get(SNAPSHOT_WINDOW_MS), 0.0);

  }

  @Test
  public void testStateWithInvalidSnapshotWindows() {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    MetricSampleAggregator aggregator = context.aggregator();

    // populate the metrics aggregator.
    // two samples for each partition except T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, T0P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, T0P1, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, T1P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 1, aggregator, T1P1, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, aggregator, T1P1, 0, SNAPSHOT_WINDOW_MS);

    LoadMonitorState state = loadMonitor.state();
    assertEquals(2, state.numValidMonitoredPartitions());
    assertEquals(0, state.numValidSnapshotWindows());
    assertEquals(2, state.monitoredSnapshotWindows().size());
    assertEquals(1.0, state.monitoredSnapshotWindows().get(SNAPSHOT_WINDOW_MS), 0.0);
    assertEquals(0.5, state.monitoredSnapshotWindows().get(SNAPSHOT_WINDOW_MS * 2), 0.0);


    // Back fill a sample for T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, aggregator, T1P1, 1, SNAPSHOT_WINDOW_MS);
    state = loadMonitor.state();
    assertEquals(4, state.numValidMonitoredPartitions());
    assertEquals(2, state.numValidSnapshotWindows());
    assertEquals(2, state.monitoredSnapshotWindows().size());
    assertEquals(1.0, state.monitoredSnapshotWindows().get(SNAPSHOT_WINDOW_MS), 0.0);
    assertEquals(1.0, state.monitoredSnapshotWindows().get(SNAPSHOT_WINDOW_MS * 2), 0.0);
  }

  @Test
  public void testMeetCompletenessRequirements() {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    MetricSampleAggregator aggregator = context.aggregator();

    ModelCompletenessRequirements requirements1 = new ModelCompletenessRequirements(1, 1.0, false);
    ModelCompletenessRequirements requirements2 = new ModelCompletenessRequirements(1, 0.5, false);
    ModelCompletenessRequirements requirements3 = new ModelCompletenessRequirements(2, 1.0, false);
    ModelCompletenessRequirements requirements4 = new ModelCompletenessRequirements(2, 0.5, false);

    // populate the metrics aggregator.
    // One stable window + one active window, enough samples for each partition except T1P1.
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, T0P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, T0P1, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, T1P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 1, aggregator, T1P1, 0, SNAPSHOT_WINDOW_MS);

    assertFalse(loadMonitor.meetCompletenessRequirements(requirements1));
    assertTrue(loadMonitor.meetCompletenessRequirements(requirements2));
    assertFalse(loadMonitor.meetCompletenessRequirements(requirements3));
    assertFalse(loadMonitor.meetCompletenessRequirements(requirements4));

    // Add more samples, two stable windows + on active window. enough samples for each partition except T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, T0P0, 2, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, T0P1, 2, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, T1P0, 2, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, aggregator, T1P1, 2, SNAPSHOT_WINDOW_MS);
    assertFalse(loadMonitor.meetCompletenessRequirements(requirements1));
    assertTrue(loadMonitor.meetCompletenessRequirements(requirements2));
    assertFalse(loadMonitor.meetCompletenessRequirements(requirements3));
    assertTrue(loadMonitor.meetCompletenessRequirements(requirements4));

    // Back fill the most recent stable window for T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, aggregator, T1P1, 1, SNAPSHOT_WINDOW_MS);
    assertTrue(loadMonitor.meetCompletenessRequirements(requirements1));
    assertTrue(loadMonitor.meetCompletenessRequirements(requirements2));
    assertFalse(loadMonitor.meetCompletenessRequirements(requirements3));
    assertTrue(loadMonitor.meetCompletenessRequirements(requirements4));

    // Back fill all stable windows for T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, aggregator, T1P1, 0, SNAPSHOT_WINDOW_MS);
    assertTrue(loadMonitor.meetCompletenessRequirements(requirements1));
    assertTrue(loadMonitor.meetCompletenessRequirements(requirements2));
    assertTrue(loadMonitor.meetCompletenessRequirements(requirements3));
    assertTrue(loadMonitor.meetCompletenessRequirements(requirements4));
  }

  // Test the case with enough snapshot windows and valid partitions.
  @Test
  public void testBasicClusterModel()
      throws NotEnoughSnapshotsException, ModelInputException, NotEnoughValidSnapshotsException {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    MetricSampleAggregator aggregator = context.aggregator();

    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, T0P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, T0P1, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, T1P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, T1P1, 0, SNAPSHOT_WINDOW_MS);

    ClusterModel clusterModel = loadMonitor.clusterModel(-1, Long.MAX_VALUE,
                                                         new ModelCompletenessRequirements(2, 1.0, false));
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
  }

  // Not enough snapshot windows and some partitions are missing from all snapshot windows.
  @Test
  public void testClusterModelWithInvalidPartitionAndInsufficientSnapshotWindows()
      throws NotEnoughSnapshotsException, ModelInputException, NotEnoughValidSnapshotsException {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    MetricSampleAggregator aggregator = context.aggregator();

    ModelCompletenessRequirements requirements1 = new ModelCompletenessRequirements(1, 1.0, false);
    ModelCompletenessRequirements requirements2 = new ModelCompletenessRequirements(1, 0.5, false);
    ModelCompletenessRequirements requirements3 = new ModelCompletenessRequirements(2, 1.0, false);
    ModelCompletenessRequirements requirements4 = new ModelCompletenessRequirements(2, 0.5, false);

    // populate the metrics aggregator.
    // two samples for each partition except T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, T0P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, T0P1, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, T1P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 1, aggregator, T1P1, 0, SNAPSHOT_WINDOW_MS);

    try {
      loadMonitor.clusterModel(-1, Long.MAX_VALUE, requirements1);
      fail("Should have thrown NotEnoughValidSnapshotsException.");
    } catch (NotEnoughValidSnapshotsException nevse) {
      // let it go
    }

    ClusterModel clusterModel = loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements2);
    assertNull(clusterModel.partition(T1P0));
    assertNull(clusterModel.partition(T1P1));
    assertEquals(1, clusterModel.partition(T0P0).leader().load().numSnapshots());
    assertEquals(3, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(1.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(1.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(1.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);

    try {
      loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements3);
      fail("Should have thrown NotEnoughValidSnapshotsException.");
    } catch (NotEnoughValidSnapshotsException nevse) {
      // let it go
    }

    try {
      loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements4);
      fail("Should have thrown NotEnoughValidSnapshotsException.");
    } catch (NotEnoughValidSnapshotsException nevse) {
      // let it go
    }
  }

  // Enough snapshot windows, some partitions are invalid in all snapshot windows.
  @Test
  public void testClusterWithInvalidPartitions()
      throws NotEnoughSnapshotsException, ModelInputException, NotEnoughValidSnapshotsException {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    MetricSampleAggregator aggregator = context.aggregator();

    ModelCompletenessRequirements requirements1 = new ModelCompletenessRequirements(1, 1.0, false);
    ModelCompletenessRequirements requirements2 = new ModelCompletenessRequirements(1, 0.5, false);
    ModelCompletenessRequirements requirements3 = new ModelCompletenessRequirements(2, 1.0, false);
    ModelCompletenessRequirements requirements4 = new ModelCompletenessRequirements(2, 0.5, false);

    // populate the metrics aggregator.
    // two samples for each partition except T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, T0P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, T0P1, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, T1P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 1, aggregator, T1P1, 0, SNAPSHOT_WINDOW_MS);

    try {
      loadMonitor.clusterModel(-1, Long.MAX_VALUE, requirements1);
      fail("Should have thrown NotEnoughValidSnapshotsException.");
    } catch (NotEnoughValidSnapshotsException nevse) {
      // let it go
    }

    ClusterModel clusterModel = loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements2);
    assertNull(clusterModel.partition(T1P0));
    assertNull(clusterModel.partition(T1P1));
    assertEquals(2, clusterModel.partition(T0P0).leader().load().numSnapshots());
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);

    try {
      loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements3);
      fail("Should have thrown NotEnoughValidSnapshotsException.");
    } catch (NotEnoughValidSnapshotsException nevse) {
      // let it go
    }

    clusterModel = loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements4);
    assertNull(clusterModel.partition(T1P0));
    assertNull(clusterModel.partition(T1P1));
    assertEquals(2, clusterModel.partition(T0P0).leader().load().numSnapshots());
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);
  }

  // Enough snapshot windows, some partitions are not available in some snapshot windows.
  @Test
  public void testClusterModelWithPartlyInvalidPartitions()
      throws NotEnoughSnapshotsException, ModelInputException, NotEnoughValidSnapshotsException {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    MetricSampleAggregator aggregator = context.aggregator();

    ModelCompletenessRequirements requirements1 = new ModelCompletenessRequirements(1, 1.0, false);
    ModelCompletenessRequirements requirements2 = new ModelCompletenessRequirements(1, 0.5, false);
    ModelCompletenessRequirements requirements3 = new ModelCompletenessRequirements(2, 1.0, false);
    ModelCompletenessRequirements requirements4 = new ModelCompletenessRequirements(2, 0.5, false);

    // populate the metrics aggregator.
    // two samples for each partition except T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, T0P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, T0P1, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, T1P0, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 1, aggregator, T1P1, 0, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, aggregator, T1P1, 1, SNAPSHOT_WINDOW_MS);

    ClusterModel clusterModel =  loadMonitor.clusterModel(-1, Long.MAX_VALUE, requirements1);
    for (TopicPartition tp : Arrays.asList(T0P0, T0P1, T1P0, T1P1)) {
      assertNotNull(clusterModel.partition(tp));
    }
    assertEquals(1, clusterModel.partition(T0P0).leader().load().numSnapshots());
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(11.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(11.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(11.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);

    assertEquals(10, clusterModel.partition(T1P1).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(10, clusterModel.partition(T1P1).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(10, clusterModel.partition(T1P1).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(10, clusterModel.partition(T1P1).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);

    clusterModel = loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements2);
    assertNull(clusterModel.partition(T1P0));
    assertNull(clusterModel.partition(T1P1));
    assertEquals(2, clusterModel.partition(T0P0).leader().load().numSnapshots());
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);

    try {
      loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements3);
      fail("Should have thrown NotEnoughValidSnapshotsException.");
    } catch (NotEnoughValidSnapshotsException nevse) {
      // let it go
    }

    clusterModel = loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements4);
    assertNull(clusterModel.partition(T1P0));
    assertNull(clusterModel.partition(T1P1));
    assertEquals(2, clusterModel.partition(T0P0).leader().load().numSnapshots());
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);
  }

  private TestContext prepareContext() {
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
    Properties props = CruiseControlUnitTestUtils.getCruiseControlProperties();
    props.put(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG, Integer.toString(NUM_SNAPSHOT_WINDOWS));
    props.put(KafkaCruiseControlConfig.MIN_SAMPLES_PER_LOAD_SNAPSHOT_CONFIG,
              Integer.toString(MIN_SAMPLES_PER_SNAPSHOT_WINDOW));
    props.put(KafkaCruiseControlConfig.LOAD_SNAPSHOT_WINDOW_MS_CONFIG, Long.toString(SNAPSHOT_WINDOW_MS));
    props.put(KafkaCruiseControlConfig.SAMPLE_STORE_CLASS_CONFIG, NoopSampleStore.class.getName());
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);
    LoadMonitor loadMonitor = new LoadMonitor(config, mockMetadataClient, _time, new MetricRegistry());

    MetricSampleAggregator aggregator = loadMonitor.aggregator();
    MetricCompletenessChecker checker = loadMonitor.completenessChecker();

    if (!Load.initialized()) {
      Load.init(config);
    }
    ModelParameters.init(config);
    loadMonitor.startUp();
    while (loadMonitor.state().state() != LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        // let it go.
      }
    }

    return new TestContext(loadMonitor, aggregator, checker, config, metadata);
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
    Metadata metadata = new Metadata();
    metadata.update(cluster, Collections.emptySet(), 0);
    return metadata;
  }

  private static class TestContext {
    private final LoadMonitor _loadMonitor;
    private final MetricSampleAggregator _aggregator;
    private final MetricCompletenessChecker _completenessChecker;
    private final KafkaCruiseControlConfig _config;
    private final Metadata _metadata;

    private TestContext(LoadMonitor loadMonitor,
                        MetricSampleAggregator aggregator,
                        MetricCompletenessChecker completenessChecker,
                        KafkaCruiseControlConfig config,
                        Metadata metadata) {
      _loadMonitor = loadMonitor;
      _aggregator = aggregator;
      _completenessChecker = completenessChecker;
      _config = config;
      _metadata = metadata;
    }

    private LoadMonitor loadmonitor() {
      return _loadMonitor;
    }

    private MetricSampleAggregator aggregator() {
      return _aggregator;
    }

    private MetricCompletenessChecker completenessChecker() {
      return _completenessChecker;
    }

    private KafkaCruiseControlConfig config() {
      return _config;
    }

    private Metadata metadata() {
      return _metadata;
    }
  }
}
