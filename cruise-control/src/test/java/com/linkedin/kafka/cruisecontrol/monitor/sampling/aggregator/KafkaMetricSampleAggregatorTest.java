/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.cruisecontrol.resource.Resource;
import com.linkedin.cruisecontrol.exception.NotEnoughSnapshotsException;
import com.linkedin.kafka.cruisecontrol.exception.NotEnoughValidSnapshotsException;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.PartitionMetricSample;
import com.linkedin.cruisecontrol.monitor.sampling.Snapshot;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.*;


/**
 * Unit test for {@link KafkaMetricSampleAggregator}.
 */
public class KafkaMetricSampleAggregatorTest {
  private static final String TOPIC = "topic";
  private static final int PARTITION = 0;
  private static final int NUM_SNAPSHOTS = 20;
  private static final long SNAPSHOT_WINDOW_MS = 1000L;
  private static final int MIN_SAMPLES_PER_LOAD_SNAPSHOT = 4;
  private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);

  @Test
  public void testRecentSnapshot() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    KafkaMetricSampleAggregator metricSampleAggregator = new KafkaMetricSampleAggregator(config, metadata);

    populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, metricSampleAggregator);

    try {
      Map<TopicPartition, Snapshot[]> snapshotsForPartition =
          metricSampleAggregator.recentSnapshots(clusterAndGeneration(metadata.fetch()), Long.MAX_VALUE).snapshots();
      assertEquals("The snapshots should only have one partition", 1, snapshotsForPartition.size());
      Snapshot[] snapshots = snapshotsForPartition.get(TP);
      assertNotNull(snapshots);
      assertEquals(NUM_SNAPSHOTS, snapshots.length);
      for (int i = 0; i < NUM_SNAPSHOTS; i++) {
        assertEquals((NUM_SNAPSHOTS - i) * SNAPSHOT_WINDOW_MS, snapshots[i].time());
        for (Resource resource : Resource.values()) {
          double expectedValue = resource == Resource.DISK ?
              (NUM_SNAPSHOTS - 1 - i) * 10 + MIN_SAMPLES_PER_LOAD_SNAPSHOT - 1 :
              (NUM_SNAPSHOTS - 1 - i) * 10 + (MIN_SAMPLES_PER_LOAD_SNAPSHOT - 1) / 2.0;
          assertEquals("The utilization for " + resource + " should be " + expectedValue,
              expectedValue, snapshots[i].utilizationFor(resource), 0);
        }
      }
    } catch (NotEnoughSnapshotsException e) {
      fail("Should not have exception " + e.toString());
    }

    // Verify the metric completeness checker state
    MetadataClient.ClusterAndGeneration clusterAndGeneration =
        new MetadataClient.ClusterAndGeneration(metadata.fetch(), 1);
    assertEquals(NUM_SNAPSHOTS, metricSampleAggregator.validSnapshotWindows(clusterAndGeneration, 1.0, 1).size());
    Map<Long, Double> monitoredPercentages = metricSampleAggregator.monitoredPercentagesByWindows(clusterAndGeneration, 1);
    for (double percentage : monitoredPercentages.values()) {
      assertEquals(1.0, percentage, 0.0);
    }
    assertEquals(NUM_SNAPSHOTS, metricSampleAggregator.availableSnapshotWindows(-1, Long.MAX_VALUE).size());
  }

  @Test
  public void testSnapshotWithUpdatedCluster() throws NotEnoughSnapshotsException, NotEnoughValidSnapshotsException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    KafkaMetricSampleAggregator metricSampleAggregator = new KafkaMetricSampleAggregator(config, metadata);

    populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, metricSampleAggregator);

    TopicPartition tp1 = new TopicPartition(TOPIC, 1);
    Cluster cluster = getCluster(Arrays.asList(TP, tp1));
    metadata.update(cluster, 1);

    Map<TopicPartition, Snapshot[]> snapshotsForPartition =
        metricSampleAggregator.recentSnapshots(clusterAndGeneration(cluster), Long.MAX_VALUE).snapshots();
    assertTrue("tp1 should not be included because recent snapshot does not include all topics",
               snapshotsForPartition.isEmpty());

    ModelCompletenessRequirements requirements =
        new ModelCompletenessRequirements(1, 0.0, true);
    MetricSampleAggregationResult result =
        metricSampleAggregator.snapshots(clusterAndGeneration(cluster), -1, Long.MAX_VALUE, requirements);
    snapshotsForPartition = result.snapshots();
    assertNotNull("tp1 should be included because includeAllTopics is set to true",
                  snapshotsForPartition.get(tp1));
    List<MetricSampleAggregationResult.SampleFlaw> sampleFlaws = result.sampleFlaw(tp1);
    assertEquals(NUM_SNAPSHOTS, sampleFlaws.size());

    for (int i = 0; i < NUM_SNAPSHOTS; i++) {
      MetricSampleAggregationResult.SampleFlaw sampleFlaw = sampleFlaws.get(i);
      assertEquals((NUM_SNAPSHOTS - i) * SNAPSHOT_WINDOW_MS, sampleFlaw.snapshotWindow());
      assertEquals(SnapshotAndImputation.Imputation.NO_VALID_IMPUTATION, sampleFlaw.imputation());
    }
  }

  @Test
  public void testSnapshotWithPartitionImputation() throws NotEnoughSnapshotsException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    KafkaMetricSampleAggregator metricSampleAggregator = new KafkaMetricSampleAggregator(config, metadata);

    TopicPartition tp1 = new TopicPartition(TOPIC, 1);
    Cluster cluster = getCluster(Arrays.asList(TP, tp1));
    metadata.update(cluster, 1);
    populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, metricSampleAggregator);
    //Populate partition 1 but leave 1 hole at NUM_SNAPSHOT'th window.
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 2,
                                                        MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        metricSampleAggregator,
                                                        tp1,
                                                        0,
                                                        SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(2,
                                                        MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        metricSampleAggregator,
                                                        tp1,
                                                        NUM_SNAPSHOTS - 1,
                                                        SNAPSHOT_WINDOW_MS);
    MetricSampleAggregationResult result = metricSampleAggregator.recentSnapshots(clusterAndGeneration(cluster), Long.MAX_VALUE);
    assertEquals(2, result.snapshots().size());
    assertTrue(result.sampleFlaw(TP).isEmpty());
    assertEquals(1, result.sampleFlaw(tp1).size());
    assertEquals((NUM_SNAPSHOTS - 1) * SNAPSHOT_WINDOW_MS, result.sampleFlaw(tp1).get(0).snapshotWindow());
    assertEquals(SnapshotAndImputation.Imputation.AVG_ADJACENT, result.sampleFlaw(tp1).get(0).imputation());
  }

  @Test
  public void testFallbackToAvgAvailable() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    KafkaMetricSampleAggregator metricSampleAggregator = new KafkaMetricSampleAggregator(config, metadata);

    // Only give two sample to the aggregator.
    populateSampleAggregator(2, 1, metricSampleAggregator);
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        metricSampleAggregator, TP, 2, SNAPSHOT_WINDOW_MS);
    try {
      MetricSampleAggregationResult result =
          metricSampleAggregator.recentSnapshots(clusterAndGeneration(metadata.fetch()), NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS);
      assertTrue(result.snapshots().isEmpty());
    } catch (NotEnoughSnapshotsException nese) {
      nese.printStackTrace();
      fail("Should not throw NotEnoughSnapshotsException");
    }
    populateSampleAggregator(2, MIN_SAMPLES_PER_LOAD_SNAPSHOT - 2, metricSampleAggregator);

    try {
      MetricSampleAggregationResult result =
          metricSampleAggregator.recentSnapshots(clusterAndGeneration(metadata.fetch()), NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS);
      int numSnapshots = result.snapshots().get(TP).length;
      assertEquals(NUM_SNAPSHOTS, numSnapshots);
      int numFlaws = 0;
      for (MetricSampleAggregationResult.SampleFlaw sampleFlaw : result.sampleFlaws().get(TP)) {
        assertEquals(SnapshotAndImputation.Imputation.AVG_AVAILABLE, sampleFlaw.imputation());
        numFlaws++;
      }
      assertEquals(2, numFlaws);

    } catch (NotEnoughSnapshotsException nese) {
      nese.printStackTrace();
      fail("Should not throw NotEnoughSnapshotsException");
    }
  }

  @Test
  public void testFallbackToAvgAdjacent() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    TopicPartition anotherTopicPartition = new TopicPartition("AnotherTopic", 1);
    Metadata metadata = getMetadata(Arrays.asList(TP, anotherTopicPartition));
    KafkaMetricSampleAggregator metricSampleAggregator = new KafkaMetricSampleAggregator(config, metadata);

    // Only give one sample to the aggregator for previous period.
    populateSampleAggregator(NUM_SNAPSHOTS, MIN_SAMPLES_PER_LOAD_SNAPSHOT - 1, metricSampleAggregator);
    // Create let (NUM_SNAPSHOT + 1) have enough samples.
    CruiseControlUnitTestUtils.populateSampleAggregator(1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, metricSampleAggregator,
                                                        TP, NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS);
    // Let a snapshot window exist but not containing samples for partition 0
    CruiseControlUnitTestUtils.populateSampleAggregator(1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, metricSampleAggregator,
                                                        anotherTopicPartition, NUM_SNAPSHOTS + 1, SNAPSHOT_WINDOW_MS);
    // Let the rest of the snapshot has enough samples.
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 1,
                                                        MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        metricSampleAggregator, TP,
                                                        NUM_SNAPSHOTS + 2,
                                                        SNAPSHOT_WINDOW_MS);

    try {
      MetricSampleAggregationResult result =
          metricSampleAggregator.recentSnapshots(clusterAndGeneration(metadata.fetch()), NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS * 2);
      int numSnapshots = result.snapshots().get(TP).length;
      assertEquals(NUM_SNAPSHOTS, numSnapshots);
      int numFlaws = 0;
      for (MetricSampleAggregationResult.SampleFlaw sampleFlaw : result.sampleFlaw(TP)) {
        assertEquals(SnapshotAndImputation.Imputation.AVG_ADJACENT, sampleFlaw.imputation());
        numFlaws++;
      }
      assertEquals(1, numFlaws);

    } catch (NotEnoughSnapshotsException nese) {
      nese.printStackTrace();
      fail("Should not throw NotEnoughSnapshotsException");
    }
  }

  @Test
  public void testTooManyFlaws() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    KafkaMetricSampleAggregator metricSampleAggregator = new KafkaMetricSampleAggregator(config, metadata);

    // Only give two samples to the aggregator.
    populateSampleAggregator(3, MIN_SAMPLES_PER_LOAD_SNAPSHOT - 1, metricSampleAggregator);
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 2, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        metricSampleAggregator, TP, 3, SNAPSHOT_WINDOW_MS);


    try {
      MetricSampleAggregationResult result =
          metricSampleAggregator.recentSnapshots(clusterAndGeneration(metadata.fetch()), NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS);
      assertTrue(result.snapshots().isEmpty());
      assertEquals(3, result.sampleFlaw(TP).size());
    } catch (NotEnoughSnapshotsException nese) {
      nese.printStackTrace();
      fail("Should not throw NotEnoughSnapshotsException");
    }
  }

  @Test
  public void testNotEnoughSnapshots() throws NotEnoughValidSnapshotsException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    KafkaMetricSampleAggregator metricSampleAggregator = new KafkaMetricSampleAggregator(config, metadata);

    populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, metricSampleAggregator);

    try {
      // Only 4 snapshots has smaller timestamp than the timestamp we passed in.
      ModelCompletenessRequirements requirements = new ModelCompletenessRequirements(NUM_SNAPSHOTS, 0.0, false);
      metricSampleAggregator.snapshots(clusterAndGeneration(metadata.fetch()), -1L, NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS - 1, requirements);
      fail("Should throw NotEnoughSnapshotsException");
    } catch (NotEnoughSnapshotsException nse) {
      // let it go
    }
  }

  @Test
  public void testExcludeInvalidMetricSample() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    KafkaMetricSampleAggregator metricSampleAggregator = new KafkaMetricSampleAggregator(config, metadata);

    populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, metricSampleAggregator);
    // Set the leader to be node 1, which is different from the leader in the metadata.
    PartitionMetricSample sampleWithDifferentLeader = new PartitionMetricSample(1, TP);
    sampleWithDifferentLeader.record(Resource.DISK, 10000);
    sampleWithDifferentLeader.record(Resource.CPU, 10000);
    sampleWithDifferentLeader.record(Resource.NW_IN, 10000);
    sampleWithDifferentLeader.record(Resource.NW_OUT, 10000);
    sampleWithDifferentLeader.close(0);

    // Only populate the CPU metric
    PartitionMetricSample incompletePartitionMetricSample = new PartitionMetricSample(0, TP);
    incompletePartitionMetricSample.record(Resource.CPU, 10000);
    incompletePartitionMetricSample.close(0);

    metricSampleAggregator.addSample(sampleWithDifferentLeader);
    metricSampleAggregator.addSample(incompletePartitionMetricSample);

    // Check the snapshot value and make sure the metric samples above are excluded.
    try {
      Map<TopicPartition, Snapshot[]> snapshotsForPartition =
          metricSampleAggregator.recentSnapshots(clusterAndGeneration(metadata.fetch()), NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS).snapshots();
      Snapshot[] snapshots = snapshotsForPartition.get(TP);
      for (Resource resource : Resource.values()) {
        double expectedValue = resource == Resource.DISK ?
            MIN_SAMPLES_PER_LOAD_SNAPSHOT - 1 : (MIN_SAMPLES_PER_LOAD_SNAPSHOT - 1) / 2.0;
        assertEquals("The utilization for " + resource + " should be " + expectedValue,
            expectedValue, snapshots[NUM_SNAPSHOTS - 1].utilizationFor(resource), 0);
      }
    } catch (NotEnoughSnapshotsException e) {
      fail("Should not have enough snapshots.");
    }
  }
  
  @Test
  public void testValidSnapshotWindows() {
    TestContext ctx = setupScenario1();
    KafkaMetricSampleAggregator aggregator = ctx.aggregator();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = ctx.clusterAndGeneration(0);
    SortedSet<Long> validSnapshotWindows = aggregator.validSnapshotWindows(clusterAndGeneration, 1.0, 4);
    assertEquals(NUM_SNAPSHOTS, validSnapshotWindows.size());
    assertValidWindows(validSnapshotWindows, NUM_SNAPSHOTS, Collections.emptySet());
  }
  
  @Test
  public void testValidSnapshotWindowsWithInvalidPartitions() {
    TestContext ctx = setupScenario2();
    KafkaMetricSampleAggregator aggregator = ctx.aggregator();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = ctx.clusterAndGeneration(0);
    
    SortedSet<Long> validSnapshotWindows = aggregator.validSnapshotWindows(clusterAndGeneration, 1.0, 4);
    assertEquals("Should have three invalid windows.", NUM_SNAPSHOTS - 3, validSnapshotWindows.size());
    assertValidWindows(validSnapshotWindows, NUM_SNAPSHOTS - 1, Arrays.asList(6, 7));
    // reduced monitored percentage should include every window.
    assertEquals(NUM_SNAPSHOTS, aggregator.validSnapshotWindows(clusterAndGeneration, 0.5, 4).size());
  }
  
  @Test
  public void testValidSnapshotWindowWithDifferentInvalidPartitions() {
    TestContext ctx = setupScenario3();
    KafkaMetricSampleAggregator aggregator = ctx.aggregator();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = ctx.clusterAndGeneration(0);
    SortedSet<Long> validSnapshotWindows = aggregator.validSnapshotWindows(clusterAndGeneration, 0.5, 6);
    assertEquals("Should have two invalid windows.", NUM_SNAPSHOTS - 2, validSnapshotWindows.size());
    assertValidWindows(validSnapshotWindows, NUM_SNAPSHOTS, Arrays.asList(6, 7));
  }
  
  @Test
  public void testValidSnapshotWindowsWithTooManyImputations() {
    TestContext ctx = setupScenario4();
    KafkaMetricSampleAggregator aggregator = ctx.aggregator();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = ctx.clusterAndGeneration(0);

    SortedSet<Long> validSnapshotWindows = aggregator.validSnapshotWindows(clusterAndGeneration, 0.5, 6);
    assertEquals("Should have two invalid windows.", NUM_SNAPSHOTS - 2, validSnapshotWindows.size());
    assertValidWindows(validSnapshotWindows, NUM_SNAPSHOTS, Arrays.asList(6, 7));
  }
  
  @Test
  public void testMonitoredPercentage() {
    TestContext ctx = setupScenario1();
    KafkaMetricSampleAggregator aggregator = ctx.aggregator();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = ctx.clusterAndGeneration(0);
    assertEquals(1.0, aggregator.monitoredPercentage(clusterAndGeneration, 4), 0.01);

    ctx = setupScenario2();
    aggregator = ctx.aggregator();
    clusterAndGeneration = ctx.clusterAndGeneration(0);
    assertEquals(0.5, aggregator.monitoredPercentage(clusterAndGeneration, 4), 0.01);

    ctx = setupScenario3();
    aggregator = ctx.aggregator();
    clusterAndGeneration = ctx.clusterAndGeneration(0);
    assertEquals((double) 2 / 6, aggregator.monitoredPercentage(clusterAndGeneration, 6), 0.01);

    ctx = setupScenario4();
    aggregator = ctx.aggregator();
    clusterAndGeneration = ctx.clusterAndGeneration(0);
    assertEquals((double) 2 / 6, aggregator.monitoredPercentage(clusterAndGeneration, 6), 0.01);
  }

  @Test
  public void testMonitoredPercentagesByWindows() {
    TestContext ctx = setupScenario1();
    KafkaMetricSampleAggregator aggregator = ctx.aggregator();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = ctx.clusterAndGeneration(0);
    
    Map<Long, Double> percentages = aggregator.monitoredPercentagesByWindows(clusterAndGeneration, 4);
    assertEquals(NUM_SNAPSHOTS, percentages.size());
    for (Map.Entry<Long, Double> entry : percentages.entrySet()) {
      assertEquals(1.0, entry.getValue(), 0.01);
    }
    
    ctx = setupScenario2();
    aggregator = ctx.aggregator();
    clusterAndGeneration = ctx.clusterAndGeneration(0);
    percentages = aggregator.monitoredPercentagesByWindows(clusterAndGeneration, 4);
    assertEquals(NUM_SNAPSHOTS, percentages.size());
    for (Map.Entry<Long, Double> entry : percentages.entrySet()) {
      long window = entry.getKey();
      if (window == 6000 || window == 7000 || window == 20000) {
        assertEquals(0.5, entry.getValue(), 0.01);
      } else {
        assertEquals(1.0, entry.getValue(), 0.01);
      }
    }

    ctx = setupScenario3();
    aggregator = ctx.aggregator();
    clusterAndGeneration = ctx.clusterAndGeneration(0);
    percentages = aggregator.monitoredPercentagesByWindows(clusterAndGeneration, 6);
    assertEquals(NUM_SNAPSHOTS, percentages.size());
    for (Map.Entry<Long, Double> entry : percentages.entrySet()) {
      long window = entry.getKey();
      if (window == 6000 || window == 7000 || window == 18000 || window == 19000) {
        assertEquals((double) 4 / 6, entry.getValue(), 0.01);
      } else {
        assertEquals(1.0, entry.getValue(), 0.01);
      }
    }

    ctx = setupScenario4();
    aggregator = ctx.aggregator();
    clusterAndGeneration = ctx.clusterAndGeneration(0);
    percentages = aggregator.monitoredPercentagesByWindows(clusterAndGeneration, 6);
    assertEquals(NUM_SNAPSHOTS, percentages.size());
    for (Map.Entry<Long, Double> entry : percentages.entrySet()) {
      long window = entry.getKey();
      if (window == 6000 || window == 7000) {
        System.out.println(window);
        assertEquals((double) 2 / 6, entry.getValue(), 0.01);
      } else {
        assertEquals((double) 4 / 6, entry.getValue(), 0.01);
      }
    }
  }
  
  /**
   * Two topics with 2 partitions each. No data missing.
   */
  private TestContext setupScenario1() {
    TopicPartition t0p1 = new TopicPartition(TOPIC, 1);
    TopicPartition t1p0 = new TopicPartition("TOPIC1", 0);
    TopicPartition t1p1 = new TopicPartition("TOPIC1", 1);
    List<TopicPartition> allPartitions = Arrays.asList(TP, t0p1, t1p0, t1p1);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(allPartitions);
    KafkaMetricSampleAggregator aggregator = new KafkaMetricSampleAggregator(config, metadata);

    for (TopicPartition tp : allPartitions) {
      populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator, tp);
    }
    return new TestContext(metadata, aggregator);
  }

  /**
   * Two topics with 2 partitions each.
   * T1P1 misses window 6000 (index=5), 7000 (index=6) and 20000 (index=19)
   * Other partitions has full data.
   */
  private TestContext setupScenario2() {
    TopicPartition t0p1 = new TopicPartition(TOPIC, 1);
    TopicPartition t1p0 = new TopicPartition("TOPIC1", 0);
    TopicPartition t1p1 = new TopicPartition("TOPIC1", 1);
    List<TopicPartition> allPartitions = Arrays.asList(TP, t0p1, t1p0, t1p1);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(allPartitions);
    KafkaMetricSampleAggregator aggregator = new KafkaMetricSampleAggregator(config, metadata);

    for (TopicPartition tp : Arrays.asList(TP, t0p1, t1p0)) {
      populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator, tp);
    }
    // Let t1p1 miss two consecutive windows and the most recent window.
    populateSampleAggregator(5, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator, t1p1);
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 8,
                                                        MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator,
                                                        t1p1,
                                                        7,
                                                        SNAPSHOT_WINDOW_MS);
    return new TestContext(metadata, aggregator);
  }

  /**
   * Three topics with 2 partitions each.
   * T0P1 missing window 18000 (index=17), 19000 (index=18)
   * T1P1 missing window 6000 (index=5), 7000 (index=6)
   * Other partitions have all data.
   */
  private TestContext setupScenario3() {
    TopicPartition t0p1 = new TopicPartition(TOPIC, 1);
    TopicPartition t1p0 = new TopicPartition("TOPIC1", 0);
    TopicPartition t1p1 = new TopicPartition("TOPIC1", 1);
    TopicPartition t2p0 = new TopicPartition("TOPIC2", 0);
    TopicPartition t2p1 = new TopicPartition("TOPIC2", 1);
    List<TopicPartition> allPartitions = Arrays.asList(TP, t0p1, t1p0, t1p1, t2p0, t2p1);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(allPartitions);
    KafkaMetricSampleAggregator aggregator = new KafkaMetricSampleAggregator(config, metadata);

    for (TopicPartition tp : Arrays.asList(TP, t1p0, t2p0, t2p1)) {
      populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator, tp);
    }
    // Let t0p1 miss the second and the third latest window.
    populateSampleAggregator(NUM_SNAPSHOTS - 3, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator, t0p1);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator, t0p1,
                                                        NUM_SNAPSHOTS - 1, SNAPSHOT_WINDOW_MS);
    // let t1p1 miss another earlier window
    populateSampleAggregator(5, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator, t1p1);
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 6, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, t1p1, 7, SNAPSHOT_WINDOW_MS);
    return new TestContext(metadata, aggregator);
  }

  /**
   * 3 Topics with 2 partitions each.
   * T0P1 has all the windows with AVG_AVAILABLE as imputation.
   * T1P1 misses window 6000 (index=5), 7000 (index=6)
   * All other partitions have full data.
   */
  private TestContext setupScenario4() {
    TopicPartition t0p1 = new TopicPartition(TOPIC, 1);
    TopicPartition t1p0 = new TopicPartition("TOPIC1", 0);
    TopicPartition t1p1 = new TopicPartition("TOPIC1", 1);
    TopicPartition t2p0 = new TopicPartition("TOPIC2", 0);
    TopicPartition t2p1 = new TopicPartition("TOPIC2", 1);
    List<TopicPartition> allPartitions = Arrays.asList(TP, t0p1, t1p0, t1p1, t2p0, t2p1);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(allPartitions);
    KafkaMetricSampleAggregator aggregator = new KafkaMetricSampleAggregator(config, metadata);

    for (TopicPartition tp : Arrays.asList(TP, t1p0, t2p0, t2p1)) {
      populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator, tp);
    }
    // Let t0p1 have too many imputations.
    populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT - 1, aggregator, t0p1);
    // let t1p1 miss another earlier window
    populateSampleAggregator(5, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator, t1p1);
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 6, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, t1p1, 7, SNAPSHOT_WINDOW_MS);
    return new TestContext(metadata, aggregator);
  }
  
  private void assertValidWindows(SortedSet<Long> actualValidWindows, int firstValidWindowIndex, Collection<Integer> invalidWindowIndexes) {
    int windowIndex = firstValidWindowIndex;
    for (long window : actualValidWindows) {
      while (invalidWindowIndexes.contains(windowIndex)) {
        windowIndex--;
      }
      assertEquals(windowIndex * SNAPSHOT_WINDOW_MS, window);
      windowIndex--;
    }
  }

  private MetadataClient.ClusterAndGeneration clusterAndGeneration(Cluster cluster) {
    return new MetadataClient.ClusterAndGeneration(cluster, 0);
  }

  private void populateSampleAggregator(int numSnapshots,
                                        int numSamplesPerSnapshot,
                                        KafkaMetricSampleAggregator metricSampleAggregator) {
    populateSampleAggregator(numSnapshots, numSamplesPerSnapshot, metricSampleAggregator, TP);
  }

  private void populateSampleAggregator(int numSnapshots,
                                        int numSamplesPerSnapshot,
                                        KafkaMetricSampleAggregator metricSampleAggregator,
                                        TopicPartition tp) {
    CruiseControlUnitTestUtils.populateSampleAggregator(numSnapshots, numSamplesPerSnapshot, metricSampleAggregator,
                                                        tp, 0, SNAPSHOT_WINDOW_MS);
  }

  private Properties getLoadMonitorProperties() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.LOAD_SNAPSHOT_WINDOW_MS_CONFIG, Long.toString(SNAPSHOT_WINDOW_MS));
    props.setProperty(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG, Integer.toString(NUM_SNAPSHOTS));
    props.setProperty(KafkaCruiseControlConfig.MIN_SAMPLES_PER_LOAD_SNAPSHOT_CONFIG, Integer.toString(MIN_SAMPLES_PER_LOAD_SNAPSHOT));
    return props;
  }

  private Metadata getMetadata(Collection<TopicPartition> partitions) {
    Metadata metadata = new Metadata();
    metadata.update(getCluster(partitions), 0);
    return metadata;
  }

  private Cluster getCluster(Collection<TopicPartition> partitions) {
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
    return new Cluster("cluster_id", allNodes, parts, Collections.emptySet(), Collections.emptySet());
  }

  private static class TestContext {
    private final Metadata _metadata;
    private final KafkaMetricSampleAggregator _aggregator;
    
    TestContext(Metadata metadata, KafkaMetricSampleAggregator aggregator) {
      _metadata = metadata;
      _aggregator = aggregator;
    }
    
    private MetadataClient.ClusterAndGeneration clusterAndGeneration(int generation) {
      return new MetadataClient.ClusterAndGeneration(_metadata.fetch(), generation);
    }
    
    private KafkaMetricSampleAggregator aggregator() {
      return _aggregator;
    }
  }
}
