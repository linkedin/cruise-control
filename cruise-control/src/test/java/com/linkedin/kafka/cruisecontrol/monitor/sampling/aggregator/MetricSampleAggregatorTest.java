/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.NotEnoughSnapshotsException;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.PartitionMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.Snapshot;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test for {@link MetricSampleAggregator}.
 */
public class MetricSampleAggregatorTest {
  private static final String TOPIC = "topic";
  private static final int PARTITION = 0;
  private static final int NUM_SNAPSHOTS = 20;
  private static final long SNAPSHOT_WINDOW_MS = 1000L;
  private static final int MIN_SAMPLES_PER_LOAD_SNAPSHOT = 4;
  private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);

  @Test
  public void testAddSampleInDifferentWindows() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    int numSnapshot = config.getInt(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG);
    MetricCompletenessChecker checker = new MetricCompletenessChecker(numSnapshot);
    MetricSampleAggregator metricSampleAggregator = new MetricSampleAggregator(config, metadata, checker);

    populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, metricSampleAggregator);

    try {
      Map<TopicPartition, Snapshot[]> snapshotsForPartition =
          metricSampleAggregator.recentSnapshots(metadata.fetch(), Long.MAX_VALUE).snapshots();
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
    ModelGeneration modelGeneration = new ModelGeneration(1, metricSampleAggregator.currentGeneration());
    assertEquals(NUM_SNAPSHOTS, checker.numValidWindows(modelGeneration, metadata.fetch(), 1.0, 1));
    Map<Long, Double> monitoredPercentages = checker.monitoredPercentages(modelGeneration, metadata.fetch(), 1);
    for (double percentage : monitoredPercentages.values()) {
      assertEquals(1.0, percentage, 0.0);
    }
    assertEquals(NUM_SNAPSHOTS, checker.numWindows(-1, Long.MAX_VALUE));
  }

  @Test
  public void testEvictOldSnapshots() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    int numSnapshot = config.getInt(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG);
    MetricCompletenessChecker checker = new MetricCompletenessChecker(numSnapshot);
    MetricSampleAggregator metricSampleAggregator = new MetricSampleAggregator(config, metadata, checker);

    // The eviction should still work after throwing exceptions.
    try {
      metricSampleAggregator.recentSnapshots(metadata.fetch(), Long.MAX_VALUE);
      metricSampleAggregator.snapshots(metadata.fetch(), -1, Long.MAX_VALUE, -1, true);
    } catch (IllegalArgumentException | NotEnoughSnapshotsException e) {
      // let it go.
    }
    populateSampleAggregator(2 * NUM_SNAPSHOTS + 2, MIN_SAMPLES_PER_LOAD_SNAPSHOT, metricSampleAggregator);

    try {
      Map<TopicPartition, Snapshot[]> snapshotsForPartition =
          metricSampleAggregator.recentSnapshots(metadata.fetch(), Long.MAX_VALUE).snapshots();
      assertEquals("Only " + NUM_SNAPSHOTS + " should be returned.",
          NUM_SNAPSHOTS, snapshotsForPartition.get(TP).length);
      for (int i = 0; i < NUM_SNAPSHOTS; i++) {
        assertEquals((2 * NUM_SNAPSHOTS + 1 - i) * SNAPSHOT_WINDOW_MS, snapshotsForPartition.get(TP)[i].time());
      }
      assertEquals(2 * NUM_SNAPSHOTS + 1, metricSampleAggregator.numSnapshotWindows());
    } catch (NotEnoughSnapshotsException e) {
      fail("There should be enough snapshots.");
    }
  }

  @Test
  public void testFallbackToAvgAvailable() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    int numSnapshot = config.getInt(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG);
    MetricCompletenessChecker checker = new MetricCompletenessChecker(numSnapshot);
    MetricSampleAggregator metricSampleAggregator = new MetricSampleAggregator(config, metadata, checker);

    // Only give two sample to the aggregator.
    populateSampleAggregator(2, 1, metricSampleAggregator);
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        metricSampleAggregator, TP, 2, SNAPSHOT_WINDOW_MS);
    try {
      MetricSampleAggregationResult result =
          metricSampleAggregator.recentSnapshots(metadata.fetch(), NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS);
      assertTrue(result.snapshots().isEmpty());
    } catch (NotEnoughSnapshotsException nese) {
      nese.printStackTrace();
      fail("Should not throw NotEnoughSnapshotsException");
    }
    populateSampleAggregator(2, MIN_SAMPLES_PER_LOAD_SNAPSHOT - 2, metricSampleAggregator);

    try {
      MetricSampleAggregationResult result =
          metricSampleAggregator.recentSnapshots(metadata.fetch(), NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS);
      int numSnapshots = result.snapshots().get(TP).length;
      assertEquals(NUM_SNAPSHOTS, numSnapshots);
      int numFlaws = 0;
      for (MetricSampleAggregationResult.SampleFlaw sampleFlaw : result.sampleFlaws().get(TP)) {
        assertEquals(MetricSampleAggregationResult.Imputation.AVG_AVAILABLE, sampleFlaw.imputation());
        numFlaws++;
      }
      assertEquals(2, numFlaws);

    } catch (NotEnoughSnapshotsException nese) {
      nese.printStackTrace();
      fail("Should not throw NotEnoughSnapshotsException");
    }
  }

  @Test
  public void testFallbackToPrevPeriod() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    TopicPartition anotherTopicPartition = new TopicPartition("AnotherTopic", 1);
    Metadata metadata = getMetadata(Arrays.asList(TP, anotherTopicPartition));
    int numSnapshot = config.getInt(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG);
    MetricCompletenessChecker checker = new MetricCompletenessChecker(numSnapshot);
    MetricSampleAggregator metricSampleAggregator = new MetricSampleAggregator(config, metadata, checker);

    // Only give one sample to the aggregator.
    populateSampleAggregator(NUM_SNAPSHOTS, MIN_SAMPLES_PER_LOAD_SNAPSHOT, metricSampleAggregator);
    // Let a snapshot window exist but not containing samples for partition 0
    CruiseControlUnitTestUtils.populateSampleAggregator(2, MIN_SAMPLES_PER_LOAD_SNAPSHOT, metricSampleAggregator,
                                                        anotherTopicPartition, NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 1,
                             MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                             metricSampleAggregator, TP,
                             NUM_SNAPSHOTS + 2,
                             SNAPSHOT_WINDOW_MS);

    try {
      MetricSampleAggregationResult result =
          metricSampleAggregator.recentSnapshots(metadata.fetch(), NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS * 2);
      int numSnapshots = result.snapshots().get(TP).length;
      assertEquals(NUM_SNAPSHOTS, numSnapshots);
      int numFlaws = 0;
      for (MetricSampleAggregationResult.SampleFlaw sampleFlaw : result.sampleFlaw(TP)) {
        assertEquals(MetricSampleAggregationResult.Imputation.PREV_PERIOD, sampleFlaw.imputation());
        // Check the sample flaw window.
        assertEquals((NUM_SNAPSHOTS + 2 - numFlaws) * SNAPSHOT_WINDOW_MS, sampleFlaw.snapshotWindow());
        // Check the snapshot window.
        assertEquals(sampleFlaw.snapshotWindow(), result.snapshots().get(TP)[NUM_SNAPSHOTS - 2 + numFlaws].time());
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
    int numSnapshot = config.getInt(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG);
    MetricCompletenessChecker checker = new MetricCompletenessChecker(numSnapshot);
    MetricSampleAggregator metricSampleAggregator = new MetricSampleAggregator(config, metadata, checker);

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
          metricSampleAggregator.recentSnapshots(metadata.fetch(), NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS * 2);
      int numSnapshots = result.snapshots().get(TP).length;
      assertEquals(NUM_SNAPSHOTS, numSnapshots);
      int numFlaws = 0;
      for (MetricSampleAggregationResult.SampleFlaw sampleFlaw : result.sampleFlaw(TP)) {
        assertEquals(MetricSampleAggregationResult.Imputation.AVG_ADJACENT, sampleFlaw.imputation());
        numFlaws++;
      }
      assertEquals(1, numFlaws);

    } catch (NotEnoughSnapshotsException nese) {
      nese.printStackTrace();
      fail("Should not throw NotEnoughSnapshotsException");
    }
  }

  // We will add the feature later.
  @Test
  @Ignore
  public void testTooManyFlaws() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    int numSnapshot = config.getInt(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG);
    MetricCompletenessChecker checker = new MetricCompletenessChecker(numSnapshot);
    MetricSampleAggregator metricSampleAggregator = new MetricSampleAggregator(config, metadata, checker);

    // Only give two samples to the aggregator.
    populateSampleAggregator(3, MIN_SAMPLES_PER_LOAD_SNAPSHOT - 1, metricSampleAggregator);
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 2, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        metricSampleAggregator, TP, 3, SNAPSHOT_WINDOW_MS);


    try {
      MetricSampleAggregationResult result =
          metricSampleAggregator.recentSnapshots(metadata.fetch(), NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS);
      assertTrue(result.snapshots().isEmpty());
      assertEquals(3, result.sampleFlaw(TP).size());
    } catch (NotEnoughSnapshotsException nese) {
      nese.printStackTrace();
      fail("Should not throw NotEnoughSnapshotsException");
    }
  }

  @Test
  public void testNotEnoughSnapshots() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    int numSnapshot = config.getInt(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG);
    MetricCompletenessChecker checker = new MetricCompletenessChecker(numSnapshot);
    MetricSampleAggregator metricSampleAggregator = new MetricSampleAggregator(config, metadata, checker);

    populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, metricSampleAggregator);

    try {
      // Only 4 snapshots has smaller timestamp than the timestamp we passed in.
      metricSampleAggregator.recentSnapshots(metadata.fetch(), NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS - 1);
      fail("Should throw NotEnoughSnapshotsException");
    } catch (NotEnoughSnapshotsException nse) {
      // let it go
    }
  }

  @Test
  public void testExcludeInvalidMetricSample() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    int numSnapshot = config.getInt(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG);
    MetricCompletenessChecker checker = new MetricCompletenessChecker(numSnapshot);
    MetricSampleAggregator metricSampleAggregator = new MetricSampleAggregator(config, metadata, checker);

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
          metricSampleAggregator.recentSnapshots(metadata.fetch(), NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS).snapshots();
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
  public void testCachedAggregationResult() throws NotEnoughSnapshotsException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    int numSnapshot = config.getInt(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG);
    MetricCompletenessChecker checker = new MetricCompletenessChecker(numSnapshot);
    MetricSampleAggregator metricSampleAggregator = new MetricSampleAggregator(config, metadata, checker);

    populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, metricSampleAggregator);
    long generation = metricSampleAggregator.currentGeneration();
    assertEquals(NUM_SNAPSHOTS + 1, generation);
    MetricSampleAggregationResult result0 = metricSampleAggregator.recentSnapshots(metadata.fetch(),
                                                                                  NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS);
    assertTrue("result0 should have been cached.", result0 == metricSampleAggregator.cachedAggregationResult());
    assertEquals("The cached aggregated window should be 20000", NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS,
                 metricSampleAggregator.cachedAggregationResultWindow());
    assertEquals("The generation should be " + generation, generation, metricSampleAggregator.currentGeneration());
    MetricSampleAggregationResult result1 = metricSampleAggregator.recentSnapshots(metadata.fetch(),
                                                                                   NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS);
    assertTrue("result1 should be returned from cached result.", result0 == result1);
    assertEquals("The generation should be " + generation, generation, metricSampleAggregator.currentGeneration());

    // Adding a metric sample to the active snapshot window, it should have no impact.
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, metricSampleAggregator, TP, NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS);
    MetricSampleAggregationResult result2 = metricSampleAggregator.recentSnapshots(metadata.fetch(),
                                                                                   NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS);
    assertTrue("Adding sample to active snapshot window has no impact on cache. result2 should have been cached.",
               result2 == result0);
    assertEquals("The generation should be " + generation, generation, metricSampleAggregator.currentGeneration());

    // Adding a metric sample to a previous snapshot window, this should invalidate the cache.
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, metricSampleAggregator, TP, 0, SNAPSHOT_WINDOW_MS);
    assertNull("The cache should have been invalidated.", metricSampleAggregator.cachedAggregationResult());
    assertEquals("The cache should have been invalidated.", -1L, metricSampleAggregator.cachedAggregationResultWindow());
    assertEquals("The generation should be " + (generation + 1), generation + 1, metricSampleAggregator.currentGeneration());
    MetricSampleAggregationResult result3 = metricSampleAggregator.recentSnapshots(metadata.fetch(),
                                                                                   NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS);
    assertTrue("result3 should have been cached.", result3 == metricSampleAggregator.cachedAggregationResult());
    assertEquals("The cached aggregated window should be 20000", NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS,
                 metricSampleAggregator.cachedAggregationResultWindow());

    // Roll out a new snapshot window, the cache should be invalidated.
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, metricSampleAggregator, TP, NUM_SNAPSHOTS + 1, SNAPSHOT_WINDOW_MS);
    assertNull("The cache should have been cleared.", metricSampleAggregator.cachedAggregationResult());
    assertEquals("The cached aggregated window should be -1", -1,
                 metricSampleAggregator.cachedAggregationResultWindow());
    MetricSampleAggregationResult result4 = metricSampleAggregator.recentSnapshots(metadata.fetch(),
                                                                                   NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS);
    assertNull("result4 should not be cached because it did not query latest window.", metricSampleAggregator.cachedAggregationResult());
    assertEquals("The cached aggregated window should be -1", -1,
                 metricSampleAggregator.cachedAggregationResultWindow());
    assertEquals("The generation should be " + (generation + 2), generation + 2, metricSampleAggregator.currentGeneration());

    // Query a new snapshot window
    MetricSampleAggregationResult result5 = metricSampleAggregator.recentSnapshots(metadata.fetch(),
                                                                                   (NUM_SNAPSHOTS + 1) * SNAPSHOT_WINDOW_MS);
    assertTrue("The cache should have been updated.", result5 != result4);
    assertTrue("The cache should have been updated to result5.", result5 == metricSampleAggregator.cachedAggregationResult());
    assertEquals("The cached aggregated window should be 21000", (NUM_SNAPSHOTS + 1) * SNAPSHOT_WINDOW_MS,
                 metricSampleAggregator.cachedAggregationResultWindow());
    assertEquals("The generation should be " + (generation + 2), generation + 2, metricSampleAggregator.currentGeneration());

    // Query a past snapshot window, it should have no impact on the cache.
    metricSampleAggregator.recentSnapshots(metadata.fetch(), NUM_SNAPSHOTS * SNAPSHOT_WINDOW_MS);
    MetricSampleAggregationResult result6 = metricSampleAggregator.recentSnapshots(metadata.fetch(),
                                                                                   (NUM_SNAPSHOTS + 1) * SNAPSHOT_WINDOW_MS);
    assertTrue("result5 should remain cached.", result5 == metricSampleAggregator.cachedAggregationResult());
    assertEquals("The cached aggregated window should be 21000", (NUM_SNAPSHOTS + 1) * SNAPSHOT_WINDOW_MS,
                 metricSampleAggregator.cachedAggregationResultWindow());
    assertTrue("Result6 should be returned from the cache.", result6 == metricSampleAggregator.cachedAggregationResult());
    assertEquals("The generation should be " + (generation + 2), generation + 2, metricSampleAggregator.currentGeneration());
  }

  @Test
  public void testConcurrency() throws NotEnoughSnapshotsException {
    final int numThreads = 10;
    final int samplesPerSnapshot = 100;
    Properties props = getLoadMonitorProperties();
    // We set the minimum number of samples per snapshot to be the total number of samples to insert.
    // So when there is a sample got lost we will fail to collect enough snapshot.
    props.setProperty(KafkaCruiseControlConfig.MIN_SAMPLES_PER_LOAD_SNAPSHOT_CONFIG,
                     Integer.toString(samplesPerSnapshot * numThreads));
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

    Set<TopicPartition> partSet = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      partSet.add(new TopicPartition(TOPIC, i));
    }
    Metadata metadata = getMetadata(partSet);
    int numSnapshot = config.getInt(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG);
    MetricCompletenessChecker checker = new MetricCompletenessChecker(numSnapshot);
    final MetricSampleAggregator metricSampleAggregator = new MetricSampleAggregator(config, metadata, checker);

    final Random random = new Random();
    List<Thread> threads = new ArrayList<>();
    Set<TopicPartition> allPartitions = Collections.synchronizedSet(new HashSet<>());
    for (int i = 0; i < numThreads; i++) {
      Thread t = new Thread() {
        @Override
        public void run() {
          // Add samples for 10 random partitions.
          for (int i = 0; i < 10; i++) {
            TopicPartition tp = new TopicPartition(TOPIC, random.nextInt(5));
            allPartitions.add(tp);
            populateSampleAggregator(NUM_SNAPSHOTS + 1, samplesPerSnapshot, metricSampleAggregator, tp);
          }
        }
      };
      threads.add(t);
    }
    threads.forEach(Thread::start);
    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        // let it go.
      }
    }
    assertEquals((NUM_SNAPSHOTS + 1) * samplesPerSnapshot * 10 * numThreads,
                 metricSampleAggregator.totalNumSamples());

  }

  private void populateSampleAggregator(int numSnapshots,
                                        int numSamplesPerSnapshot,
                                        MetricSampleAggregator metricSampleAggregator) {
    populateSampleAggregator(numSnapshots, numSamplesPerSnapshot, metricSampleAggregator, TP);
  }

  private void populateSampleAggregator(int numSnapshots,
                                        int numSamplesPerSnapshot,
                                        MetricSampleAggregator metricSampleAggregator,
                                        TopicPartition tp) {
    CruiseControlUnitTestUtils.populateSampleAggregator(numSnapshots, numSamplesPerSnapshot, metricSampleAggregator,
                                                        tp, 0, SNAPSHOT_WINDOW_MS);
  }

  private Properties getLoadMonitorProperties() {
    Properties props = CruiseControlUnitTestUtils.getCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.LOAD_SNAPSHOT_WINDOW_MS_CONFIG, Long.toString(SNAPSHOT_WINDOW_MS));
    props.setProperty(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG, Integer.toString(NUM_SNAPSHOTS));
    props.setProperty(KafkaCruiseControlConfig.MIN_SAMPLES_PER_LOAD_SNAPSHOT_CONFIG, Integer.toString(MIN_SAMPLES_PER_LOAD_SNAPSHOT));
    return props;
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
    Cluster cluster = new Cluster(allNodes, parts, Collections.emptySet());
    Metadata metadata = new Metadata();
    metadata.update(cluster, 0);
    return metadata;
  }


}
