/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.cruisecontrol.exception.NotEnoughSnapshotsException;
import com.linkedin.cruisecontrol.monitor.sampling.Snapshot;
import com.linkedin.cruisecontrol.resource.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import java.util.Random;
import org.junit.Test;

import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation.Imputation.AVG_ADJACENT;
import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation.Imputation.AVG_AVAILABLE;
import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation.Imputation.FORCED_INSUFFICIENT;
import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation.Imputation.NO_VALID_IMPUTATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class MetricSampleAggregatorTest {
  private static final int NUM_SNAPSHOTS = 20;
  private static final long SNAPSHOT_WINDOW_MS = 1000L;
  private static final int MIN_SAMPLES_PER_LOAD_SNAPSHOT = 4;
  private static final Integer ENTITY = 1234;
  private static final Integer ENTITY2 = 5678;

  @Test
  public void testAddSampleInDifferentWindows() {
    MetricSampleAggregator<Integer> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT);
    populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator);

    List<WindowAggregationResult<Integer>> windowResults =
        aggregator.windowAggregationResults(-1, Long.MAX_VALUE, Collections.emptySet());
    assertNotNull(windowResults);
    assertEquals(NUM_SNAPSHOTS, windowResults.size());
    for (int i = 0; i < NUM_SNAPSHOTS; i++) {
      WindowAggregationResult<Integer> windowResult = windowResults.get(i);
      assertEquals((NUM_SNAPSHOTS - i) * SNAPSHOT_WINDOW_MS, windowResult.window());
      Snapshot snapshot = windowResult.snapshots().get(ENTITY);
      for (Resource resource : Resource.values()) {
        double expectedValue = resource == Resource.DISK ?
            (NUM_SNAPSHOTS - 1 - i) * 10 + MIN_SAMPLES_PER_LOAD_SNAPSHOT - 1 :
            (NUM_SNAPSHOTS - 1 - i) * 10 + (MIN_SAMPLES_PER_LOAD_SNAPSHOT - 1) / 2.0;
        assertEquals("The utilization for " + resource + " should be " + expectedValue,
                     expectedValue, snapshot.utilizationFor(resource), 0);
      }
    }

    assertEquals(NUM_SNAPSHOTS + 1, aggregator.numSnapshotWindows());
    assertEquals(NUM_SNAPSHOTS, aggregator.availableSnapshotWindows(-1, Long.MAX_VALUE).size());
  }

  @Test
  public void testGeneration() {
    MetricSampleAggregator<Integer> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT);

    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, ENTITY, 0, SNAPSHOT_WINDOW_MS);
    assertEquals(NUM_SNAPSHOTS + 1, aggregator.currentGeneration());

    List<WindowAggregationResult<Integer>> windowResults =
        aggregator.windowAggregationResults(-1, Long.MAX_VALUE, Collections.emptySet());
    for (int i = 0; i < NUM_SNAPSHOTS; i++) {
      assertEquals(NUM_SNAPSHOTS - i, windowResults.get(i).generation().intValue());
    }

    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1,
                                                        aggregator, ENTITY2, 1, SNAPSHOT_WINDOW_MS);
    windowResults = aggregator.windowAggregationResults(-1, Long.MAX_VALUE, Collections.emptySet());
    assertEquals(NUM_SNAPSHOTS + 2, windowResults.get(NUM_SNAPSHOTS - 2).generation().intValue());
  }

  @Test
  public void testBasicCache() {
    MetricSampleAggregator<Integer> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT);

    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, ENTITY, 0, SNAPSHOT_WINDOW_MS);
    assertEquals(NUM_SNAPSHOTS + 1, aggregator.currentGeneration());

    List<WindowAggregationResult<Integer>> windowResults1 =
        aggregator.windowAggregationResults(-1, Long.MAX_VALUE, Collections.emptySet());

    List<WindowAggregationResult<Integer>> windowResults2 =
        aggregator.windowAggregationResults(-1, Long.MAX_VALUE, Collections.emptySet());

    assertEquals(NUM_SNAPSHOTS, windowResults1.size());
    assertEquals(NUM_SNAPSHOTS, windowResults2.size());

    for (int i = 0; i < NUM_SNAPSHOTS; i++) {
      assertTrue(windowResults1.get(i) == windowResults2.get(i));
    }
  }

  @Test
  public void testCacheInvalidatedWithNewSample() {
    MetricSampleAggregator<Integer> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT);

    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, ENTITY, 0, SNAPSHOT_WINDOW_MS);
    assertEquals(NUM_SNAPSHOTS + 1, aggregator.currentGeneration());

    List<WindowAggregationResult<Integer>> windowResults1 =
        aggregator.windowAggregationResults(-1, Long.MAX_VALUE, Collections.emptySet());

    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1,
                                                        aggregator, ENTITY2, 1, SNAPSHOT_WINDOW_MS);

    List<WindowAggregationResult<Integer>> windowResults2 =
        aggregator.windowAggregationResults(-1, Long.MAX_VALUE, Collections.emptySet());

    assertEquals(NUM_SNAPSHOTS, windowResults1.size());
    assertEquals(NUM_SNAPSHOTS, windowResults2.size());

    for (int i = 0; i < NUM_SNAPSHOTS; i++) {
      if (i == NUM_SNAPSHOTS - 2) {
        assertFalse(windowResults1.get(i) == windowResults2.get(i));
      } else {
        assertTrue(windowResults1.get(i) == windowResults2.get(i));
      }
    }
  }

  @Test
  public void testCacheInvalidatedWithDifferentEntitySet() {
    MetricSampleAggregator<Integer> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT);
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, ENTITY, 0, SNAPSHOT_WINDOW_MS);

    List<WindowAggregationResult<Integer>> windowResults1 =
        aggregator.windowAggregationResults(-1, Long.MAX_VALUE, Collections.singleton(ENTITY));

    List<WindowAggregationResult<Integer>> windowResults2 =
        aggregator.windowAggregationResults(-1, Long.MAX_VALUE, new HashSet<>(Arrays.asList(ENTITY, ENTITY2)));

    List<WindowAggregationResult<Integer>> windowResults3 =
        aggregator.windowAggregationResults(-1, Long.MAX_VALUE, Collections.singleton(ENTITY));

    for (int i = 0; i < NUM_SNAPSHOTS; i++) {
      assertFalse(windowResults1.get(i) == windowResults2.get(i));
      assertTrue(windowResults2.get(i) == windowResults3.get(i));
    }
  }

  @Test
  public void testEvictOldSnapshots() {
    MetricSampleAggregator<Integer> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT);

    // The eviction should still work after throwing exceptions.
    try {
      aggregator.windowAggregationResults(Long.MAX_VALUE, -1, null);
    } catch (IllegalArgumentException e) {
      // let it go.
    }
    populateSampleAggregator(NUM_SNAPSHOTS + 2, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator);

    List<WindowAggregationResult<Integer>> windowResults =
        aggregator.windowAggregationResults(-1, Long.MAX_VALUE, Collections.emptySet());
    assertEquals("Only " + NUM_SNAPSHOTS + " should be returned.", NUM_SNAPSHOTS, windowResults.size());
    for (int i = 0; i < NUM_SNAPSHOTS; i++) {
      assertEquals((NUM_SNAPSHOTS + 1 - i) * SNAPSHOT_WINDOW_MS, windowResults.get(i).window());
    }
    assertEquals(NUM_SNAPSHOTS + 1, aggregator.numSnapshotWindows());
  }

  @Test
  public void testFallbackToAvgAvailable() {
    MetricSampleAggregator<Integer> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT);

    // Only give one sample to the aggregator.
    populateSampleAggregator(2, 1, aggregator);
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, ENTITY, 2, SNAPSHOT_WINDOW_MS);

    List<WindowAggregationResult<Integer>> windowResults =
        aggregator.windowAggregationResults(-1, Long.MAX_VALUE, Collections.emptySet());

    assertEquals(NUM_SNAPSHOTS, windowResults.size());
    assertInvalid(windowResults.get(NUM_SNAPSHOTS - 1), ENTITY, FORCED_INSUFFICIENT);
    assertInvalid(windowResults.get(NUM_SNAPSHOTS - 2), ENTITY, FORCED_INSUFFICIENT);
    for (int i = 0; i < NUM_SNAPSHOTS - 2; i++) {
      assertValid(windowResults.get(i), ENTITY);
    }

    // Fill in some samples to meet the minimum requirements.
    populateSampleAggregator(2, MIN_SAMPLES_PER_LOAD_SNAPSHOT - 2, aggregator);
    windowResults = aggregator.windowAggregationResults(-1, Long.MAX_VALUE, Collections.emptySet());

    assertEquals(NUM_SNAPSHOTS, windowResults.size());
    assertImputation(windowResults.get(NUM_SNAPSHOTS - 1), ENTITY, AVG_AVAILABLE);
    assertImputation(windowResults.get(NUM_SNAPSHOTS - 2), ENTITY, AVG_AVAILABLE);
    for (int i = 0; i < NUM_SNAPSHOTS - 2; i++) {
      assertValid(windowResults.get(i), ENTITY);
    }
  }

  @Test
  public void testFallbackToAvgAdjacent() {
    MetricSampleAggregator<Integer> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT);

    // Let window 0 have enough samples.
    CruiseControlUnitTestUtils.populateSampleAggregator(1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator,
                                                        ENTITY, 0, SNAPSHOT_WINDOW_MS);
    // Let a snapshot window exist but not containing samples for partition 0
    CruiseControlUnitTestUtils.populateSampleAggregator(1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator,
                                                        ENTITY2, 1, SNAPSHOT_WINDOW_MS);
    // Let the rest of the snapshot has enough samples.
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 1,
                                                        MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, ENTITY,
                                                        2,
                                                        SNAPSHOT_WINDOW_MS);

    List<WindowAggregationResult<Integer>> windowResults =
        aggregator.windowAggregationResults(-1, Long.MAX_VALUE, new HashSet<>(Arrays.asList(ENTITY, ENTITY2)));

    assertEquals(NUM_SNAPSHOTS, windowResults.size());
    for (int i = 0; i < NUM_SNAPSHOTS; i++) {
      if (i == NUM_SNAPSHOTS - 2) {
        assertImputation(windowResults.get(i), ENTITY, AVG_ADJACENT);
        assertValid(windowResults.get(i), ENTITY2);
      } else {
        assertValid(windowResults.get(i), ENTITY);
        assertInvalid(windowResults.get(i), ENTITY2, NO_VALID_IMPUTATION);
      }
    }
  }

  @Test
  public void testFallbackToAvgAdjacentOnTheEdge() {
    MetricSampleAggregator<Integer> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT);
    
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator,
                                                        ENTITY2, 0, SNAPSHOT_WINDOW_MS);
    // Leave the first and last window empty for entity
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 2, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator,
                                                        ENTITY, 1, SNAPSHOT_WINDOW_MS);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator,
                                                        ENTITY, NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS);


    List<WindowAggregationResult<Integer>> windowResults =
        aggregator.windowAggregationResults(-1, Long.MAX_VALUE, new HashSet<>(Arrays.asList(ENTITY, ENTITY2)));

    assertEquals(NUM_SNAPSHOTS, windowResults.size());
    for (int i = 0; i < NUM_SNAPSHOTS; i++) {
      System.out.println(i + ", " + windowResults.get(i).window());
      if (i == NUM_SNAPSHOTS - 1 || i == 0) {
        assertInvalid(windowResults.get(i), ENTITY, NO_VALID_IMPUTATION);
      } else {
        assertValid(windowResults.get(i), ENTITY);
      }
      assertValid(windowResults.get(i), ENTITY2);
    }
  }

  @Test
  public void testEarliestSnapshotWindow() {
    MetricSampleAggregator<Integer> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT);
    assertNull(aggregator.earliestSnapshotWindow());
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, ENTITY, 0, SNAPSHOT_WINDOW_MS);
    assertEquals(SNAPSHOT_WINDOW_MS, aggregator.earliestSnapshotWindow().longValue());
  }

  @Test
  public void testAllSnapshotWindows() {
    MetricSampleAggregator<Integer> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT);
    assertTrue(aggregator.allSnapshotWindows().isEmpty());
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, ENTITY, 0, SNAPSHOT_WINDOW_MS);
    List<Long> allSnapshotWindows = aggregator.allSnapshotWindows();
    assertEquals(NUM_SNAPSHOTS + 1, allSnapshotWindows.size());
    for (int i = 0; i < NUM_SNAPSHOTS + 1; i++) {
      assertEquals((NUM_SNAPSHOTS + 1 - i) * SNAPSHOT_WINDOW_MS, allSnapshotWindows.get(i).longValue());
    }
  }

  @Test
  public void testAvailableSnapshotWindows() {
    MetricSampleAggregator<Integer> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT);
    assertTrue(aggregator.availableSnapshotWindows().isEmpty());
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, ENTITY, 0, SNAPSHOT_WINDOW_MS);

    List<Long> availableSnapshotWindows = aggregator.availableSnapshotWindows();
    assertEquals(NUM_SNAPSHOTS - 2, availableSnapshotWindows.size());
    for (int i = 0; i < NUM_SNAPSHOTS - 2; i++) {
      assertEquals((NUM_SNAPSHOTS - 2 - i) * SNAPSHOT_WINDOW_MS, availableSnapshotWindows.get(i).longValue());
    }

    CruiseControlUnitTestUtils.populateSampleAggregator(2, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, ENTITY, NUM_SNAPSHOTS - 1,
                                                        SNAPSHOT_WINDOW_MS);

    availableSnapshotWindows = aggregator.availableSnapshotWindows();
    assertEquals(NUM_SNAPSHOTS, availableSnapshotWindows.size());
    for (int i = 0; i < NUM_SNAPSHOTS; i++) {
      assertEquals((NUM_SNAPSHOTS - i) * SNAPSHOT_WINDOW_MS, availableSnapshotWindows.get(i).longValue());
    }

    availableSnapshotWindows = aggregator.availableSnapshotWindows(2 * SNAPSHOT_WINDOW_MS, 3 * SNAPSHOT_WINDOW_MS);
    assertEquals(1, availableSnapshotWindows.size());
    assertEquals(2 * SNAPSHOT_WINDOW_MS, availableSnapshotWindows.get(0).longValue());
  }

  @Test
  public void testConcurrency() throws NotEnoughSnapshotsException {
    final int numThreads = 10;
    final int numEntities = 5;
    final int samplesPerSnapshot = 100;

    // We set the minimum number of samples per snapshot to be the total number of samples to insert.
    // So when there is a sample got lost we will fail to collect enough snapshot.
    final MetricSampleAggregator<Integer> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, samplesPerSnapshot * numThreads * 2);

    final Random random = new Random();
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      Thread t = new Thread() {
        @Override
        public void run() {
          // Add samples for 10 random partitions.
          int startingEntity = random.nextInt(5) % numEntities;
          for (int i = 0; i < 10; i++) {
            int entity = (startingEntity + i) % numEntities;
            populateSampleAggregator(NUM_SNAPSHOTS + 1, samplesPerSnapshot, aggregator, entity);
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
                 aggregator.totalNumSamples());

    List<WindowAggregationResult<Integer>> windowResults =
        aggregator.windowAggregationResults(-1, Long.MAX_VALUE, Collections.emptySet());
    assertEquals(NUM_SNAPSHOTS, windowResults.size());
    for (int i = 0; i < NUM_SNAPSHOTS; i++) {
      for (int j = 0; j < numEntities; j++) {
        assertValid(windowResults.get(i), j);
      }
    }
  }

  private void assertValid(WindowAggregationResult<Integer> windowResult, Integer entity) {
    assertFalse(windowResult.invalidEntities().containsKey(entity));
    assertFalse(windowResult.entityWithImputations().containsKey(entity));
    assertTrue(windowResult.snapshots().containsKey(entity));
  }

  private void assertImputation(WindowAggregationResult<Integer> windowResult,
                                Integer entity,
                                SnapshotAndImputation.Imputation imputation) {
    assertFalse(windowResult.invalidEntities().containsKey(entity));
    assertEquals(imputation, windowResult.entityWithImputations().get(entity));
    assertTrue(windowResult.snapshots().containsKey(entity));
  }

  private void assertInvalid(WindowAggregationResult<Integer> windowResult,
                             Integer entity,
                             SnapshotAndImputation.Imputation imputation) {
    assertEquals(imputation, windowResult.invalidEntities().get(entity));
    assertEquals(imputation, windowResult.entityWithImputations().get(entity));
    assertTrue(windowResult.snapshots().containsKey(entity));
  }

  private void populateSampleAggregator(int numSnapshots,
                                        int numSamplesPerSnapshot,
                                        MetricSampleAggregator<Integer> metricSampleAggregator) {
    populateSampleAggregator(numSnapshots, numSamplesPerSnapshot, metricSampleAggregator, ENTITY);
  }

  private void populateSampleAggregator(int numSnapshots,
                                        int numSamplesPerSnapshot,
                                        MetricSampleAggregator<Integer> metricSampleAggregator,
                                        Integer entity) {
    CruiseControlUnitTestUtils.populateSampleAggregator(numSnapshots, numSamplesPerSnapshot, metricSampleAggregator,
                                                        entity, 0, SNAPSHOT_WINDOW_MS);
  }
}
