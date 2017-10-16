/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.cruisecontrol.IntegerEntity;
import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.metricdef.ValueComputingStrategy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for {@link MetricSampleAggregator}
 */
public class MetricSampleAggregatorTest {
  private static final float EPSILON = 0.01f;
  private static final int NUM_SNAPSHOTS = 20;
  private static final long SNAPSHOT_WINDOW_MS = 1000L;
  private static final int MIN_SAMPLES_PER_LOAD_SNAPSHOT = 4;
  private static final IntegerEntity ENTITY1 = new IntegerEntity("g1", 1234);
  private static final IntegerEntity ENTITY2 = new IntegerEntity("g1", 5678);
  private static final IntegerEntity ENTITY3 = new IntegerEntity("g2", 1234);
  private final MetricDef _metricDef = CruiseControlUnitTestUtils.getMetricDef();

  @Test
  public void testAddSampleInDifferentWindows() throws NotEnoughValidWindowsException {
    MetricSampleAggregator<String, IntegerEntity> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                     0, _metricDef);
    // The remaining windows should NUM_SNAPSHOT - 2 to 2 * NUM_SNAPSHOT - 3;
    populateSampleAggregator(2 * NUM_SNAPSHOTS - 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator);

    AggregationOptions<String, IntegerEntity> options =
        new AggregationOptions<>(1, 1, NUM_SNAPSHOTS, Collections.emptySet(),
                                 AggregationOptions.Granularity.ENTITY_GROUP);
    MetricSampleAggregationResult<String, IntegerEntity> aggResults =
        aggregator.aggregate(-1, Long.MAX_VALUE, options);
    assertNotNull(aggResults);

    assertEquals(1, aggResults.valuesAndImputations().size());
    
    for (Map.Entry<IntegerEntity, ValuesAndImputations> entry : aggResults.valuesAndImputations().entrySet()) {
      ValuesAndImputations valuesAndImputations = entry.getValue();
      List<Long> windows = valuesAndImputations.windows();
      assertEquals(NUM_SNAPSHOTS, windows.size());
      for (int i = 0; i < NUM_SNAPSHOTS; i++) {
        assertEquals((2 * NUM_SNAPSHOTS - 2 - i) * SNAPSHOT_WINDOW_MS, windows.get(i).longValue());
      }
      for (MetricInfo info : _metricDef.all().values()) {
        MetricValues valuesForMetric = valuesAndImputations.metricValues().valuesFor(info.id());
        for (int i = 0; i < NUM_SNAPSHOTS; i++) {
          double expectedValue;
          if (info.strategy() == ValueComputingStrategy.LATEST || info.strategy() == ValueComputingStrategy.MAX) {
            expectedValue = (2 * NUM_SNAPSHOTS - 3 - i) * 10 + MIN_SAMPLES_PER_LOAD_SNAPSHOT - 1;
          } else {
            expectedValue = (2 * NUM_SNAPSHOTS - 3 - i) * 10 + (MIN_SAMPLES_PER_LOAD_SNAPSHOT - 1) / 2.0;
          }
          assertEquals("The utilization for " + info.name() + " should be " + expectedValue,
                       expectedValue, valuesForMetric.get(i % NUM_SNAPSHOTS), 0);
        }
      }
    }

    assertEquals(NUM_SNAPSHOTS + 1, aggregator.allWindows().size());
    assertEquals(NUM_SNAPSHOTS, aggregator.numAvailableWindows());
  }

  @Test
  public void testGeneration() throws NotEnoughValidWindowsException {
    MetricSampleAggregator<String, IntegerEntity> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                     0, _metricDef);

    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, ENTITY1, 0, SNAPSHOT_WINDOW_MS,
                                                        _metricDef);
    assertEquals(NUM_SNAPSHOTS + 1, aggregator.generation().intValue());

    AggregationOptions<String, IntegerEntity> options =
        new AggregationOptions<>(1, 1, NUM_SNAPSHOTS, Collections.emptySet(),
                                 AggregationOptions.Granularity.ENTITY_GROUP);
    MetricSampleAggregatorState<String, IntegerEntity> windowState = aggregator.aggregatorState();
    for (int i = 1; i < NUM_SNAPSHOTS + 1; i++) {
      assertEquals(NUM_SNAPSHOTS + 1, windowState.windowStates().get((long) i).generation().intValue());
    }

    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1,
                                                        aggregator, ENTITY2, 1, SNAPSHOT_WINDOW_MS, _metricDef);
    aggregator.completeness(-1, Long.MAX_VALUE, options);
    assertEquals(NUM_SNAPSHOTS + 2, windowState.windowStates().get((long) 2).generation().intValue());
  }

  @Test
  public void testEarliestSnapshotWindow() {
    MetricSampleAggregator<String, IntegerEntity> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                     0, _metricDef);
    assertNull(aggregator.earliestWindow());
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, ENTITY1, 0, SNAPSHOT_WINDOW_MS,
                                                        _metricDef);
    assertEquals(SNAPSHOT_WINDOW_MS, aggregator.earliestWindow().longValue());
    CruiseControlUnitTestUtils.populateSampleAggregator(2, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, ENTITY1, NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS,
                                                        _metricDef);
    assertEquals(2 * SNAPSHOT_WINDOW_MS, aggregator.earliestWindow().longValue());
  }

  @Test
  public void testAllSnapshotWindows() {
    MetricSampleAggregator<String, IntegerEntity> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                     0, _metricDef);
    assertTrue(aggregator.allWindows().isEmpty());
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                                        aggregator, ENTITY1, 0, SNAPSHOT_WINDOW_MS,
                                                        _metricDef);
    List<Long> allStWindows = aggregator.allWindows();
    assertEquals(NUM_SNAPSHOTS + 1, allStWindows.size());
    for (int i = 0; i < NUM_SNAPSHOTS + 1; i++) {
      assertEquals((i + 1) * SNAPSHOT_WINDOW_MS, allStWindows.get(i).longValue());
    }
  }

  @Test
  public void testAvailableSnapshotWindows() {
    MetricSampleAggregator<String, IntegerEntity> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                     0, _metricDef);
    assertTrue(aggregator.availableWindows().isEmpty());
    CruiseControlUnitTestUtils.populateSampleAggregator(1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator,
                                                        ENTITY1, 0, SNAPSHOT_WINDOW_MS, _metricDef);
    assertTrue(aggregator.availableWindows().isEmpty());
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 2, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator,
                                                        ENTITY1, 1, SNAPSHOT_WINDOW_MS, _metricDef);
    List<Long> availableSnapshotWindows = aggregator.availableWindows();
    assertEquals(NUM_SNAPSHOTS - 2, availableSnapshotWindows.size());
    for (int i = 0; i < NUM_SNAPSHOTS - 2; i++) {
      assertEquals((i + 1) * SNAPSHOT_WINDOW_MS, availableSnapshotWindows.get(i).longValue());
    }
  }

  @Test
  public void testAggregationOption1() throws NotEnoughValidWindowsException {
    MetricSampleAggregator<String, IntegerEntity> aggregator = prepareCompletenessTestEnv();

    // Let the group coverage to be 1
    AggregationOptions<String, IntegerEntity> options =
        new AggregationOptions<>(0.5, 1, NUM_SNAPSHOTS,
                                 new HashSet<>(Arrays.asList(ENTITY1, ENTITY2, ENTITY3)), AggregationOptions.Granularity.ENTITY);
    MetricSampleCompleteness<String, IntegerEntity> completeness =
        aggregator.completeness(-1, Long.MAX_VALUE, options);
    assertTrue(completeness.validWindowIndexes().isEmpty());
    assertTrue(completeness.coveredEntities().isEmpty());
    assertTrue(completeness.coveredEntityGroups().isEmpty());
    assertCompletenessByWindowIndex(completeness);
  }

  @Test
  public void testAggregationOption2() {
    MetricSampleAggregator<String, IntegerEntity> aggregator = prepareCompletenessTestEnv();
    // Change the group coverage requirement to 0, window 3, 4, 20 will be excluded because minEntityCoverage is not met.
    AggregationOptions<String, IntegerEntity> options =
        new AggregationOptions<>(0.5, 0.0, NUM_SNAPSHOTS,
                                 new HashSet<>(Arrays.asList(ENTITY1, ENTITY2, ENTITY3)), AggregationOptions.Granularity.ENTITY);
    MetricSampleCompleteness<String, IntegerEntity> completeness = aggregator.completeness(-1, Long.MAX_VALUE, options);
    assertEquals(17, completeness.validWindowIndexes().size());
    assertFalse(completeness.validWindowIndexes().contains(3L));
    assertFalse(completeness.validWindowIndexes().contains(4L));
    assertFalse(completeness.validWindowIndexes().contains(20L));
    assertEquals(2, completeness.coveredEntities().size());
    assertTrue(completeness.coveredEntities().contains(ENTITY1));
    assertTrue(completeness.coveredEntities().contains(ENTITY3));
    assertEquals(1, completeness.coveredEntityGroups().size());
    assertTrue(completeness.coveredEntityGroups().contains(ENTITY3.group()));
    assertCompletenessByWindowIndex(completeness);
  }

  @Test
  public void testAggregationOption3() {
    MetricSampleAggregator<String, IntegerEntity> aggregator = prepareCompletenessTestEnv();
    // Change the option to have 0.5 as minEntityGroupCoverage. This will exclude window index 3, 4, 20.
    AggregationOptions<String, IntegerEntity> options =
        new AggregationOptions<>(0.0, 0.5, NUM_SNAPSHOTS,
                                 new HashSet<>(Arrays.asList(ENTITY1, ENTITY2, ENTITY3)), AggregationOptions.Granularity.ENTITY);

    MetricSampleCompleteness<String, IntegerEntity> completeness = aggregator.completeness(-1, Long.MAX_VALUE, options);
    assertEquals(17, completeness.validWindowIndexes().size());
    assertFalse(completeness.validWindowIndexes().contains(3L));
    assertFalse(completeness.validWindowIndexes().contains(4L));
    assertFalse(completeness.validWindowIndexes().contains(20L));
    assertEquals(2, completeness.coveredEntities().size());
    assertTrue(completeness.coveredEntities().contains(ENTITY1));
    assertTrue(completeness.coveredEntities().contains(ENTITY3));
    assertEquals(1, completeness.coveredEntityGroups().size());
    assertTrue(completeness.coveredEntityGroups().contains(ENTITY3.group()));
    assertCompletenessByWindowIndex(completeness);
  }

  @Test
  public void testAggregationOption4() {
    MetricSampleAggregator<String, IntegerEntity> aggregator = prepareCompletenessTestEnv();
    // Change the option to have 0.5 as minEntityGroupCoverage. This will exclude window index 3, 4, 20.
    AggregationOptions<String, IntegerEntity> options =
        new AggregationOptions<>(0.0, 0.0, NUM_SNAPSHOTS,
                                 new HashSet<>(Arrays.asList(ENTITY1, ENTITY2, ENTITY3)), AggregationOptions.Granularity.ENTITY);

    MetricSampleCompleteness<String, IntegerEntity> completeness = aggregator.completeness(-1, Long.MAX_VALUE, options);
    assertEquals(20, completeness.validWindowIndexes().size());
    assertEquals(1, completeness.coveredEntities().size());
    assertTrue(completeness.coveredEntities().contains(ENTITY1));
    assertTrue(completeness.coveredEntityGroups().isEmpty());
    assertCompletenessByWindowIndex(completeness);
  }

  @Test
  public void testAggregationOption5() {
    MetricSampleAggregator<String, IntegerEntity> aggregator = prepareCompletenessTestEnv();
    // Change the option to use entity group granularity. In this case ENTITY1 will not be considered as valid entity
    // so there will be no valid windows.
    AggregationOptions<String, IntegerEntity> options =
        new AggregationOptions<>(0.5, 0.0, NUM_SNAPSHOTS,
                                 new HashSet<>(Arrays.asList(ENTITY1, ENTITY2, ENTITY3)), AggregationOptions.Granularity.ENTITY_GROUP);
    MetricSampleCompleteness<String, IntegerEntity> completeness = aggregator.completeness(-1, Long.MAX_VALUE, options);
    assertTrue(completeness.validWindowIndexes().isEmpty());
    assertTrue(completeness.coveredEntities().isEmpty());
    assertTrue(completeness.coveredEntityGroups().isEmpty());
    assertCompletenessByWindowIndex(completeness);
  }

  @Test
  public void testAggregationOption6() {
    MetricSampleAggregator<String, IntegerEntity> aggregator = prepareCompletenessTestEnv();
    // Change the option to use entity group granularity and reduce the minEntityCoverage to 0.3. This will
    // include ENTITY3 except in window 3, 4, 20.
    AggregationOptions<String, IntegerEntity> options =
        new AggregationOptions<>(0.3, 0.0, NUM_SNAPSHOTS,
                                 new HashSet<>(Arrays.asList(ENTITY1, ENTITY2, ENTITY3)), AggregationOptions.Granularity.ENTITY_GROUP);
    MetricSampleCompleteness<String, IntegerEntity> completeness = aggregator.completeness(-1, Long.MAX_VALUE, options);
    assertEquals(17, completeness.validWindowIndexes().size());
    assertFalse(completeness.validWindowIndexes().contains(3L));
    assertFalse(completeness.validWindowIndexes().contains(4L));
    assertFalse(completeness.validWindowIndexes().contains(20L));
    assertEquals(1, completeness.coveredEntities().size());
    assertTrue(completeness.coveredEntities().contains(ENTITY3));
    assertEquals(1, completeness.coveredEntityGroups().size());
    assertTrue(completeness.coveredEntityGroups().contains(ENTITY3.group()));
    assertCompletenessByWindowIndex(completeness);
  }

  @Test
  public void testConcurrency() throws NotEnoughValidWindowsException {
    final int numThreads = 10;
    final int numEntities = 5;
    final int samplesPerSnapshot = 100;
    final int numRandomEntities = 10;

    // We set the minimum number of samples per snapshot to be the total number of samples to insert.
    // So when there is a sample got lost we will fail to collect enough snapshot.
    final MetricSampleAggregator<String, IntegerEntity> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS,
                                     samplesPerSnapshot * numThreads * (numRandomEntities / numEntities),
                                     0, _metricDef);

    final Random random = new Random();
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      Thread t = new Thread() {
        @Override
        public void run() {
          // Add samples for 10 random partitions.
          int startingEntity = random.nextInt(5) % numEntities;
          for (int i = 0; i < numRandomEntities; i++) {
            IntegerEntity entity = new IntegerEntity("group", (startingEntity + i) % numEntities);
            populateSampleAggregator(2 * NUM_SNAPSHOTS + 1, samplesPerSnapshot, aggregator, entity);
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
    assertEquals((NUM_SNAPSHOTS + 1) * samplesPerSnapshot * numRandomEntities * numThreads, aggregator.numSamples());

    AggregationOptions<String, IntegerEntity> options =
        new AggregationOptions<>(1, 1, NUM_SNAPSHOTS, Collections.emptySet(),
                                 AggregationOptions.Granularity.ENTITY_GROUP);
    MetricSampleAggregationResult<String, IntegerEntity> aggResult =
        aggregator.aggregate(-1, Long.MAX_VALUE, options);
    assertEquals(numEntities, aggResult.valuesAndImputations().size());
    assertTrue(aggResult.invalidEntities().isEmpty());
    for (ValuesAndImputations valuesAndImputations : aggResult.valuesAndImputations().values()) {
      assertEquals(NUM_SNAPSHOTS, valuesAndImputations.windows().size());
      assertTrue(valuesAndImputations.imputations().isEmpty());
    }
  }

  private MetricSampleAggregator<String, IntegerEntity> prepareCompletenessTestEnv() {
    MetricSampleAggregator<String, IntegerEntity> aggregator =
        new MetricSampleAggregator<>(NUM_SNAPSHOTS, SNAPSHOT_WINDOW_MS, MIN_SAMPLES_PER_LOAD_SNAPSHOT,
                                     0, _metricDef);
    populateSampleAggregator(NUM_SNAPSHOTS + 1, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator, ENTITY1);
    populateSampleAggregator(2, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator, ENTITY3);
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_SNAPSHOTS - 5, MIN_SAMPLES_PER_LOAD_SNAPSHOT, aggregator,
                                                        ENTITY3, 4, SNAPSHOT_WINDOW_MS, _metricDef);
    return aggregator;
  }

  private void assertCompletenessByWindowIndex(MetricSampleCompleteness<String, IntegerEntity> completeness) {
    for (long wi = 1; wi <= NUM_SNAPSHOTS; wi++) {
      if (wi == 3L || wi == 4L || wi == 20L) {
        assertEquals(1.0f / 3, completeness.entityCoverageByWindowIndex().get(wi), EPSILON);
        assertEquals(0, completeness.entityCoverageWithGroupGranularityByWindowIndex().get(wi), EPSILON);
        assertEquals(0.0, completeness.entityGroupCoverageByWindowIndex().get(wi), EPSILON);
      } else {
        assertEquals(2.0f / 3, completeness.entityCoverageByWindowIndex().get(wi), EPSILON);
        assertEquals(1.0f / 3, completeness.entityCoverageWithGroupGranularityByWindowIndex().get(wi), EPSILON);
        assertEquals(0.5, completeness.entityGroupCoverageByWindowIndex().get(wi), EPSILON);
      }
    }
  }

  private void populateSampleAggregator(int numSnapshots,
                                        int numSamplesPerSnapshot,
                                        MetricSampleAggregator<String, IntegerEntity> metricSampleAggregator) {
    populateSampleAggregator(numSnapshots, numSamplesPerSnapshot, metricSampleAggregator, ENTITY1);
  }

  private void populateSampleAggregator(int numSnapshots,
                                        int numSamplesPerSnapshot,
                                        MetricSampleAggregator<String, IntegerEntity> metricSampleAggregator,
                                        IntegerEntity entity) {
    CruiseControlUnitTestUtils.populateSampleAggregator(numSnapshots, numSamplesPerSnapshot, metricSampleAggregator,
                                                        entity, 0, SNAPSHOT_WINDOW_MS, _metricDef);
  }
}
