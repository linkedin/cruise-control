/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.IntegerEntity;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.metricdef.AggregationFunction;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class RawMetricValuesTest {
  private static final float EPSILON = 0.01f;
  private static final int NUM_WINDOWS = 5;
  private static final int NUM_WINDOWS_TO_KEEP = NUM_WINDOWS + 1;
  private static final int MIN_SAMPLES_PER_WINDOW = 4;
  private MetricDef _metricDef;

  @Before
  public void setup() {
    _metricDef = new MetricDef().define("metric1", AggregationFunction.AVG.name())
                                .define("metric2", AggregationFunction.MAX.name())
                                .define("metric3", AggregationFunction.LATEST.name());
  }
  
  @Test
  public void testAddSampleToEvictedWindows() {
    RawMetricValues rawValues = new RawMetricValues(2, MIN_SAMPLES_PER_WINDOW);
    rawValues.updateOldestWindowIndex(2);
    MetricSample<String, IntegerEntity> m1 = getMetricSample(10, 10, 10);
    rawValues.addSample(m1, 1, _metricDef);
    assertEquals(0, rawValues.numSamples());
  }
  
  @Test
  public void testAddSampleUpdateImputation() {
    // Let the minSamplePerWindow to be MIN_SAMPLE_PER_WINDOW + 1 so all the windows needs imputation.
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW + 1);
    // All the window index should be 2,3,4,5,6,7
    prepareWindowMissingAtIndex(rawValues, Arrays.asList(3, 5), 2);
    // now add sample to window 2 and 6 to make them valid without flaws.
    MetricSample<String, IntegerEntity> m = getMetricSample(10, 10, 10);
    rawValues.addSample(m, 2, _metricDef);
    rawValues.addSample(m, 6, _metricDef);
    assertTrue(rawValues.isValidAtWindowIndex(2));
    assertFalse(rawValues.isFlawedAtWindowIndex(2));
    assertTrue(rawValues.isValidAtWindowIndex(6));
    assertFalse(rawValues.isFlawedAtWindowIndex(6));
    // At this point window 3 and 5 should still be invalid
    assertFalse(rawValues.isValidAtWindowIndex(3));
    assertFalse(rawValues.isFlawedAtWindowIndex(3));
    assertFalse(rawValues.isValidAtWindowIndex(5));
    assertFalse(rawValues.isFlawedAtWindowIndex(5));
    
    // now adding sample to 4 should make 3 and 5 valid with flaw of ADJACENT_AVG
    rawValues.addSample(m, 4, _metricDef);
    assertTrue(rawValues.isValidAtWindowIndex(4));
    assertFalse(rawValues.isFlawedAtWindowIndex(4));
    assertTrue(rawValues.isValidAtWindowIndex(3));
    assertTrue(rawValues.isFlawedAtWindowIndex(3));
    assertTrue(rawValues.isValidAtWindowIndex(5));
    assertTrue(rawValues.isFlawedAtWindowIndex(5));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void testAddToWindowLargerThanCurrentWindow() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW);
    rawValues.updateOldestWindowIndex(0);
    rawValues.addSample(getMetricSample(10, 10, 10), NUM_WINDOWS_TO_KEEP, _metricDef);
  }

  @Test
  public void testAggregateSingleWindow() {
    RawMetricValues rawValues = new RawMetricValues(2, MIN_SAMPLES_PER_WINDOW);
    rawValues.updateOldestWindowIndex(0);
    MetricSample<String, IntegerEntity> m1 = getMetricSample(10, 10, 10);
    MetricSample<String, IntegerEntity> m2 = getMetricSample(6, 6, 6);
    MetricSample<String, IntegerEntity> m3 = getMetricSample(2, 12, 8);
    MetricSample<String, IntegerEntity> m4 = getMetricSample(18, 10, 2);

    // No sample
    ValuesAndImputations valuesAndImputations = aggregate(rawValues, new TreeSet<>(Collections.singleton(0L)));
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(0).get(0), EPSILON);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(1).get(0), EPSILON);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(2).get(0), EPSILON);
    assertEquals(1, valuesAndImputations.imputations().size());
    Assert.assertEquals(Imputation.NO_VALID_IMPUTATION, valuesAndImputations.imputations().get(0));

    // Add the first sample
    addSample(rawValues, m1, 0);
    valuesAndImputations = aggregate(rawValues, new TreeSet<>(Collections.singleton(0L)));
    assertEquals(10, valuesAndImputations.metricValues().valuesFor(0).get(0), EPSILON);
    assertEquals(10, valuesAndImputations.metricValues().valuesFor(1).get(0), EPSILON);
    assertEquals(10, valuesAndImputations.metricValues().valuesFor(2).get(0), EPSILON);
    assertEquals(1, valuesAndImputations.imputations().size());
    Assert.assertEquals(Imputation.FORCED_INSUFFICIENT, valuesAndImputations.imputations().get(0));

    // Add the second sample
    addSample(rawValues, m2, 0);
    valuesAndImputations = aggregate(rawValues, new TreeSet<>(Collections.singleton(0L)));
    assertEquals(8, valuesAndImputations.metricValues().valuesFor(0).get(0), EPSILON);
    assertEquals(10, valuesAndImputations.metricValues().valuesFor(1).get(0), EPSILON);
    assertEquals(6, valuesAndImputations.metricValues().valuesFor(2).get(0), EPSILON);
    assertEquals(1, valuesAndImputations.imputations().size());
    Assert.assertEquals(Imputation.AVG_AVAILABLE, valuesAndImputations.imputations().get(0));

    // Add the third sample
    addSample(rawValues, m3, 0);
    valuesAndImputations = aggregate(rawValues, new TreeSet<>(Collections.singleton(0L)));
    assertEquals(6, valuesAndImputations.metricValues().valuesFor(0).get(0), EPSILON);
    assertEquals(12, valuesAndImputations.metricValues().valuesFor(1).get(0), EPSILON);
    assertEquals(8, valuesAndImputations.metricValues().valuesFor(2).get(0), EPSILON);
    assertEquals(1, valuesAndImputations.imputations().size());
    Assert.assertEquals(Imputation.AVG_AVAILABLE, valuesAndImputations.imputations().get(0));

    // Add the fourth sample
    addSample(rawValues, m4, 0);
    valuesAndImputations = aggregate(rawValues, new TreeSet<>(Collections.singleton(0L)));
    assertEquals(9, valuesAndImputations.metricValues().valuesFor(0).get(0), EPSILON);
    assertEquals(12, valuesAndImputations.metricValues().valuesFor(1).get(0), EPSILON);
    assertEquals(2, valuesAndImputations.metricValues().valuesFor(2).get(0), EPSILON);
    assertEquals(0, valuesAndImputations.imputations().size());
  }

  @Test
  public void testAggregateMultipleWindows() {
    for (int i = 0; i < NUM_WINDOWS * 2; i++) {
      RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW);
      rawValues.updateOldestWindowIndex(i);
      float[][] expected = populate(rawValues, i);
      ValuesAndImputations valuesAndImputations = aggregate(rawValues, allIndexes(i));
      assertAggregatedValues(valuesAndImputations.metricValues(), expected, i);
    }
  }

  @Test
  public void testImputationAdjacentAvgAtMiddle() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW);
    prepareWindowMissingAtIndex(rawValues, 1);
    ValuesAndImputations valuesAndImputations = aggregate(rawValues, allIndexes(0));
    assertEquals(11.5, valuesAndImputations.metricValues().valuesFor(0).get(1), EPSILON);
    assertEquals(13.0, valuesAndImputations.metricValues().valuesFor(1).get(1), EPSILON);
    assertEquals(13.0, valuesAndImputations.metricValues().valuesFor(2).get(1), EPSILON);
    assertEquals(1, valuesAndImputations.imputations().size());
    Assert.assertEquals(Imputation.AVG_ADJACENT, valuesAndImputations.imputations().get(1));
  }

  @Test
  public void testImputationAdjacentAvgAtLeftEdge() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW);
    prepareWindowMissingAtIndex(rawValues, 0);
    ValuesAndImputations valuesAndImputations = aggregate(rawValues, allIndexes(0));
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(0).get(0), EPSILON);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(1).get(0), EPSILON);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(2).get(0), EPSILON);
    assertEquals(1, valuesAndImputations.imputations().size());
    Assert.assertEquals(Imputation.NO_VALID_IMPUTATION, valuesAndImputations.imputations().get(0));
  }

  @Test
  public void testImputationAdjacentAvgAtRightEdge() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW);
    prepareWindowMissingAtIndex(rawValues, NUM_WINDOWS);
    rawValues.updateOldestWindowIndex(1);
    ValuesAndImputations valuesAndImputations = aggregate(rawValues, allIndexes(1));
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(0).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(1).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(2).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(1, valuesAndImputations.imputations().size());
    Assert.assertEquals(Imputation.NO_VALID_IMPUTATION, valuesAndImputations.imputations().get(NUM_WINDOWS - 1));
  }

  @Test
  public void testImputationAdjacentAvgAtLeftEdgeWithWrapAround() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW);
    prepareWindowMissingAtIndex(rawValues, 0);

    // When oldest window index is 0, position 0 is the first index and should have no imputation.
    ValuesAndImputations valuesAndImputations = rawValues.aggregate(allIndexes(0), _metricDef);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(0).get(0), EPSILON);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(1).get(0), EPSILON);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(2).get(0), EPSILON);
    assertEquals(1, valuesAndImputations.imputations().size());
    Assert.assertEquals(Imputation.NO_VALID_IMPUTATION, valuesAndImputations.imputations().get(0));

    // When oldest window index is 2, position 0 is the last index and should have no imputation.
    rawValues.updateOldestWindowIndex(2);
    valuesAndImputations = rawValues.aggregate(allIndexes(2), _metricDef);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(0).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(1).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(2).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(1, valuesAndImputations.imputations().size());
    Assert.assertEquals(Imputation.NO_VALID_IMPUTATION, valuesAndImputations.imputations().get(NUM_WINDOWS - 1));

    // when the oldest window index is 3, position 0 is the 3rd index. There should be an imputation.
    rawValues.updateOldestWindowIndex(3);
    valuesAndImputations = rawValues.aggregate(allIndexes(3), _metricDef);
    assertEquals(31.5, valuesAndImputations.metricValues().valuesFor(0).get(3), EPSILON);
    assertEquals(33.0, valuesAndImputations.metricValues().valuesFor(1).get(3), EPSILON);
    assertEquals(33.0, valuesAndImputations.metricValues().valuesFor(2).get(3), EPSILON);
    assertEquals(1, valuesAndImputations.imputations().size());
    Assert.assertEquals(Imputation.AVG_ADJACENT, valuesAndImputations.imputations().get(3));
  }

  @Test
  public void testImputationAdjacentAvgAtRightEdgeWithWrapAround() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW);
    prepareWindowMissingAtIndex(rawValues, NUM_WINDOWS);
    // When oldest window index is 1, position NUM_WINDOWS is the last index and should have no imputation.
    rawValues.updateOldestWindowIndex(1);
    ValuesAndImputations valuesAndImputations = rawValues.aggregate(allIndexes(1), _metricDef);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(0).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(1).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(2).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(1, valuesAndImputations.imputations().size());
    Assert.assertEquals(Imputation.NO_VALID_IMPUTATION, valuesAndImputations.imputations().get(NUM_WINDOWS - 1));

    // When oldest window index is NUM_WINDOWS, position NUM_WINDOWS is the first index, it should have no imputation.
    rawValues.updateOldestWindowIndex(NUM_WINDOWS);
    valuesAndImputations = rawValues.aggregate(allIndexes(NUM_WINDOWS), _metricDef);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(0).get(0), EPSILON);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(1).get(0), EPSILON);
    assertEquals(0, valuesAndImputations.metricValues().valuesFor(2).get(0), EPSILON);
    assertEquals(1, valuesAndImputations.imputations().size());
    Assert.assertEquals(Imputation.NO_VALID_IMPUTATION, valuesAndImputations.imputations().get(0));

    // when the oldest window index is 3, position NUM_WINDOWS is the 3rd index. There should be an imputation.
    rawValues.updateOldestWindowIndex(3);
    valuesAndImputations = rawValues.aggregate(allIndexes(3), _metricDef);
    assertEquals(21.5, valuesAndImputations.metricValues().valuesFor(0).get(2), EPSILON);
    assertEquals(23.0, valuesAndImputations.metricValues().valuesFor(1).get(2), EPSILON);
    assertEquals(23.0, valuesAndImputations.metricValues().valuesFor(2).get(2), EPSILON);
    assertEquals(1, valuesAndImputations.imputations().size());
    Assert.assertEquals(Imputation.AVG_ADJACENT, valuesAndImputations.imputations().get(2));
  }

  @Test
  public void testAdjacentAvgAtEdgeWhenNewWindowRollsOut() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW);
    prepareWindowMissingAtIndex(rawValues, NUM_WINDOWS - 1);

    assertFalse(rawValues.isValidAtWindowIndex(NUM_WINDOWS - 1));
    assertFalse(rawValues.isFlawedAtWindowIndex(NUM_WINDOWS - 1));

    rawValues.updateOldestWindowIndex(1);

    assertTrue(rawValues.isValidAtWindowIndex(NUM_WINDOWS - 1));
    assertTrue(rawValues.isFlawedAtWindowIndex(NUM_WINDOWS - 1));
  }

  @Test
  public void testAdjacentAvgAtEdgeWhenNewWindowRollsOutWithLargeLeap() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW);
    prepareWindowMissingAtIndex(rawValues, NUM_WINDOWS - 1);

    assertFalse(rawValues.isValidAtWindowIndex(NUM_WINDOWS - 1));
    assertFalse(rawValues.isFlawedAtWindowIndex(NUM_WINDOWS - 1));

    rawValues.updateOldestWindowIndex(NUM_WINDOWS - 1);

    assertFalse(rawValues.isValidAtWindowIndex(NUM_WINDOWS - 1));
    assertFalse(rawValues.isFlawedAtWindowIndex(NUM_WINDOWS - 1));
  }

  @Test
  public void testIsValid() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW);
    rawValues.updateOldestWindowIndex(0);
    MetricSample<String, IntegerEntity> m = getMetricSample(10, 10, 10);

    for (int i = 0; i < NUM_WINDOWS_TO_KEEP; i++) {
      for (int j = 0; j < MIN_SAMPLES_PER_WINDOW - 1; j++) {
        addSample(rawValues, m, i);
      }
    }
    assertTrue(rawValues.isValid(5));
    assertFalse(rawValues.isValid(4));
    addSample(rawValues, m, 0);
    assertTrue(rawValues.isValid(4));
  }

  private void assertAggregatedValues(AggregatedMetricValues values, float[][] expectedValues, int startingIndex) {
    for (int metricId = 0; metricId < _metricDef.all().size(); metricId++) {
      MetricValues actualValues = values.valuesFor(metricId);
      for (int i = 0; i < NUM_WINDOWS; i++) {
        assertEquals(String.format("[%d, %d] does not match.", metricId, i),
                     expectedValues[metricId][(i + startingIndex) % NUM_WINDOWS_TO_KEEP],
                     actualValues.get(i % NUM_WINDOWS),
                     EPSILON);
      }
    }
  }

  private void prepareWindowMissingAtIndex(RawMetricValues rawValues, int idx) {
    prepareWindowMissingAtIndex(rawValues, Collections.singleton(idx), 0);
  }
  
  private void prepareWindowMissingAtIndex(RawMetricValues rawValues, Collection<Integer> indexes, int oldestWindowIndex) {
    rawValues.updateOldestWindowIndex(oldestWindowIndex);
    populate(rawValues, oldestWindowIndex);
    // reset index, we need to play the trick to change the oldest window index temporarily to make it work.
    rawValues.updateOldestWindowIndex(oldestWindowIndex + 100);
    indexes.forEach(idx -> rawValues.resetWindowIndexes(idx, 1));
    rawValues.updateOldestWindowIndex(oldestWindowIndex);
  }

  private float[][] populate(RawMetricValues rawValues, long startingWindowIndex) {
    float[][] expected = new float[_metricDef.size()][NUM_WINDOWS_TO_KEEP];
    for (long windowIdx = startingWindowIndex; windowIdx < startingWindowIndex + NUM_WINDOWS_TO_KEEP; windowIdx++) {
      for (int i = 0; i < MIN_SAMPLES_PER_WINDOW; i++) {
        float v = 10 * windowIdx + i;
        MetricSample<String, IntegerEntity> m = getMetricSample(v, v, v);
        rawValues.addSample(m, windowIdx, _metricDef);

        for (MetricInfo info : _metricDef.all()) {
          switch (info.strategy()) {
            case AVG:
              expected[info.id()][(int) (windowIdx % NUM_WINDOWS_TO_KEEP)] += v / MIN_SAMPLES_PER_WINDOW;
              break;
            case MAX:
            case LATEST:
              expected[info.id()][(int) (windowIdx % NUM_WINDOWS_TO_KEEP)] = v;
              break;
            default:
              throw new IllegalStateException("Should never be here");
          }
        }
      }
    }
    return expected;
  }

  private SortedSet<Long> allIndexes(int oldestWindowIndex) {
    SortedSet<Long> indexes = new TreeSet<>();
    for (long i = 0; i < NUM_WINDOWS; i++) {
      indexes.add(oldestWindowIndex + i);
    }
    return indexes;
  }

  private void addSample(RawMetricValues rawValues, MetricSample<String, IntegerEntity> metricSample, int index) {
    rawValues.addSample(metricSample, index, _metricDef);
  }

  private ValuesAndImputations aggregate(RawMetricValues rawValues, SortedSet<Long> indexes) {
    return rawValues.aggregate(indexes, _metricDef);
  }

  private MetricSample<String, IntegerEntity> getMetricSample(float v1, float v2, float v3) {
    MetricSample<String, IntegerEntity> metricSample = new MetricSample<>(new IntegerEntity("group", 0));
    metricSample.record("metric1", v1, _metricDef);
    metricSample.record("metric2", v2, _metricDef);
    metricSample.record("metric3", v3, _metricDef);
    metricSample.close(0);
    return metricSample;
  }
}
