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
  private static final byte MIN_SAMPLES_PER_WINDOW = 4;
  private static final int NUM_RAW_METRICS = 3;
  private MetricDef _metricDef;

  /**
   * Setup the unit test.
   */
  @Before
  public void setup() {
    _metricDef = new MetricDef().define("metric1", null, AggregationFunction.AVG.name())
                                .define("metric2", null, AggregationFunction.MAX.name())
                                .define("metric3", null, AggregationFunction.LATEST.name());
  }

  @Test
  public void testAddSampleToEvictedWindows() {
    RawMetricValues rawValues = new RawMetricValues(2, MIN_SAMPLES_PER_WINDOW, NUM_RAW_METRICS);
    rawValues.updateOldestWindowIndex(2);
    MetricSample<String, IntegerEntity> m1 = getMetricSample(10, 10, 10);
    rawValues.addSample(m1, 1, _metricDef);
    assertEquals(0, rawValues.numSamples());
  }

  @Test
  public void testAddSampleUpdateExtrapolation() {
    // Let the minSamplePerWindow to be MIN_SAMPLE_PER_WINDOW + 1 so all the windows needs extrapolation.
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, (byte) (MIN_SAMPLES_PER_WINDOW + 1), NUM_RAW_METRICS);
    // All the window index should be 2,3,4,5,6,7
    prepareWindowMissingAtIndex(rawValues, Arrays.asList(3, 5), 2);
    // now add sample to window 2 and 6 to make them valid without flaws.
    MetricSample<String, IntegerEntity> m = getMetricSample(10, 10, 10);
    rawValues.addSample(m, 2, _metricDef);
    rawValues.addSample(m, 6, _metricDef);
    assertTrue(rawValues.isValidAtWindowIndex(2));
    assertFalse(rawValues.isExtrapolatedAtWindowIndex(2));
    assertTrue(rawValues.isValidAtWindowIndex(6));
    assertFalse(rawValues.isExtrapolatedAtWindowIndex(6));
    // At this point window 3 and 5 should still be invalid
    assertFalse(rawValues.isValidAtWindowIndex(3));
    assertFalse(rawValues.isExtrapolatedAtWindowIndex(3));
    assertFalse(rawValues.isValidAtWindowIndex(5));
    assertFalse(rawValues.isExtrapolatedAtWindowIndex(5));

    // now adding sample to 4 should make 3 and 5 valid with flaw of ADJACENT_AVG
    rawValues.addSample(m, 4, _metricDef);
    assertTrue(rawValues.isValidAtWindowIndex(4));
    assertFalse(rawValues.isExtrapolatedAtWindowIndex(4));
    assertTrue(rawValues.isValidAtWindowIndex(3));
    assertTrue(rawValues.isExtrapolatedAtWindowIndex(3));
    assertTrue(rawValues.isValidAtWindowIndex(5));
    assertTrue(rawValues.isExtrapolatedAtWindowIndex(5));
  }

  @Test (expected = IllegalArgumentException.class)
  public void testAddToWindowLargerThanCurrentWindow() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW, NUM_RAW_METRICS);
    rawValues.updateOldestWindowIndex(0);
    rawValues.addSample(getMetricSample(10, 10, 10), NUM_WINDOWS_TO_KEEP, _metricDef);
  }

  @Test
  public void testAggregateSingleWindow() {
    RawMetricValues rawValues = new RawMetricValues(2, MIN_SAMPLES_PER_WINDOW, NUM_RAW_METRICS);
    rawValues.updateOldestWindowIndex(0);
    MetricSample<String, IntegerEntity> m1 = getMetricSample(10, 10, 10);
    MetricSample<String, IntegerEntity> m2 = getMetricSample(6, 6, 6);
    MetricSample<String, IntegerEntity> m3 = getMetricSample(2, 12, 8);
    MetricSample<String, IntegerEntity> m4 = getMetricSample(18, 10, 2);

    // No sample
    ValuesAndExtrapolations valuesAndExtrapolations = aggregate(rawValues, new TreeSet<>(Collections.singleton(0L)));
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 0).get(0), EPSILON);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 1).get(0), EPSILON);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 2).get(0), EPSILON);
    assertEquals(1, valuesAndExtrapolations.extrapolations().size());
    Assert.assertEquals(Extrapolation.NO_VALID_EXTRAPOLATION, valuesAndExtrapolations.extrapolations().get(0));

    // Add the first sample
    addSample(rawValues, m1, 0);
    valuesAndExtrapolations = aggregate(rawValues, new TreeSet<>(Collections.singleton(0L)));
    assertEquals(10, valuesAndExtrapolations.metricValues().valuesFor((short) 0).get(0), EPSILON);
    assertEquals(10, valuesAndExtrapolations.metricValues().valuesFor((short) 1).get(0), EPSILON);
    assertEquals(10, valuesAndExtrapolations.metricValues().valuesFor((short) 2).get(0), EPSILON);
    assertEquals(1, valuesAndExtrapolations.extrapolations().size());
    Assert.assertEquals(Extrapolation.FORCED_INSUFFICIENT, valuesAndExtrapolations.extrapolations().get(0));

    // Add the second sample
    addSample(rawValues, m2, 0);
    valuesAndExtrapolations = aggregate(rawValues, new TreeSet<>(Collections.singleton(0L)));
    assertEquals(8, valuesAndExtrapolations.metricValues().valuesFor((short) 0).get(0), EPSILON);
    assertEquals(10, valuesAndExtrapolations.metricValues().valuesFor((short) 1).get(0), EPSILON);
    assertEquals(6, valuesAndExtrapolations.metricValues().valuesFor((short) 2).get(0), EPSILON);
    assertEquals(1, valuesAndExtrapolations.extrapolations().size());
    Assert.assertEquals(Extrapolation.AVG_AVAILABLE, valuesAndExtrapolations.extrapolations().get(0));

    // Add the third sample
    addSample(rawValues, m3, 0);
    valuesAndExtrapolations = aggregate(rawValues, new TreeSet<>(Collections.singleton(0L)));
    assertEquals(6, valuesAndExtrapolations.metricValues().valuesFor((short) 0).get(0), EPSILON);
    assertEquals(12, valuesAndExtrapolations.metricValues().valuesFor((short) 1).get(0), EPSILON);
    assertEquals(8, valuesAndExtrapolations.metricValues().valuesFor((short) 2).get(0), EPSILON);
    assertEquals(1, valuesAndExtrapolations.extrapolations().size());
    Assert.assertEquals(Extrapolation.AVG_AVAILABLE, valuesAndExtrapolations.extrapolations().get(0));

    // Add the fourth sample
    addSample(rawValues, m4, 0);
    valuesAndExtrapolations = aggregate(rawValues, new TreeSet<>(Collections.singleton(0L)));
    assertEquals(9, valuesAndExtrapolations.metricValues().valuesFor((short) 0).get(0), EPSILON);
    assertEquals(12, valuesAndExtrapolations.metricValues().valuesFor((short) 1).get(0), EPSILON);
    assertEquals(2, valuesAndExtrapolations.metricValues().valuesFor((short) 2).get(0), EPSILON);
    assertEquals(0, valuesAndExtrapolations.extrapolations().size());
  }

  @Test
  public void testAggregateMultipleWindows() {
    for (int i = 0; i < NUM_WINDOWS * 2; i++) {
      RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW, NUM_RAW_METRICS);
      rawValues.updateOldestWindowIndex(i);
      float[][] expected = populate(rawValues, i);
      ValuesAndExtrapolations valuesAndExtrapolations = aggregate(rawValues, allWindowIndices(i));
      assertAggregatedValues(valuesAndExtrapolations.metricValues(), expected, i);
    }
  }

  @Test
  public void testExtrapolationAdjacentAvgAtMiddle() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW, NUM_RAW_METRICS);
    prepareWindowMissingAtIndex(rawValues, 1);
    ValuesAndExtrapolations valuesAndExtrapolations = aggregate(rawValues, allWindowIndices(0));
    assertEquals(11.5, valuesAndExtrapolations.metricValues().valuesFor((short) 0).get(1), EPSILON);
    assertEquals(13.0, valuesAndExtrapolations.metricValues().valuesFor((short) 1).get(1), EPSILON);
    assertEquals(13.0, valuesAndExtrapolations.metricValues().valuesFor((short) 2).get(1), EPSILON);
    assertEquals(1, valuesAndExtrapolations.extrapolations().size());
    Assert.assertEquals(Extrapolation.AVG_ADJACENT, valuesAndExtrapolations.extrapolations().get(1));
  }

  @Test
  public void testExtrapolationAdjacentAvgAtLeftEdge() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW, NUM_RAW_METRICS);
    prepareWindowMissingAtIndex(rawValues, 0);
    ValuesAndExtrapolations valuesAndExtrapolations = aggregate(rawValues, allWindowIndices(0));
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 0).get(0), EPSILON);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 1).get(0), EPSILON);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 2).get(0), EPSILON);
    assertEquals(1, valuesAndExtrapolations.extrapolations().size());
    Assert.assertEquals(Extrapolation.NO_VALID_EXTRAPOLATION, valuesAndExtrapolations.extrapolations().get(0));
  }

  @Test
  public void testExtrapolationAdjacentAvgAtRightEdge() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW, NUM_RAW_METRICS);
    prepareWindowMissingAtIndex(rawValues, NUM_WINDOWS);
    rawValues.updateOldestWindowIndex(1);
    ValuesAndExtrapolations valuesAndExtrapolations = aggregate(rawValues, allWindowIndices(1));
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 0).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 1).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 2).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(1, valuesAndExtrapolations.extrapolations().size());
    Assert.assertEquals(Extrapolation.NO_VALID_EXTRAPOLATION, valuesAndExtrapolations.extrapolations().get(NUM_WINDOWS - 1));
  }

  @Test
  public void testExtrapolationAdjacentAvgAtLeftEdgeWithWrapAround() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW, NUM_RAW_METRICS);
    prepareWindowMissingAtIndex(rawValues, 0);

    // When oldest window index is 0, position 0 is the first index and should have no extrapolation.
    ValuesAndExtrapolations valuesAndExtrapolations = rawValues.aggregate(allWindowIndices(0), _metricDef);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 0).get(0), EPSILON);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 1).get(0), EPSILON);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 2).get(0), EPSILON);
    assertEquals(1, valuesAndExtrapolations.extrapolations().size());
    Assert.assertEquals(Extrapolation.NO_VALID_EXTRAPOLATION, valuesAndExtrapolations.extrapolations().get(0));

    // When oldest window index is 2, position 0 is the last index and should have no extrapolation.
    rawValues.updateOldestWindowIndex(2);
    valuesAndExtrapolations = rawValues.aggregate(allWindowIndices(2), _metricDef);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 0).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 1).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 2).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(1, valuesAndExtrapolations.extrapolations().size());
    Assert.assertEquals(Extrapolation.NO_VALID_EXTRAPOLATION, valuesAndExtrapolations.extrapolations().get(NUM_WINDOWS - 1));

    // when the oldest window index is 3, position 0 is the 3rd index. There should be an extrapolation.
    rawValues.updateOldestWindowIndex(3);
    valuesAndExtrapolations = rawValues.aggregate(allWindowIndices(3), _metricDef);
    assertEquals(31.5, valuesAndExtrapolations.metricValues().valuesFor((short) 0).get(3), EPSILON);
    assertEquals(33.0, valuesAndExtrapolations.metricValues().valuesFor((short) 1).get(3), EPSILON);
    assertEquals(33.0, valuesAndExtrapolations.metricValues().valuesFor((short) 2).get(3), EPSILON);
    assertEquals(1, valuesAndExtrapolations.extrapolations().size());
    Assert.assertEquals(Extrapolation.AVG_ADJACENT, valuesAndExtrapolations.extrapolations().get(3));
  }

  @Test
  public void testExtrapolationAdjacentAvgAtRightEdgeWithWrapAround() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW, NUM_RAW_METRICS);
    prepareWindowMissingAtIndex(rawValues, NUM_WINDOWS);
    // When oldest window index is 1, position NUM_WINDOWS is the last index and should have no extrapolation.
    rawValues.updateOldestWindowIndex(1);
    ValuesAndExtrapolations valuesAndExtrapolations = rawValues.aggregate(allWindowIndices(1), _metricDef);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 0).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 1).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 2).get(NUM_WINDOWS - 1), EPSILON);
    assertEquals(1, valuesAndExtrapolations.extrapolations().size());
    Assert.assertEquals(Extrapolation.NO_VALID_EXTRAPOLATION, valuesAndExtrapolations.extrapolations().get(NUM_WINDOWS - 1));

    // When oldest window index is NUM_WINDOWS, position NUM_WINDOWS is the first index, it should have no extrapolation.
    rawValues.updateOldestWindowIndex(NUM_WINDOWS);
    valuesAndExtrapolations = rawValues.aggregate(allWindowIndices(NUM_WINDOWS), _metricDef);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 0).get(0), EPSILON);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 1).get(0), EPSILON);
    assertEquals(0, valuesAndExtrapolations.metricValues().valuesFor((short) 2).get(0), EPSILON);
    assertEquals(1, valuesAndExtrapolations.extrapolations().size());
    Assert.assertEquals(Extrapolation.NO_VALID_EXTRAPOLATION, valuesAndExtrapolations.extrapolations().get(0));

    // when the oldest window index is 3, position NUM_WINDOWS is the 3rd index. There should be an extrapolation.
    rawValues.updateOldestWindowIndex(3);
    valuesAndExtrapolations = rawValues.aggregate(allWindowIndices(3), _metricDef);
    assertEquals(21.5, valuesAndExtrapolations.metricValues().valuesFor((short) 0).get(2), EPSILON);
    assertEquals(23.0, valuesAndExtrapolations.metricValues().valuesFor((short) 1).get(2), EPSILON);
    assertEquals(23.0, valuesAndExtrapolations.metricValues().valuesFor((short) 2).get(2), EPSILON);
    assertEquals(1, valuesAndExtrapolations.extrapolations().size());
    Assert.assertEquals(Extrapolation.AVG_ADJACENT, valuesAndExtrapolations.extrapolations().get(2));
  }

  @Test
  public void testAdjacentAvgAtEdgeWhenNewWindowRollsOut() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW, NUM_RAW_METRICS);
    prepareWindowMissingAtIndex(rawValues, NUM_WINDOWS - 1);

    assertFalse(rawValues.isValidAtWindowIndex(NUM_WINDOWS - 1));
    assertFalse(rawValues.isExtrapolatedAtWindowIndex(NUM_WINDOWS - 1));

    rawValues.updateOldestWindowIndex(1);

    assertTrue(rawValues.isValidAtWindowIndex(NUM_WINDOWS - 1));
    assertTrue(rawValues.isExtrapolatedAtWindowIndex(NUM_WINDOWS - 1));
  }

  @Test
  public void testAdjacentAvgAtEdgeWhenNewWindowRollsOutWithLargeLeap() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW, NUM_RAW_METRICS);
    prepareWindowMissingAtIndex(rawValues, NUM_WINDOWS - 1);

    assertFalse(rawValues.isValidAtWindowIndex(NUM_WINDOWS - 1));
    assertFalse(rawValues.isExtrapolatedAtWindowIndex(NUM_WINDOWS - 1));

    rawValues.updateOldestWindowIndex(NUM_WINDOWS - 1);

    assertFalse(rawValues.isValidAtWindowIndex(NUM_WINDOWS - 1));
    assertFalse(rawValues.isExtrapolatedAtWindowIndex(NUM_WINDOWS - 1));
  }

  @Test
  public void testIsValid() {
    RawMetricValues rawValues = new RawMetricValues(NUM_WINDOWS_TO_KEEP, MIN_SAMPLES_PER_WINDOW, NUM_RAW_METRICS);
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
    for (short metricId = 0; metricId < _metricDef.all().size(); metricId++) {
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

  private void prepareWindowMissingAtIndex(RawMetricValues rawValues, Collection<Integer> windowIndices, int oldestWindowIndex) {
    rawValues.updateOldestWindowIndex(oldestWindowIndex);
    populate(rawValues, oldestWindowIndex);
    // reset index, we need to play the trick to change the oldest window index temporarily to make it work.
    rawValues.updateOldestWindowIndex(oldestWindowIndex + 100);
    windowIndices.forEach(startingWindowIndex -> rawValues.sanityCheckWindowRangeReset(startingWindowIndex, 1));
    windowIndices.forEach(startingWindowIndex -> rawValues.resetWindowIndices(startingWindowIndex, 1));
    rawValues.updateOldestWindowIndex(oldestWindowIndex);
  }

  private float[][] populate(RawMetricValues rawValues, long startingWindowIndex) {
    float[][] expected = new float[_metricDef.size()][NUM_WINDOWS_TO_KEEP];
    for (long windowIndex = startingWindowIndex; windowIndex < startingWindowIndex + NUM_WINDOWS_TO_KEEP; windowIndex++) {
      for (int i = 0; i < MIN_SAMPLES_PER_WINDOW; i++) {
        float v = 10 * windowIndex + i;
        MetricSample<String, IntegerEntity> m = getMetricSample(v, v, v);
        rawValues.addSample(m, windowIndex, _metricDef);

        for (MetricInfo info : _metricDef.all()) {
          switch (info.aggregationFunction()) {
            case AVG:
              expected[info.id()][(int) (windowIndex % NUM_WINDOWS_TO_KEEP)] += v / MIN_SAMPLES_PER_WINDOW;
              break;
            case MAX:
            case LATEST:
              expected[info.id()][(int) (windowIndex % NUM_WINDOWS_TO_KEEP)] = v;
              break;
            default:
              throw new IllegalStateException("Should never be here");
          }
        }
      }
    }
    return expected;
  }

  private SortedSet<Long> allWindowIndices(int oldestWindowIndex) {
    SortedSet<Long> windowIndices = new TreeSet<>();
    for (long i = 0; i < NUM_WINDOWS; i++) {
      windowIndices.add(oldestWindowIndex + i);
    }
    return windowIndices;
  }

  private void addSample(RawMetricValues rawValues, MetricSample<String, IntegerEntity> metricSample, int windowIndex) {
    rawValues.addSample(metricSample, windowIndex, _metricDef);
  }

  private ValuesAndExtrapolations aggregate(RawMetricValues rawValues, SortedSet<Long> windowIndices) {
    return rawValues.aggregate(windowIndices, _metricDef);
  }

  /**
   * Test code should ensure that the {@link #NUM_RAW_METRICS} size matches the metrics generated here
   * @param v1 First value to be recorded.
   * @param v2 Second value to be recorded.
   * @param v3 Third value to be recorded.
   * @return A metric sample with the given metric values.
   */
  private MetricSample<String, IntegerEntity> getMetricSample(float v1, float v2, float v3) {
    MetricSample<String, IntegerEntity> metricSample = new MetricSample<>(new IntegerEntity("group", 0));
    metricSample.record(_metricDef.metricInfo("metric1"), v1);
    metricSample.record(_metricDef.metricInfo("metric2"), v2);
    metricSample.record(_metricDef.metricInfo("metric3"), v3);
    metricSample.close(0);
    return metricSample;
  }
}
