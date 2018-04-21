/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information. 
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class MetricValuesTest {
  private static final double DELTA = 1E-6;
  
  @Test
  public void testSet() {     
    MetricValues metricValues = getMetricValues();
    metricValues.set(2, 4.0);
    assertValues(metricValues, 1.0, 3.0, 4.0, (double) 8 / 3, 4.0, 1.0);
  }
  
  @Test 
  public void testSetLowerAtMaxIndex() {
    MetricValues metricValues = getMetricValues();
    metricValues.set(1, 1.0);
    assertValues(metricValues, 1.0, 1.0, 2.0, (double) 4 / 3, 2.0, 1.0);
  }

  @Test
  public void testSetHigherAtMaxIndex() {
    MetricValues metricValues = getMetricValues();
    metricValues.set(1, 4.0);
    assertValues(metricValues, 1.0, 4.0, 2.0, (double) 7 / 3, 4.0, 1.0);
  }
  
  @Test
  public void testAddAtIndex() {
    MetricValues metricValues = getMetricValues();
    metricValues.add(0, 1);
    assertValues(metricValues, 2.0, 3.0, 2.0, (double) 7 / 3, 3.0, 2.0);
  }
  
  @Test
  public void testAddPositiveAtMaxIndex() {
    MetricValues metricValues = getMetricValues();
    metricValues.add(1, 1);
    assertValues(metricValues, 1.0, 4.0, 2.0, (double) 7 / 3, 4.0, 1.0);
  }
  
  @Test
  public void testAddNegativeAtMaxIndex() {
    MetricValues metricValues = getMetricValues();
    metricValues.add(1, -1);
    assertValues(metricValues, 1.0, 2.0, 2.0, (double) 5 / 3, 2.0, 1.0);
  }
  
  @Test
  public void testAddArray() {
    MetricValues metricValues = getMetricValues();
    metricValues.add(new double[]{1.0, 2.0, 4.0});
    assertValues(metricValues, 2.0, 5.0, 6.0, (double) 13 / 3, 6.0, 2.0);
  }
  
  @Test
  public void testSubtractArray() {
    MetricValues metricValues = getMetricValues();
    metricValues.subtract(new double[]{1.0, 4.0, 1.0});
    assertValues(metricValues, 0.0, -1.0, 1.0, 0.0, 1.0, 0.0);
  }
  
  @Test
  public void testClear() {
    MetricValues metricValues = getMetricValues();
    metricValues.clear();
    assertValues(metricValues, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
  }
  
  private void assertValues(MetricValues values, double first, double second, double third, 
                            double avg, double max, double latest) {
    assertEquals(first, values.get(0), DELTA);
    assertEquals(second, values.get(1), DELTA);
    assertEquals(third, values.get(2), DELTA);
    assertEquals(avg, values.avg(), DELTA);
    assertEquals(max, values.max(), DELTA);
    assertEquals(latest, values.latest(), DELTA);
  }
  
  private MetricValues getMetricValues() {
    MetricValues metricValues = new MetricValues(3);
    metricValues.set(0, 1.0);
    metricValues.set(1, 3.0);
    metricValues.set(2, 2.0);
    assertValues(metricValues, 1.0, 3.0, 2.0, 2.0, 3.0, 1.0);
    return metricValues;
  }
  
}
