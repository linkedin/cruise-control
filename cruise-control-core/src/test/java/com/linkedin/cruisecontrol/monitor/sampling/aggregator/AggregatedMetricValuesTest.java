/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class AggregatedMetricValuesTest {

  @Test
  public void testAdd() {
    Map<Integer, MetricValues> valuesMap = getValuesMap();

    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(valuesMap);
    aggregatedMetricValues.add(aggregatedMetricValues);

    for (Map.Entry<Integer, MetricValues> entry : valuesMap.entrySet()) {
      MetricValues values = entry.getValue();
      for (int j = 0; j < 10; j++) {
        assertEquals(2 * j, values.get(j), 0.01);
      }
    }
  }

  @Test
  public void testDeduct() {
    Map<Integer, MetricValues> valuesMap = getValuesMap();

    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(valuesMap);
    aggregatedMetricValues.subtract(aggregatedMetricValues);

    for (Map.Entry<Integer, MetricValues> entry : valuesMap.entrySet()) {
      MetricValues values = entry.getValue();
      for (int j = 0; j < 10; j++) {
        assertEquals(0, values.get(j), 0.01);
      }
    }
  }
  
  @Test
  public void testAddValuesToEmptyAggregatedMetricValues() {
    Map<Integer, MetricValues> valuesMap = getValuesMap();
    
    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(new HashMap<>());
    AggregatedMetricValues toBeAdded = new AggregatedMetricValues(valuesMap);
    
    aggregatedMetricValues.add(toBeAdded);
    for (int i = 0; i < 2; i++) {
      MetricValues values = aggregatedMetricValues.valuesFor(i);
      for (int j = 0; j < 10; j++) {
        assertEquals(j, values.get(j), 0.01);
      }
    }
  }

  private Map<Integer, MetricValues> getValuesMap() {
    Map<Integer, MetricValues> valuesMap = new TreeMap<>();

    for (int i = 0; i < 2; i++) {
      MetricValues value = new MetricValues(10);
      for (int j = 0; j < 10; j++) {
        value.set(j, j);
      }
      valuesMap.put(i, value);
    }
    return valuesMap;
  }

}
