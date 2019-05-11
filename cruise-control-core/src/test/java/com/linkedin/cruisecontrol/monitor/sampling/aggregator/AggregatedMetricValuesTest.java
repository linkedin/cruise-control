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
    Map<Short, MetricValues> valuesByMetricId = getValuesByMetricId();

    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(valuesByMetricId);
    aggregatedMetricValues.add(aggregatedMetricValues);

    for (Map.Entry<Short, MetricValues> entry : valuesByMetricId.entrySet()) {
      MetricValues values = entry.getValue();
      for (int j = 0; j < 10; j++) {
        assertEquals(2 * j, values.get(j), 0.01);
      }
    }
  }

  @Test
  public void testDeduct() {
    Map<Short, MetricValues> valuesByMetricId = getValuesByMetricId();

    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(valuesByMetricId);
    aggregatedMetricValues.subtract(aggregatedMetricValues);

    for (Map.Entry<Short, MetricValues> entry : valuesByMetricId.entrySet()) {
      MetricValues values = entry.getValue();
      for (int j = 0; j < 10; j++) {
        assertEquals(0, values.get(j), 0.01);
      }
    }
  }

  @Test
  public void testAddValuesToEmptyAggregatedMetricValues() {
    Map<Short, MetricValues> valuesByMetricId = getValuesByMetricId();

    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(new HashMap<>());
    AggregatedMetricValues toBeAdded = new AggregatedMetricValues(valuesByMetricId);

    aggregatedMetricValues.add(toBeAdded);
    for (short i = 0; i < 2; i++) {
      MetricValues values = aggregatedMetricValues.valuesFor(i);
      for (int j = 0; j < 10; j++) {
        assertEquals(j, values.get(j), 0.01);
      }
    }
  }

  private Map<Short, MetricValues> getValuesByMetricId() {
    Map<Short, MetricValues> valuesMap = new TreeMap<>();

    for (short i = 0; i < 2; i++) {
      MetricValues value = new MetricValues(10);
      for (int j = 0; j < 10; j++) {
        value.set(j, j);
      }
      valuesMap.put(i, value);
    }
    return valuesMap;
  }

}
