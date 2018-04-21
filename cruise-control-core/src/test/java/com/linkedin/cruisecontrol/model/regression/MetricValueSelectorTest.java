/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.regression;

import org.junit.Test;

import static org.junit.Assert.*;


public class MetricValueSelectorTest {

  @Test
  public void testInclude() {
    MetricDiversity diversity = new MetricDiversity(20, 50);
    diversity.updateBucketInfo(0);
    diversity.updateBucketInfo(20);
    for (int i = 1; i < 21; i++) {
      for (int j = 0; j < i; j++) {
        diversity.addValue(j);
      }
    }

    for (int numBucketsToCap = 0; numBucketsToCap < 20; numBucketsToCap++) {
      MetricValueSelector valueSelector = new MetricValueSelector(numBucketsToCap, diversity);
      assertEquals(20 - numBucketsToCap, valueSelector.fairCountCap());
      for (int i = 1; i < 21; i++) {
        for (int j = 0; j < i; j++) {
          if (valueSelector.include(j)) {
            valueSelector.included(j);
          }
          if (j > numBucketsToCap || (i - j) < valueSelector.fairCountCap()) {
            assertTrue(String.format("numBucketToCap=%d, Value %d, %d should be included", numBucketsToCap, i, j), valueSelector.include(j));
          } else {
            assertFalse(String.format("numBucketToCap=%d, Value %d, %d should not be included", numBucketsToCap, i, j), valueSelector.include(j));
          }
        }
      }
    }
  }
}
