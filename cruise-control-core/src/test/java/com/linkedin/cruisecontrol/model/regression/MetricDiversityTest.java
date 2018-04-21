/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.regression;

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;

import static org.junit.Assert.*;


public class MetricDiversityTest {

  @Test
  public void testShouldRemove() {
    MetricDiversity diversity = new MetricDiversity(5, 10);
    diversity.addValue(6.0f);
    diversity.addValue(1.0f);
    assertShouldNotRemove(Arrays.asList(1.0f, 6.0f), diversity);

    for (int i = 1; i < 6; i++) {
      for (int j = 0; j <= 10; j++) {
        diversity.addValue((float) i * 1);
      }
    }
    assertShouldRemove(Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f), diversity);
  }

  private void assertShouldRemove(Collection<Float> values, MetricDiversity diversity) {
    values.forEach(v -> assertTrue(v + " should be needed.", diversity.shouldRemove(v)));
  }

  private void assertShouldNotRemove(Collection<Float> values, MetricDiversity diversity) {
    values.forEach(v -> assertFalse(v + " should not be needed.", diversity.shouldRemove(v)));
  }
}
