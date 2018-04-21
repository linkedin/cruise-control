/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.regression;

import com.linkedin.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.cruisecontrol.CruiseControlUnitTestUtils.METRIC1;
import static com.linkedin.cruisecontrol.CruiseControlUnitTestUtils.METRIC2;
import static com.linkedin.cruisecontrol.CruiseControlUnitTestUtils.METRIC3;
import static org.junit.Assert.*;


public class RegressionMetricsAccumulatorTest {
  private static final int MAX_EXPONENT = 2;
  private static final double DELTA = 1E-6;
  private CausalRelation _causalRelation;
  private MetricDef _metricDef;
  
  @Before
  public void prepare() {
    _metricDef = CruiseControlUnitTestUtils.getMetricDef();
    _causalRelation = new CausalRelation(Collections.singleton(_metricDef.metricInfo(METRIC1)), 
                                         Arrays.asList(_metricDef.metricInfo(METRIC2), _metricDef.metricInfo(METRIC3)));
  }
  
  @Test
  public void testRecordMetricValues() {
    List<double[]> data = prepareData(10);
    RegressionMetricsAccumulator accumulator = getMetricsAccumulator();
    for (double[] values : data) {
      accumulator.recordMetricValues(values);
    }
    accumulator.maybeRemoveOldValues();
    // Should not have removed anything.
    Map<MetricInfo, MetricDiversity> diversityForMetrics = accumulator.diversityForMetrics();
    assertEquals(_causalRelation.indexes().size(), diversityForMetrics.size());
    for (MetricDiversity diversity : diversityForMetrics.values()) {
      Map<Integer, Integer> counts = diversity.countsByBucket();
      for (int i = 0; i < 5; i++) {
        assertEquals("There should be 2 values in bucket " + i, 2, counts.get(i).intValue());
      }
    }
  }

  @Test
  public void testMaybeRemoveOldValues() {
    List<double[]> data = prepareData(100);
    RegressionMetricsAccumulator accumulator = getMetricsAccumulator();
    for (double[] values : data) {
      accumulator.recordMetricValues(values);
    }
    accumulator.maybeRemoveOldValues();
    // Should have removed additional value samples.
    Map<MetricInfo, MetricDiversity> diversityForMetrics = accumulator.diversityForMetrics();
    assertEquals(_causalRelation.indexes().size(), diversityForMetrics.size());
    for (MetricDiversity diversity : diversityForMetrics.values()) {
      Map<Integer, Integer> counts = diversity.countsByBucket();
      for (int i = 0; i < 5; i++) {
        assertEquals("There should be 2 values in bucket " + i, 5, counts.get(i).intValue());
      }
    }
    // Check the samples
    Map<Long, double[]> sampleValues = accumulator.sampleValues();
    assertEquals(5 * 5, sampleValues.size());
  }

  @Test
  public void testGetRegressionData() {
    List<double[]> data = prepareData(10);
    RegressionMetricsAccumulator accumulator = getMetricsAccumulator();
    for (double[] values : data) {
      accumulator.recordMetricValues(values);
    }
    accumulator.maybeRemoveOldValues();
    Set<MetricInfo> resultantMetrics = _causalRelation.resultantMetrics();
    DataForLinearRegression regressionData = accumulator.getRegressionData(resultantMetrics);

    double[] dataForResultantMetric = regressionData.resultantMetricValues().get(_metricDef.metricInfo(METRIC1));
    assertEquals(10, dataForResultantMetric.length);
    int resultantMetricIndex = _causalRelation.indexes().get(_metricDef.metricInfo(METRIC1));
    for (int i = 0; i < 10; i++) {
      // Because the metrics are prioritized in reverse order, the later samples will show first.
      assertEquals((9 - i) + (double) resultantMetricIndex / 10, dataForResultantMetric[i], DELTA);
    }

    double[][] causalMetricData = regressionData.causalMetricsValues();
    assertEquals(10, causalMetricData.length);
    for (int i = 0; i < 10; i++) {
      for (MetricInfo metricInfo : _causalRelation.causalMetrics()) {
        int causalMetricIndex = _causalRelation.indexes().get(metricInfo);
        for (int exp = 1; exp <= MAX_EXPONENT; exp++) {
          // Because the metrics are prioritized in reverse order, the later samples will show first.
          assertEquals(Math.pow((9 - i) + (double) causalMetricIndex / 10, exp), 
                       causalMetricData[i][causalMetricIndex + (exp - 1) * _causalRelation.causalMetrics().size()], 
                       DELTA);
        }
      }
    }
  }
  
  private RegressionMetricsAccumulator getMetricsAccumulator() {
    return new RegressionMetricsAccumulator(_causalRelation, 5, 3, 5, 2);
  }

  private List<double[]> prepareData(int numSamplesPerMetric) {
    List<double[]> data = new ArrayList<>();
    for (int i = 0; i < numSamplesPerMetric; i++) {
      double[] values = new double[_causalRelation.indexes().size()];
      for (int index : _causalRelation.indexes().values()) {
        values[index] = i + (double) index / 10;
      }
      data.add(values);
    }
    return data;
  }
  
  
}
