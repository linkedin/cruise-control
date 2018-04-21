/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.  
 */

package com.linkedin.cruisecontrol.model.regression;

import com.linkedin.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.cruisecontrol.IntegerEntity;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.linkedin.cruisecontrol.CruiseControlUnitTestUtils.METRIC1;
import static com.linkedin.cruisecontrol.CruiseControlUnitTestUtils.METRIC2;
import static com.linkedin.cruisecontrol.CruiseControlUnitTestUtils.METRIC3;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class LinearRegressionTest {
  private static final double DELTA = 1E-6;
  private static final IntegerEntity ENTITY = new IntegerEntity("group", 0);
  private static final double COEFFICIENT_1 = 2.0;
  private static final double COEFFICIENT_2 = 3.0;
  
  private int _maxExponent;
  
  @Parameterized.Parameters(name = "maxExponent = {0}")
  public static Collection<Object[]> exponents() {
    List<Object[]> exponents = new ArrayList<>();
    exponents.add(new Integer[]{1});
    exponents.add(new Integer[]{2});
    exponents.add(new Integer[]{3});
    return exponents;
  }
  
  
  public LinearRegressionTest(int maxExponent) {
    _maxExponent = maxExponent;
  }
  
  @Test
  public void testLinearRegression() {
    LinearRegression linearRegression = getLinearRegression();
    linearRegression.addMetricSamples(generateMetricSamples(_maxExponent));
    linearRegression.updateModelCoefficient();
    
    MetricDef metricDef = CruiseControlUnitTestUtils.getMetricDef();
    Map<Integer, Double> coefficients1 = 
        linearRegression.getCoefficients(metricDef.metricInfo(METRIC2), metricDef.metricInfo(METRIC1));
    Map<Integer, Double> coefficients2 =
        linearRegression.getCoefficients(metricDef.metricInfo(METRIC3), metricDef.metricInfo(METRIC1));
    for (int exp = 1; exp <= _maxExponent; exp++) {
      assertEquals(COEFFICIENT_1, coefficients1.get(exp), DELTA);
      assertEquals(COEFFICIENT_2, coefficients2.get(exp), DELTA);
    }
  }
  
  private LinearRegression getLinearRegression() {
    MetricDef metricDef = CruiseControlUnitTestUtils.getMetricDef();
    return new LinearRegression("test", 
                                new HashSet<>(Arrays.asList(metricDef.metricInfo(METRIC2),
                                                            metricDef.metricInfo(METRIC3))),
                                Collections.singleton(metricDef.metricInfo(METRIC1)),
                                5,
                                0,
                                3,
                                _maxExponent,
                                10);
  }

  /**
   * A private method generates the data with the given max exponent. The polynomial function for the causal relation
   * are:
   * METRIC1(i) = i, where i = 1, 2, ... _maxExponent
   * METRIC2(i) = Sum(COEFFICIENT_1 * METRIC1^i), where i = 1, 2, ... _maxExponent
   * METRIC3(i) = Sum(COEFFICIENT_2 * METRIC1^i), where i = 1, 2, ... _maxExponent
   * 
   * @param maxExponent the max exponent for the linear regression.
   * @return the metric samples that conform to the above polynomial function.
   */
  private List<MetricSample<String, IntegerEntity>> generateMetricSamples(int maxExponent) {
    MetricDef metricDef = CruiseControlUnitTestUtils.getMetricDef(); 
    List<MetricSample<String, IntegerEntity>> samples = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      MetricSample<String, IntegerEntity> sample = new MetricSample<>(ENTITY);
      double value1 = i;
      double value2 = 0;
      double value3 = 0;
      for (int exp = 1; exp <= maxExponent; exp++) {
        value2 = (value2 + COEFFICIENT_1) * value1;
        value3 = (value3 + COEFFICIENT_2) * value1;
      }
      sample.record(metricDef.metricInfo(METRIC1), value1);
      sample.record(metricDef.metricInfo(METRIC2), value2);
      sample.record(metricDef.metricInfo(METRIC3), value3);
      sample.close(i);
      samples.add(sample);
    }
    return samples;
  }
}
