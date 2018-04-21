/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.  
 */

package com.linkedin.cruisecontrol.model.estimator.impl;

import com.linkedin.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.cruisecontrol.IntegerEntity;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.model.regression.LinearRegression;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import org.junit.Test;

import static com.linkedin.cruisecontrol.CruiseControlUnitTestUtils.METRIC1;
import static com.linkedin.cruisecontrol.CruiseControlUnitTestUtils.METRIC2;
import static com.linkedin.cruisecontrol.CruiseControlUnitTestUtils.METRIC3;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class LinearRegressionEstimatorTest {
  private static final IntegerEntity ENTITY = new IntegerEntity("group", 0); 
  private static final String CAUSAL_RELATION_FILE = "TestCausalRelation.json";
  private static final String LINEAR_REGRESSION_NAME_1 = "regression_1";
  private static final String LINEAR_REGRESSION_NAME_2 = "regression_2";
  
  @Test
  public void testLoadLinearRegressions() {
    LinearRegressionEstimator linearRegressionEstimator = getLinearRegressionEstimator();

    MetricDef metricDef = CruiseControlUnitTestUtils.getMetricDef();
    Map<String, LinearRegression> linearRegressions = linearRegressionEstimator.linearRegressions();
    assertEquals(2, linearRegressions.size());
    Map<MetricInfo, SortedSet<LinearRegression>> linearRegressionsByResultantMetrics =
        linearRegressionEstimator.linearRegressionByResultantMetrics();
    assertEquals(2, linearRegressionsByResultantMetrics.size());
    SortedSet<LinearRegression> regressionsForMetric3 = linearRegressionsByResultantMetrics.get(metricDef.metricInfo(METRIC3));
    assertEquals(2, regressionsForMetric3.size());
    assertTrue(regressionsForMetric3.contains(linearRegressions.get(LINEAR_REGRESSION_NAME_1)));
    assertTrue(regressionsForMetric3.contains(linearRegressions.get(LINEAR_REGRESSION_NAME_2)));
    SortedSet<LinearRegression> regressionsForMetric2 = linearRegressionsByResultantMetrics.get(metricDef.metricInfo(METRIC2));
    assertEquals(1, regressionsForMetric2.size());
    assertTrue(regressionsForMetric2.contains(linearRegressions.get(LINEAR_REGRESSION_NAME_2)));
    
    Set<MetricInfo> causalMetrics = linearRegressions.get(LINEAR_REGRESSION_NAME_1).causalRelation().causalMetrics();
    Set<MetricInfo> resultantMetrics = linearRegressions.get(LINEAR_REGRESSION_NAME_1).causalRelation().resultantMetrics();
    assertEquals(2, causalMetrics.size());
    assertTrue(causalMetrics.containsAll(Arrays.asList(metricDef.metricInfo(METRIC1), metricDef.metricInfo(METRIC2))));
    assertEquals(1, resultantMetrics.size());
    assertTrue(resultantMetrics.contains(metricDef.metricInfo(METRIC3)));

    causalMetrics = linearRegressions.get(LINEAR_REGRESSION_NAME_2).causalRelation().causalMetrics();
    resultantMetrics = linearRegressions.get(LINEAR_REGRESSION_NAME_2).causalRelation().resultantMetrics();
    assertEquals(1, causalMetrics.size());
    assertTrue(causalMetrics.contains(metricDef.metricInfo(METRIC1)));
    assertEquals(2, resultantMetrics.size());
    assertTrue(resultantMetrics.containsAll(Arrays.asList(metricDef.metricInfo(METRIC2), metricDef.metricInfo(METRIC3))));
  }
  
  @Test
  public void testBestRegression() {
    LinearRegressionEstimator linearRegressionEstimator = getLinearRegressionEstimator();
    // Iterate over each linear regression definition and provide metric samples that fits into their model.
    // Then verify that that linear regression was picked to be the best linear regression.
    for (LinearRegression linearRegression : linearRegressionEstimator.linearRegressions().values()) {
      linearRegressionEstimator.reset();
      List<MetricSample<String, IntegerEntity>> samples =
          generateMetricSamplesForRegression(linearRegression.causalRelation().causalMetrics(),
                                             linearRegression.causalRelation().resultantMetrics(),
                                             linearRegression.maxExponent(),
                                             123456789L);
      linearRegressionEstimator.addTrainingSamples(samples);
      linearRegressionEstimator.train();
      // Add another set of samples to trigger estimation.
      samples = generateMetricSamplesForRegression(linearRegression.causalRelation().causalMetrics(),
                                                   linearRegression.causalRelation().resultantMetrics(),
                                                   linearRegression.maxExponent(),
                                                   987654321L);
      linearRegressionEstimator.addTrainingSamples(samples);
      for (MetricInfo resultantMetric : linearRegression.causalRelation().resultantMetrics()) {
        assertEquals(linearRegression.name(), linearRegressionEstimator.bestLinearRegression(resultantMetric).name());
      }
    }
  }

  /**
   * A private method generates the data that best fits causal relation definition and linear regression max exponent.
   *
   * @return the metric samples that conform to the above polynomial function.
   */
  private List<MetricSample<String, IntegerEntity>> generateMetricSamplesForRegression(Set<MetricInfo> causalMetrics,
                                                                                       Set<MetricInfo> resultantMetrics,
                                                                                       int maxExponent,
                                                                                       long randomSeed) {
    List<MetricSample<String, IntegerEntity>> samples = new ArrayList<>();
    Random random = new Random(randomSeed);
    for (int i = 0; i < 10; i++) {
      MetricSample<String, IntegerEntity> sample = new MetricSample<>(ENTITY);
      for (MetricInfo causalMetric : causalMetrics) {
        sample.record(causalMetric, random.nextInt(10));
      }
      for (MetricInfo resultantMetric : resultantMetrics) {
        double value = 0.0;
        for (MetricInfo causalMetric : causalMetrics) {
          // The coefficient of each causal metric for a given resultant metric is the sum of their metric id.
          // The exponential value is applied to the coefficients as well.
          int coefficient = resultantMetric.id() + causalMetric.id();
          double valueAddition = 0.0;
          for (int exp = 1; exp <= maxExponent; exp++) {
            valueAddition += Math.pow(coefficient, exp) * Math.pow(sample.metricValue(causalMetric.id()), exp);
          }
          value += valueAddition;
        }
        sample.record(resultantMetric, value);
      }
      sample.close(i);
      samples.add(sample);
    }
    return samples;
  }
  
  private LinearRegressionEstimator getLinearRegressionEstimator() {
    LinearRegressionEstimator linearRegressionEstimator = new TestLinearRegressionEstimator();
    String causalRelationFileName = this.getClass().getClassLoader().getResource(CAUSAL_RELATION_FILE).getFile();
    Map<String, String> config =
        Collections.singletonMap(LinearRegressionEstimator.CAUSAL_RELATION_DEFINITION_FILE_CONFIG,
                                 causalRelationFileName);
    linearRegressionEstimator.configure(config);
    return linearRegressionEstimator;
  }
  
  private static class TestLinearRegressionEstimator extends LinearRegressionEstimator {
    @Override
    protected MetricDef metricDef() {
      return CruiseControlUnitTestUtils.getMetricDef();
    }
  }
}
