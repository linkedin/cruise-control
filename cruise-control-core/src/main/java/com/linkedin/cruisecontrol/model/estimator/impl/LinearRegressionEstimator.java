/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.estimator.impl;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.linkedin.cruisecontrol.CruiseControlUtils;
import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.model.estimator.MetricEstimator;
import com.linkedin.cruisecontrol.model.regression.LinearRegression;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;


/**
 * An abstract class that implements the {@link MetricEstimator} using {@link LinearRegression}.
 * <p>
 *   This class reads the causal relations from a config file in the following format:
 * </p>
 * <pre>
 *   {
 *     "linearRegressions": [
 *       {
 *         "name": "regression_1",
 *         "causalMetrics": ["CausalMetric_1", "CausalMetric_2", "CausalMetric_3"],
 *         "resultantMetrics": ["ResultantMetric_1", "ResultantMetric_2"],
 *         "numValueBuckets": 20,
 *         "numBucketsToCapForFairness": 10,
 *         "maxSamplesPerBucket": 50,
 *         "maxExponent": 2,
 *         "errorStatsWindowSize": 1000
 *       },
 *       {
 *         "name": "regression_2",
 *         "causalMetrics": ["CausalMetric_1", "CausalMetric_2"],
 *         "resultantMetrics": ["ResultantMetric_3", "ResultantMetric_4"],
 *         "numValueBuckets": 20,
 *         "numBucketsToCapForFairness": 10,
 *         "maxSamplesPerBucket": 50,
 *         "maxExponent": 2,
 *         "errorStatsWindowSize": 1000
 *       },
 *       ...
 *     ]
 *   }
 * </pre>
 * The each regression may contain multiple resultant metrics if they share the same causal metrics. These resultant
 * metrics are, however, independent to each other.
 *
 * This estimator assumes all the metric samples share the same linear regression model, i.e. this estimator assumes
 * homogeneous entities. The {@link #estimate(Entity, MetricInfo, AggregatedMetricValues)} call ignores the passed
 * in entities. Use {@link HeterogeneousLinearRegressionEstimator} for heterogeneous use cases.
 */
public abstract class LinearRegressionEstimator implements MetricEstimator {
  private static final Logger LOG = LoggerFactory.getLogger(LinearRegressionEstimator.class);
  public static final String CAUSAL_RELATION_DEFINITION_FILE_CONFIG = "causal.relation.definition.file";
  private final Map<String, LinearRegression> _linearRegressionMap;
  private final Map<MetricInfo, SortedSet<LinearRegression>> _linearRegressionsByResultantMetrics;

  public LinearRegressionEstimator() {
    _linearRegressionMap = new HashMap<>();
    _linearRegressionsByResultantMetrics = new HashMap<>();
  }

  /**
   * @return the metric definition to use for linear regression.
   */
  protected abstract MetricDef metricDef();

  @Override
  public void addTrainingSamples(Collection<? extends MetricSample> trainingSamples) {
    _linearRegressionMap.values().forEach(lr -> lr.addMetricSamples(trainingSamples));
    updateLinearRegressionRank();
  }

  @Override
  public float trainingProgress() {
    double progress = 0.0f;
    for (LinearRegression lr : _linearRegressionMap.values()) {
      progress = Math.max(progress, lr.modelCoefficientTrainingCompleteness());
    }
    return (float) progress / _linearRegressionMap.size();
  }

  @Override
  public String trainingProgressDescription() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, LinearRegression> entry : _linearRegressionMap.entrySet()) {
      String name = entry.getKey();
      LinearRegression lr = entry.getValue();
      sb.append(name).append(": \n").append(lr.modelState()).append("\n\n");
    }
    return sb.toString();
  }

  @Override
  public void train() {
    _linearRegressionMap.values().forEach(LinearRegression::updateModelCoefficient);
  }

  @Override
  public MetricValues estimate(Entity entity, MetricInfo resultantMetric, AggregatedMetricValues causalMetricValues) {
    LinearRegression bestRegression = bestLinearRegression(resultantMetric);
    return bestRegression.estimate(resultantMetric, causalMetricValues, null, null);
  }

  @Override
  public MetricValues estimate(Entity entity,
                               MetricInfo resultantMetric,
                               AggregatedMetricValues causalMetricValues,
                               AggregatedMetricValues causalMetricValueChanges,
                               ChangeType changeType) {
    LinearRegression bestRegression = bestLinearRegression(resultantMetric);
    return bestRegression.estimate(resultantMetric, causalMetricValues, causalMetricValueChanges, changeType);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    String linearRegressionDefinitionFile = (String) configs.get(CAUSAL_RELATION_DEFINITION_FILE_CONFIG);
    CruiseControlUtils.ensureValidString(CAUSAL_RELATION_DEFINITION_FILE_CONFIG, linearRegressionDefinitionFile);
    try {
      loadLinearRegressions(linearRegressionDefinitionFile);
    } catch (FileNotFoundException e) {
      throw new ConfigException("Linear regression definition file " + linearRegressionDefinitionFile
                                    + " is not found.", e);
    }
  }

  @Override
  public void reset() {
    _linearRegressionMap.values().forEach(LinearRegression::reset);
  }

  // Methods specific to the linear regression.
  /**
   * From the best linear regression, get the coefficients of the given causal metric for the given resultant metric 
   * with all the exponents.
   *
   * @param resultantMetric the resultant metric to get the coefficients for.
   * @param causalMetric the causal metric whose coefficients should be returned.
   * @return A mapping from exponents to the coefficient of the given causal metric and resultant metric combination.
   */
  public Map<Integer, Double> getCoefficients(MetricInfo resultantMetric, MetricInfo causalMetric) {
    return bestLinearRegression(resultantMetric).getCoefficients(resultantMetric, causalMetric);
  }

  /**
   * Estimate the average value of the given resultant metric using the average values of the given causal metrics.
   *
   * @param resultantMetric the resultant metric whose value is to be estimated.
   * @param causalMetricValues the causal metrics values.
   * @return the estimated average value of the given resultant metric.
   */
  public double estimateAvg(MetricInfo resultantMetric, AggregatedMetricValues causalMetricValues) {
    return bestLinearRegression(resultantMetric).estimateAvg(resultantMetric, causalMetricValues);
  }

  /**
   * Get the estimation error statistics of the best linear regression for the given resultant metric.
   * 
   * @param resultantMetric the resultant metric to get the estimation error statistics.
   * @return the estimation error statistics of this linear regression for the given resultant metric
   */
  public DescriptiveStatistics estimationErrorStats(MetricInfo resultantMetric) {
    return bestLinearRegression(resultantMetric).estimationErrorStats(resultantMetric);
  }

  /**
   * Get the state of this best linear regression for the given resultant metric.
   * 
   * @return the state of this linear regression.
   */
  public LinearRegression.LinearRegressionState modelState(MetricInfo resultantMetric) {
    return bestLinearRegression(resultantMetric).modelState();
  }
  
  // Package private methods only for unit test.
  Map<String, LinearRegression> linearRegressions() {
    return _linearRegressionMap;
  }
  
  Map<MetricInfo, SortedSet<LinearRegression>> linearRegressionByResultantMetrics() {
    return _linearRegressionsByResultantMetrics;
  }
  
  // private helper methods.
  /**
   * Get the best linear regression for the resultant metric. 
   * The best linear regression is defined using {@link LinearRegressionComparator}.
   * Package private for unit test.
   * 
   * @param resultantMetric the resultant metric to get the the best linear regression. 
   * @return the best linear regression for the given resultant metric.
   */
  LinearRegression bestLinearRegression(MetricInfo resultantMetric) {
    LinearRegression bestRegression = _linearRegressionsByResultantMetrics.get(resultantMetric).first();
    if (bestRegression == null || !bestRegression.trainingCompleted(resultantMetric)) {
      throw new IllegalStateException("The linear regression for resultant metric " + resultantMetric
                                          + " is not ready.");
    }
    return bestRegression;
  }
  
  private void loadLinearRegressions(String linearRegressionDefinitionFile) throws FileNotFoundException {
    JsonReader reader = null;
    try {
      reader = new JsonReader(new InputStreamReader(new FileInputStream(linearRegressionDefinitionFile), StandardCharsets.UTF_8));
      Gson gson = new Gson();
      List<LinearRegressionDef> linearRegressionDefList =
          ((LinearRegressionList) gson.fromJson(reader, LinearRegressionList.class)).linearRegressions;
      for (LinearRegressionDef linearRegressionDef : linearRegressionDefList) {
        Set<MetricInfo> resultantMetrics = toMetricInfoSet(linearRegressionDef.resultantMetrics);
        LinearRegression lr = new LinearRegression(linearRegressionDef.name,
                                                   resultantMetrics,
                                                   toMetricInfoSet(linearRegressionDef.causalMetrics),
                                                   linearRegressionDef.numValueBuckets,
                                                   linearRegressionDef.numBucketsToCapForFairness,
                                                   linearRegressionDef.maxSamplesPerBucket,
                                                   linearRegressionDef.maxExponent,
                                                   linearRegressionDef.errorStatsWindowSize);
        if (_linearRegressionMap.put(linearRegressionDef.name, lr) != null) {
          throw new ConfigException("Linear regression " + linearRegressionDef.name + " is defined more than once.");
        }
        for (MetricInfo resultantMetric : resultantMetrics) {
          _linearRegressionsByResultantMetrics.computeIfAbsent(resultantMetric,
                                                              rm -> new TreeSet<>(new LinearRegressionComparator(resultantMetric)))
                                              .add(lr);
        }
      }
    } finally {
      try {
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        // let it go.
      }
    }
  }

  private void updateLinearRegressionRank() {
    for (Map.Entry<MetricInfo, SortedSet<LinearRegression>> entry : _linearRegressionsByResultantMetrics.entrySet()) {
      SortedSet<LinearRegression> linearRegressions = entry.getValue();
      Set<LinearRegression> temp = new HashSet<>(linearRegressions);
      linearRegressions.clear();
      linearRegressions.addAll(temp);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Updated linear regression rank for {} to {}", entry.getKey().name(), 
                  linearRegressions.stream().map(LinearRegression::name).collect(toList()));
      }
      if (LOG.isTraceEnabled()) {
        for (LinearRegression linearRegression : linearRegressions) {
          LOG.debug("{} estimation error stats for {}: {}", linearRegression.name(), entry.getKey().name(), 
                    linearRegression.estimationErrorStats(entry.getKey()));
        }
      }
    }
  }

  private Set<MetricInfo> toMetricInfoSet(Set<String> metricNames) {
    Set<MetricInfo> metricInfoSet = new HashSet<>();
    for (String metricName : metricNames) {
      metricInfoSet.add(metricDef().metricInfo(metricName));
    }
    return metricInfoSet;
  }

  /**
   * A class that compares multiple linear regressions for the same resultant metric. The smaller the error
   * is the higher the linear regression is ranked.
   */
  private static class LinearRegressionComparator implements Comparator<LinearRegression> {
    private final MetricInfo _resultantMetric;

    LinearRegressionComparator(MetricInfo resultantMetric) {
      _resultantMetric = resultantMetric;
    }

    @Override
    public int compare(LinearRegression lr1, LinearRegression lr2) {
      if (lr1 == lr2) {
        return 0;
      }
      boolean lr1TrainingCompleted = lr1.trainingCompleted(_resultantMetric);
      boolean lr2TrainingCompleted = lr2.trainingCompleted(_resultantMetric);
      // First check if the estimation has finished or not.
      if (lr1TrainingCompleted && !lr2TrainingCompleted) {
        return -1;
      } else if (!lr1TrainingCompleted && lr2TrainingCompleted) {
        return 1;
      } else if (lr1TrainingCompleted) {
        // If both are finished, check the error percentile.
        DescriptiveStatistics errStats1 = lr1.estimationErrorStats(_resultantMetric);
        DescriptiveStatistics errStats2 = lr2.estimationErrorStats(_resultantMetric);
        // Compare the errors in this percentile order.
        for (double percentile : Arrays.asList(95.0, 90.0, 75.0, 50.0)) {
          int result = Double.compare(errStats1.getPercentile(percentile), errStats2.getPercentile(percentile));
          if (result != 0) {
            return result;
          }
        }
      }
      // Lastly check the name.
      return lr1.name().compareTo(lr2.name());
    }
  }

  /**
   * Classes for JSON parsing using GSON.
   */
  private static class LinearRegressionList {
    List<LinearRegressionDef> linearRegressions;
  }

  private static class LinearRegressionDef {
    String name;
    Set<String> causalMetrics;
    Set<String> resultantMetrics;
    int numValueBuckets;
    int numBucketsToCapForFairness;
    int maxSamplesPerBucket;
    int maxExponent;
    int errorStatsWindowSize;
  }
}
