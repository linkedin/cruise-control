/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.regression;

import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.model.estimator.MetricEstimator;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that perform the linear regression for causal relations. This class supports multiple resultant
 * metrics as long as the resultant metrics share the same causal metrics.
 *
 * This class is thread safe, but it just uses simple synchronization because the sample update frequency is
 * expected to be low.
 */
public class LinearRegression {
  private static final Logger LOG = LoggerFactory.getLogger(LinearRegression.class);
  private final String _name;
  // From resultant metric ids to the coefficients to that metric.
  private final Map<MetricInfo, LinearRegressionCoefficients> _coefficients;
  // The estimation errors for each of the resultant metric.
  private final Map<MetricInfo, SynchronizedDescriptiveStatistics> _estimationErrorStats;
  // The regression metrics accumulator that helps handling the metric selection and filtering
  private final CausalRelation _causalRelation;
  private final RegressionMetricsAccumulator _metricsAccumulator;
  private final int _maxExponent;
  private final int _errStatsWindowSize;

  /**
   * Construct a LinearRegression to represent the causal relation among given resultant metrics and causal metrics.
   * The resultant metrics and causal metrics are assumed to be independent to each other.
   *
   * @param name the name of this linear regression.
   * @param resultantMetrics the resultant metrics in the causal relation.
   * @param causalMetrics the causal metrics in the causal relation.
   * @param numBuckets the number of buckets to use to hold the metric values.
   * @param numBucketsToCapForFairness the number of top buckets with most samples to cap.
   *                                   See {@link MetricValueSelector} for more details.
   * @param maxSamplesPerBucket maximum number of metric values to use in each bucket for the linear regression.
   * @param maxExponent the degree of polynomial function to use in the linear regression.
   * @param errStatsWindowSize the number of values to keep when track the estimation error statistics.
   */
  public LinearRegression(String name,
                          Set<MetricInfo> resultantMetrics,
                          Set<MetricInfo> causalMetrics,
                          int numBuckets,
                          int numBucketsToCapForFairness,
                          int maxSamplesPerBucket,
                          int maxExponent,
                          int errStatsWindowSize) {
    _name = name;
    _causalRelation = new CausalRelation(resultantMetrics, causalMetrics);
    _coefficients = new HashMap<>();
    _estimationErrorStats = new HashMap<>();
    _metricsAccumulator = new RegressionMetricsAccumulator(_causalRelation, numBuckets, numBucketsToCapForFairness,
                                                           maxSamplesPerBucket, maxExponent);
    _maxExponent = maxExponent;
    _errStatsWindowSize = errStatsWindowSize;
  }

  /**
   * Get the name of this linear regression.
   * @return the name of this linear regression.
   */
  public String name() {
    return _name;
  }

  /**
   * @return the causal relation of this linear regression.
   */
  public CausalRelation causalRelation() {
    return _causalRelation;
  }

  /**
   * @return the max exponent used in this linear regression, i.e. the order of the polynomial function.
   */
  public int maxExponent() {
    return _maxExponent;
  }

  /**
   * Add a collection of metric samples as the training data for this linear regression.
   *
   * @param trainingSamples the {@link MetricSample}s to add as training data.
   */
  public synchronized void addMetricSamples(Collection<? extends MetricSample> trainingSamples) {
    if (trainingSamples != null && !trainingSamples.isEmpty()) {
      boolean maybeRemoveOldValues = false;
      for (MetricSample sample : trainingSamples) {
        double[] values = new double[_causalRelation.indexes().size()];
        for (Map.Entry<MetricInfo, Integer> entry : _causalRelation.indexes().entrySet()) {
          MetricInfo metric = entry.getKey();
          int index = entry.getValue();
          values[index] = sample.metricValue(metric.id());
        }
        if (_metricsAccumulator.recordMetricValues(values)) {
          maybeRemoveOldValues = true;
        }
        // For each sample added to the linear regression, the sample is used both for training and coefficients
        // verification.
        updateEstimationError(sample);
        LOG.trace("{} added metric sample {}", _name, sample);
      }
      if (maybeRemoveOldValues) {
        _metricsAccumulator.maybeRemoveOldValues();
      }
    }
  }

  /**
   * Trigger the calculation of the model parameters.
   *
   * @return A set of resultant metrics whose causal metric coefficients are generated;
   */
  public synchronized Set<MetricInfo> updateModelCoefficient() {
    Set<MetricInfo> completedResultantMetrics = new HashSet<>();
    for (MetricInfo resultantMetric : _causalRelation.resultantMetrics()) {
      DataForLinearRegression dataForLinearRegression =
          _metricsAccumulator.getRegressionData(Collections.singleton(resultantMetric));
      if (updateModelCoefficient(dataForLinearRegression)) {
        completedResultantMetrics.add(resultantMetric);
      }
    }
    return completedResultantMetrics;
  }

  /**
   * Check if the training for a given resultant metric has completed, i.e. the coefficients for all the defined
   * causal metrics are available.
   *
   * @param resultantMetric the resultant metric to check.
   * @return true if all the coefficients of causal metrics for the given resultant metric is ready, false otherwise.
   */
  public synchronized boolean trainingCompleted(MetricInfo resultantMetric) {
    return _coefficients.containsKey(resultantMetric);
  }

  /**
   * Get the coefficients of the given causal metric for the given resultant metric with all the exponents.
   *
   * @param resultantMetric the resultant metric to get the coefficients for.
   * @param causalMetric the causal metric whose coefficients should be returned.
   * @return A mapping from exponents to the coefficient of the given causal metric and resultant metric combination.
   */
  public synchronized Map<Integer, Double> getCoefficients(MetricInfo resultantMetric, MetricInfo causalMetric) {
    LinearRegressionCoefficients coefficientsForResultantMetric = _coefficients.get(resultantMetric);
    if (coefficientsForResultantMetric == null) {
      return null;
    } else {
      return coefficientsForResultantMetric.getCoefficients(causalMetric);
    }
  }

  /**
   * Estimate the values of the resultant metric given the causal metric values and the potential value changes.
   *
   * @param resultantMetric the resultant metric whose values are to be estimated.
   * @param causalMetricValues the causal metric values to use for the estimation.
   * @param causalMetricValueChanges the potential changes to the causal metrics.
   * @param changeType the type of causal metric value changes, i.e. add or subtract.
   * @return a {@link MetricValues} of the resultant metric.
   */
  public synchronized MetricValues estimate(MetricInfo resultantMetric,
                                            AggregatedMetricValues causalMetricValues,
                                            AggregatedMetricValues causalMetricValueChanges,
                                            MetricEstimator.ChangeType changeType) {
    LinearRegressionCoefficients coefficientsForResultantMetric = _coefficients.get(resultantMetric);
    if (coefficientsForResultantMetric == null) {
      throw new IllegalStateException("The coefficients for resultant metric " + resultantMetric + " are not "
                                          + "available for linear regression " + _name);
    }
    MetricValues result = new MetricValues(causalMetricValues.length());
    // Iterate over the exponents
    for (int exp = 1; exp <= _maxExponent; exp++) {
      Map<MetricInfo, Double> coefficients = coefficientsForResultantMetric.getCoefficients(exp);
      // Iterate over each causal metric
      for (Map.Entry<MetricInfo, Double> entry : coefficients.entrySet()) {
        MetricInfo causalMetric = entry.getKey();
        double coefficient = entry.getValue();
        MetricValues valuesForCausalMetric = causalMetricValues.valuesFor(causalMetric.id());
        MetricValues valueChangesForCausalMetric =
            causalMetricValueChanges == null ? null : causalMetricValueChanges.valuesFor(causalMetric.id());
        // Iterate over the values.
        for (int i = 0; i < result.length(); i++) {
          double causalMetricValue = valuesForCausalMetric.get(i);
          if (valueChangesForCausalMetric != null) {
            switch (changeType) {
              case ADDITION:
                causalMetricValue += valueChangesForCausalMetric.get(i);
                break;
              case SUBTRACTION:
                causalMetricValue -= valueChangesForCausalMetric.get(i);
                break;
              default:
                throw new IllegalArgumentException("Invalid change type " + changeType);
            }
          }
          result.add(i, coefficient * Math.pow(causalMetricValue, exp));
        }
      }
    }
    return result;
  }

  /**
   * Estimate the average value of the given resultant metric using the average values of the given causal metrics.
   *
   * @param resultantMetric the resultant metric whose value is to be estimated.
   * @param causalMetricValues the causal metrics values.
   * @return the estimated average value of the given resultant metric.
   */
  public synchronized double estimateAvg(MetricInfo resultantMetric, AggregatedMetricValues causalMetricValues) {
    LinearRegressionCoefficients coefficientsForResultantMetric = _coefficients.get(resultantMetric);
    if (coefficientsForResultantMetric == null) {
      throw new IllegalStateException("The coefficients for resultant metric " + resultantMetric + " are not "
                                          + "available for linear regression " + _name);
    }
    double estimatedResultantMetricValue = 0.0;
    for (int exp = 1; exp <= _maxExponent; exp++) {
      Map<MetricInfo, Double> coefficientsForExp = coefficientsForResultantMetric.getCoefficients(exp);
      for (Map.Entry<MetricInfo, Double> entry : coefficientsForExp.entrySet()) {
        estimatedResultantMetricValue +=
            entry.getValue() * Math.pow(causalMetricValues.valuesFor(entry.getKey().id()).avg(), exp);
      }
    }
    return estimatedResultantMetricValue;
  }

  /**
   * Get the estimation error statistics of this linear regression for the given resultant metric.
   * 
   * @param resultantMetric the resultant metric to get the estimation error statistics.
   * @return the estimation error statistics of this linear regression for the given resultant metric
   */
  public synchronized DescriptiveStatistics estimationErrorStats(MetricInfo resultantMetric) {
    return _estimationErrorStats.get(resultantMetric);
  }

  /**
   * Get the training completeness in a percent from 0 to 1. 0 means no training data at all, 1 means training
   * data is complete. The value returned by this method should not be confused with
   * {@link #trainingCompleted(MetricInfo)}. That method checks whether the coefficients could be generated or not.
   * This method indicates the completeness of the training data. In another word, based on what quality of the
   * training data were those coefficients generated.
   *
   * @return a double between 0 and 1, indicating the training data quality.
   */
  public synchronized double modelCoefficientTrainingCompleteness() {
    return _metricsAccumulator.trainingDataCompleteness();
  }

  /**
   * Get the state of this linear regression.
   * @return the state of this linear regression.
   */
  public synchronized LinearRegressionState modelState() {
    DataForLinearRegression dataForLinearRegression =
        _metricsAccumulator.getRegressionData(_causalRelation.resultantMetrics());
    updateModelCoefficient(dataForLinearRegression);
    Map<MetricInfo, String> errorStatsSummary = new TreeMap<>();
    _estimationErrorStats.forEach((m, errStats) -> errorStatsSummary.put(m, errorStatsSummary(errStats)));
    return new LinearRegressionState(_coefficients,
                                     _metricsAccumulator.diversitySummary(),
                                     _metricsAccumulator.diversitySummary(dataForLinearRegression),
                                     errorStatsSummary);
  }

  /**
   * Reset the state of this linear regression.
   */
  public synchronized void reset() {
    LOG.info("Resetting linear regression {}", _name);
    _coefficients.clear();
    _estimationErrorStats.clear();
    _metricsAccumulator.reset();
  }

  /**
   * Update the linear regression model coefficients.
   * @param dataForLinearRegression the data to perform linear regression.
   * @return true if the coefficients has been generated, false otherwise.
   */
  private boolean updateModelCoefficient(DataForLinearRegression dataForLinearRegression) {
    try {
      if (dataForLinearRegression.causalMetricsValues().length >= _causalRelation.causalMetrics().size()) {
        updateCoefficients(dataForLinearRegression);
        return true;
      }
    } catch (Exception e) {
      LOG.warn("{} Received exception when updating coefficients", _name, e);
    }
    return false;
  }

  private void updateCoefficients(DataForLinearRegression dataForLinearRegression) {
    for (Map.Entry<MetricInfo, double[]> entry : dataForLinearRegression.resultantMetricValues().entrySet()) {
      MetricInfo resultantMetric = entry.getKey();
      double[] resultantMetricValues = entry.getValue();
      OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
      regression.setNoIntercept(true);
      regression.newSampleData(resultantMetricValues, dataForLinearRegression.causalMetricsValues());
      double[] parameters = regression.estimateRegressionParameters();
      LinearRegressionCoefficients coefficients = new LinearRegressionCoefficients();
      for (MetricInfo causalMetric : _causalRelation.causalMetrics()) {
        for (int exp = 1; exp <= _maxExponent; exp++) {
          int paramIndex = _causalRelation.index(causalMetric) + (exp - 1) * _causalRelation.causalMetrics().size();
          coefficients.setCoefficients(exp, causalMetric, parameters[paramIndex]);
        }
      }
      _coefficients.put(resultantMetric, coefficients);
      // Log only when debug is enabled.
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("%s coefficients generated for %s: %s = %s", _name, resultantMetric.name(), 
                                resultantMetric.name(), coefficients.toStringWithIndent("", "")));
      }
    }
  }
  
  private void updateEstimationError(MetricSample sample) {
    for (MetricInfo resultantMetric : _causalRelation.resultantMetrics()) {
      LinearRegressionCoefficients coefficients = _coefficients.get(resultantMetric);
      if (coefficients != null) {
        double estimatedResultantMetricValue = 0.0;
        for (int exp = 1; exp <= _maxExponent; exp++) {
          Map<MetricInfo, Double> coefficientsForExp = coefficients.getCoefficients(exp);
          for (Map.Entry<MetricInfo, Double> entry : coefficientsForExp.entrySet()) {
            MetricInfo causalMetric = entry.getKey();
            estimatedResultantMetricValue +=
                coefficientsForExp.get(causalMetric) * Math.pow(sample.metricValue(causalMetric.id()), exp);
          }
        }
        double actualResultantMetricValue = sample.metricValue(resultantMetric.id());
        double error = Math.abs(estimatedResultantMetricValue - actualResultantMetricValue);
        DescriptiveStatistics estimationErrorStatsForRM =
            _estimationErrorStats.computeIfAbsent(resultantMetric, rm -> new SynchronizedDescriptiveStatistics(_errStatsWindowSize));
        estimationErrorStatsForRM.addValue(error);
        LOG.debug("{} estimated {}: actual: {}, estimated: {}, error: {}", _name, resultantMetric.name(),
                  actualResultantMetricValue, estimatedResultantMetricValue,
                  estimatedResultantMetricValue - actualResultantMetricValue);
      }
    }
  }

  private String errorStatsSummary(DescriptiveStatistics errorStats) {
    return String.format("Mean: %.3f, Std_Dev: %.3f, 50 Percentile: %.3f, 75 Percentile: %.3f, "
                             + "90 Percentile: %.3f, 95 Percentile: %.3f, 99 Percentile: %.3f, 99.9 Percentile: %.3f",
                         errorStats.getMean(), errorStats.getStandardDeviation(), errorStats.getPercentile(50),
                         errorStats.getPercentile(75), errorStats.getPercentile(90),
                         errorStats.getPercentile(95), errorStats.getPercentile(99),
                         errorStats.getPercentile(99.9));
  }

  /**
   * A class describing the linear regression state.
   * 1. The coefficients.
   * 2. The summary of available sample values
   * 3. The summary of used sample values
   * 4. The summary of estimation error stats.
   */
  public static class LinearRegressionState {
    private final Map<MetricInfo, LinearRegressionCoefficients> _modelCoefficients;
    private final String _availableSampleValuesSummary;
    private final String _usedSampleValuesSummary;
    private final Map<MetricInfo, String> _estimationErrorStatsSummary;

    LinearRegressionState(Map<MetricInfo, LinearRegressionCoefficients> coefficients,
                          String availableSampleValuesSummary,
                          String usedSampleValuesSummary,
                          Map<MetricInfo, String> estimationErrorStatsSummary) {
      _modelCoefficients = coefficients;
      _availableSampleValuesSummary = availableSampleValuesSummary;
      _usedSampleValuesSummary = usedSampleValuesSummary;
      _estimationErrorStatsSummary = estimationErrorStatsSummary;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("All available data summary\n").append(_availableSampleValuesSummary).append("\n\n");
      builder.append("Used data summary\n").append(_usedSampleValuesSummary);
      builder.append("Coefficients from available samples: \n");
      for (Map.Entry<MetricInfo, LinearRegressionCoefficients> entry : _modelCoefficients.entrySet()) {
        MetricInfo resultantMetric = entry.getKey();
        LinearRegressionCoefficients coefficientsForMetric = entry.getValue();
        builder.append(String.format("\tCoefficients for %s%n", resultantMetric.name()));
        builder.append(coefficientsForMetric.toStringWithIndent("\t", "\n"));
        builder.append("\n");
      }

      for (Map.Entry<MetricInfo, String> entry : _estimationErrorStatsSummary.entrySet()) {
        MetricInfo resultantMetric = entry.getKey();
        builder.append(String.format("%n%n%s estimation error histogram:%n", resultantMetric.name()))
               .append("\t").append(entry.getValue()).append("\n");
      }
      return builder.toString();
    }
  }
}
