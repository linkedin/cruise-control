/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.regression;

import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * A class holding the data that could be passed to the linear regression directly.
 */
class DataForLinearRegression {
  private final Map<MetricInfo, double[]> _resultantMetricsValues;
  private final double[][] _causalMetricsValues;
  private final Map<MetricInfo, MetricValueSelector> _valueSelectors;
  private final CausalRelation _causalRelation;
  private final Set<MetricInfo> _resultantMetrics;
  private final int _valueCounts;
  private final int _maxExponent;
  private volatile int _metricsCount;

  /**
   * Constructor.
   * @param causalRelation the defined causal relation.
   * @param resultantMetrics the resultant metrics to include.
   * @param valueCounts the total count of values.
   * @param valueSelectors the value selectors used to pick the values to be added to this class.
   * @param maxExponent the maximum exponent of the polynomial function.
   */
  DataForLinearRegression(CausalRelation causalRelation,
                          Set<MetricInfo> resultantMetrics,
                          int valueCounts,
                          Map<MetricInfo, MetricValueSelector> valueSelectors,
                          int maxExponent) {
    _causalRelation = causalRelation;
    _resultantMetrics = resultantMetrics;
    _valueCounts = valueCounts;
    _maxExponent = maxExponent;
    _resultantMetricsValues = new HashMap<>();
    _causalMetricsValues = new double[_valueCounts][_causalRelation.causalMetrics().size() * _maxExponent];
    _valueSelectors = valueSelectors;
    _metricsCount = 0;
  }

  /**
   * Add values for linear regression. This methods only picks the values for the interested metrics. 
   * @param metricValues an array containing values for all the metrics.
   */
  void addValues(double[] metricValues) {
    int index = _metricsCount++;
    for (MetricInfo resultantMetric : _resultantMetrics) {
      double[] resultantMetricValues = 
          _resultantMetricsValues.computeIfAbsent(resultantMetric, rm -> new double[_valueCounts]);
      resultantMetricValues[index] = metricValues[_causalRelation.index(resultantMetric)];
    }

    for (int i = 1; i <= _maxExponent; i++) {
      for (MetricInfo metric : _causalRelation.causalMetrics()) {
        int causalMetricIndex = _causalRelation.index(metric);
        double metricValue = metricValues[causalMetricIndex];
        int valueIndex = causalMetricIndex + (i - 1) * _causalRelation.causalMetrics().size();
        _causalMetricsValues[index][valueIndex] = Math.pow(metricValue, i);
      }
    }
  }

  /**
   * @return the resultant metric values for linear regression.
   */
  Map<MetricInfo, double[]> resultantMetricValues() {
    return _resultantMetricsValues;
  }

  /**
   * @return the causal metric values for linear regression.
   */
  double[][] causalMetricsValues() {
    return _causalMetricsValues;
  }

  /**
   * @return the value selectors used to pick the values for linear regression.
   */
  Map<MetricInfo, MetricValueSelector> metricValueSelectors() {
    return _valueSelectors;
  }
}
