/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.regression;

import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;


/**
 * A class that hosts the linear regression coefficients for a single metric. The coefficients of a linear 
 * regression model are represented in a polynomial form. More specifically,
 * <p>
 *   <tt>RM = Sum(A(i, j) * CM(i)^j), where CM(i) means the i(th) causal metric, i = 0, 1, ... k; 
 *   j means the order of the causal metric, j = 1, 2, ... n</tt>
 * </p>
 * This class hosts the coefficients A(i, j).
 */
public class LinearRegressionCoefficients {
  // A map whose keys are the exponents and the values are coefficients for the corresponding exponents.
  private final Map<Integer, Map<MetricInfo, Double>> _coefficients;
  private int _metricNameMaxLength = 0;

  /**
   * This class should not be constructed outside of the package.
   */
  LinearRegressionCoefficients() {
    _coefficients = new TreeMap<>();
  }

  /**
   * Set the coefficient for a given exponent and causal metric.
   * @param exponent the exponent to set the coefficient for.
   * @param causalMetric the causal metric to set the coefficient for.
   * @param coefficient the coefficient.
   */
  void setCoefficients(int exponent, MetricInfo causalMetric, double coefficient) {
    _coefficients.computeIfAbsent(exponent, e -> new TreeMap<>())
                 .put(causalMetric, coefficient);
    _metricNameMaxLength = Math.max(_metricNameMaxLength, causalMetric.name().length());
  }

  /**
   * Get the coefficients of a given causal metric.
   * 
   * @param causalMetric the causal metric to get coefficients for;
   * @return a mapping from exponent to the coefficients of the given causal metric.
   */
  Map<Integer, Double> getCoefficients(MetricInfo causalMetric) {
    Map<Integer, Double> coefficients = new HashMap<>();
    for (Map.Entry<Integer, Map<MetricInfo, Double>> entry : _coefficients.entrySet()) {
      coefficients.put(entry.getKey(), entry.getValue().get(causalMetric));
    }
    return coefficients;
  }

  /**
   * Get the coefficients for all causal metrics of a given exponent.
   *
   * @param exponent the exponent to get coefficients.
   * @return a mapping from causal metrics to their coefficients for the given exponent.
   */
  Map<MetricInfo, Double> getCoefficients(int exponent) {
    return _coefficients.get(exponent);
  }

  /**
   * Get the well formatted string of the coefficients with given indentation.
   * @param indent the indentation to apply.
   * @return the formatted string with the indentation.
   */
  String toStringWithIndent(String indent, String exponentSplitter) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Integer, Map<MetricInfo, Double>> entry : _coefficients.entrySet()) {
      int exponent = entry.getKey();
      Map<MetricInfo, Double> coefficients = entry.getValue();
      sb.append(indent);
      appendWithExponents(sb, coefficients, exponent).append(exponentSplitter);
    }
    return sb.toString();
  }
  
  private StringBuilder appendWithExponents(StringBuilder sb, Map<MetricInfo, Double> coefficients, int exponent) {
    int maxExponentStringLength = Integer.toString(_coefficients.size()).length();
    int exponentLength = Integer.toString(exponent).length();
    StringJoiner sj = new StringJoiner(" + ", "", exponent == _coefficients.size() ? "" : " + ");
    for (Map.Entry<MetricInfo, Double> entry : coefficients.entrySet()) {
      MetricInfo causalMetric = entry.getKey();
      int metricPrintLength = maxExponentStringLength + causalMetric.name().length() - exponentLength;
      sj.add(String.format("%.4f * %" + metricPrintLength + "s^%d", 
                           entry.getValue(), entry.getKey().name(), exponent));
    }
    return sb.append(sj.toString());
  }
}
