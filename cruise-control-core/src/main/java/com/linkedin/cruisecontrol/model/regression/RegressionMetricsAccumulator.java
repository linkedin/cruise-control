/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.regression;

import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that helps holding the raw metrics values for a causal relation regression. The raw metric values are
 * kept with diversity requirement.
 *
 * @see MetricDiversity
 */
public class RegressionMetricsAccumulator {
  private static final Logger LOG = LoggerFactory.getLogger(RegressionMetricsAccumulator.class);
  // The sample id generator.
  private static final AtomicLong ID_GENERATOR = new AtomicLong(0);
  // Sample id to the sample value array.
  private final SortedMap<Long, double[]> _values;
  // The metric id to the diversity of that metric.
  private final Map<MetricInfo, MetricDiversity> _diversityForMetrics;
  // The number of buckets to maintain.
  private final int _numBuckets;
  // The number of buckets to include for linear regression model. If there are N buckets and this value is set to x,
  // then number of samples in the x'th bucket ordered by number of samples will be used to determine the maximum
  // samples in each bucket that will be used.
  private final int _numBucketsToCapForFairness;
  // The maximum allowed number for each bucket.
  private final int _maxSamplesPerBucket;
  // The causal relation among all the metrics.
  private final CausalRelation _causalRelation;
  // The degree of the polynomial function to use for regression.
  private final int _maxExponent;

  RegressionMetricsAccumulator(CausalRelation causalRelation,
                               int numBuckets,
                               int numBucketsToCapForFairness,
                               int maxSamplesPerBucket,
                               int maxExponent) {
    _causalRelation = causalRelation;
    // Use the reverse order to prioritize the newer samples.
    _values = new TreeMap<>(Comparator.reverseOrder());
    _diversityForMetrics = new HashMap<>();
    _numBuckets = numBuckets;
    _numBucketsToCapForFairness = numBucketsToCapForFairness;
    _maxSamplesPerBucket = maxSamplesPerBucket;
    _maxExponent = maxExponent;
  }

  /**
   * Record a new observation of the raw metric values.
   * @param metricValues the observation of metric values. The index represents the index of each metric in the
   * {@link CausalRelation}.
   *                     
   * @return true if there might be some old metric values that need to be removed, false otherwise.
   */
  synchronized boolean recordMetricValues(double[] metricValues) {
    // rebucket if needed.
    maybeRebucket(metricValues);
    // Always add the metric to the map.
    _values.put(ID_GENERATOR.getAndIncrement(), metricValues);
    boolean maybeRemoveOldValues = false;
    for (Map.Entry<MetricInfo, MetricDiversity> diversityEntry : _diversityForMetrics.entrySet()) {
      MetricInfo metric = diversityEntry.getKey();
      MetricDiversity diversity = diversityEntry.getValue();
      if (diversity.addValue(metricValues[_causalRelation.index(metric)])) {
        maybeRemoveOldValues = true;
      }
    }
    return maybeRemoveOldValues;
  }

  /**
   * Remove some old values that are no longer needed from the accumulator.
   */
  synchronized void maybeRemoveOldValues() {
    Map<Long, double[]> valuesInReverseOrder = new TreeMap<>(_values);
    for (Map.Entry<Long, double[]> entry : valuesInReverseOrder.entrySet()) {
      double[] values = entry.getValue();
      boolean shouldRemove = true;
      for (Map.Entry<MetricInfo, MetricDiversity> diversityEntry : _diversityForMetrics.entrySet()) {
        MetricInfo metric = diversityEntry.getKey();
        MetricDiversity diversity = diversityEntry.getValue();
        shouldRemove = shouldRemove && diversity.shouldRemove(values[_causalRelation.index(metric)]);
      }
      // The values are only removed when all the metrics agrees.
      if (shouldRemove) {
        _values.remove(entry.getKey());
        for (Map.Entry<MetricInfo, MetricDiversity> diversityEntry : _diversityForMetrics.entrySet()) {
          MetricInfo metric = diversityEntry.getKey();
          MetricDiversity diversity = diversityEntry.getValue();
          diversity.removeSample(values[_causalRelation.index(metric)]);
        }
      }
    }
  }

  /**
   * Get the data for linear regression.
   *
   * @return {@link DataForLinearRegression} that is ready for regression.
   */
  synchronized DataForLinearRegression getRegressionData(Set<MetricInfo> resultantMetrics) {
    for (MetricInfo resultantMetric : resultantMetrics) {
      if (!_causalRelation.resultantMetrics().contains(resultantMetric)) {
        throw new IllegalArgumentException("Metric " + resultantMetric.name() + " is not defined as a resultant" 
                                               + " metric. Defined resultant metrics are " 
                                               + _causalRelation.resultantMetrics());
      }
    }
    Map<MetricInfo, MetricValueSelector> valueSelectors = new HashMap<>();

    for (MetricInfo resultantMetric : resultantMetrics) {
      MetricDiversity resultantMetricDiversity = _diversityForMetrics.get(resultantMetric);
      valueSelectors.put(resultantMetric, new MetricValueSelector(_numBucketsToCapForFairness, resultantMetricDiversity));
    }

    _causalRelation.causalMetrics().forEach(causalMetric -> {
      MetricDiversity causalMetricDiversity = _diversityForMetrics.get(causalMetric);
      valueSelectors.put(causalMetric, new MetricValueSelector(_numBucketsToCapForFairness, causalMetricDiversity));
    });


    List<double[]> valuesIncluded = new ArrayList<>();
    for (double[] valuesForMetrics : _values.values()) {
      if (shouldInclude(valuesForMetrics, valueSelectors)) {
        valuesIncluded.add(valuesForMetrics);
      }
    }
    DataForLinearRegression data = new DataForLinearRegression(_causalRelation, resultantMetrics, 
                                                               valuesIncluded.size(), valueSelectors, _maxExponent);
    valuesIncluded.forEach(data::addValues);
    return data;
  }

  /**
   * @return the sample values that is kept for linear regression.
   */
  synchronized Map<Long, double[]> sampleValues() {
    return Collections.unmodifiableMap(_values);
  }

  /**
   * Clear the state of the RegressionMetricsAccumulator.
   */
  synchronized void reset() {
    _values.clear();
    _diversityForMetrics.clear();
  }

  /**
   * @return the diversity for the metrics.
   */
  synchronized Map<MetricInfo, MetricDiversity> diversityForMetrics() {
    return Collections.unmodifiableMap(_diversityForMetrics);
  }

  /**
   * Get the training data completeness. The training data completeness is defined as the following:
   * <ol>
   *   1. For each metric, <tt>CompletenessForMetric = min(1, NonEmptyBucket / NumBucketsToCapForFairness)</tt>;
   *   2. For all the metrics, <tt>OverallCompleteness = Sum(CompletenessForMetric) / NumMetrics</tt>;
   * </ol>
   *
   * Note that as the new metric values come in, dynamic rebucketing may happen. Therefore it is possible that a
   * previously completed metric become not completed later on.
   *
   * @return the overall completeness of the metric values.
   */
  synchronized double trainingDataCompleteness() {
    double sumOfMetricCompleteness = 0.0;
    for (MetricDiversity diversity : _diversityForMetrics.values()) {
      int numNonEmptyBuckets = diversity.countsByBucket().size();
      sumOfMetricCompleteness += Math.min(1.0, (double) numNonEmptyBuckets / _numBucketsToCapForFairness);
    }
    return sumOfMetricCompleteness / _causalRelation.indexes().size();
  }

  /**
   * Get the diversity summary of the accumulated metric sample values.
   * @return the diversity summary of the accumulated metric sample values.
   */
  synchronized String diversitySummary() {
    StringBuilder sb = new StringBuilder();
    sb.append("NumSamples: ").append(_values.size()).append("\n");
    sb.append("Diversity: \n");
    for (Map.Entry<MetricInfo, MetricDiversity> entry : _diversityForMetrics.entrySet()) {
      sb.append(entry.getKey().name()).append("\n").append(entry.getValue().diversitySummary()).append("\n");
    }
    return sb.toString();
  }

  /**
   * Get the string summary of diversity of the given data. 
   * 
   * @param dataForLinearRegression The data to get the diversity summary for.
   * @return A string summarize the diversity of the given data.
   */
  synchronized String diversitySummary(DataForLinearRegression dataForLinearRegression) {
    StringBuilder sb = new StringBuilder();
    sb.append("NumSamples: ").append(dataForLinearRegression.causalMetricsValues().length).append("\n");
    sb.append("Diversity: \n");
    for (Map.Entry<MetricInfo, MetricValueSelector> entry : dataForLinearRegression.metricValueSelectors().entrySet()) {
      MetricInfo metric = entry.getKey();
      sb.append(metric.name()).append("\n")
        .append("fairCountCap: ").append(entry.getValue().fairCountCap()).append("\n")
        .append(_diversityForMetrics.get(metric).diversitySummary(entry.getValue().includedCountsByBuckets())).append("\n");
    }
    return sb.toString();
  }

  /**
   * Determine if a given value array should be included in the linear regression data.
   * @param values the value array check.
   * @param valueSelectors the selectors to choose values.
   * @return true if the value should be included, false otherwise.
   */
  private boolean shouldInclude(double[] values, Map<MetricInfo, MetricValueSelector> valueSelectors) {
    boolean included = false;
    for (Map.Entry<MetricInfo, MetricValueSelector> entry : valueSelectors.entrySet()) {
      MetricInfo metric = entry.getKey();
      MetricValueSelector valueSelector = entry.getValue();
      included = valueSelector.include(values[_causalRelation.index(metric)]);
      if (included) {
        break;
      }
    }
    if (included) {
      valueSelectors.forEach((metric, valueSelector) -> valueSelector.included(values[_causalRelation.index(metric)]));
    }
    return included;
  }

  /**
   * Rebucket the value range for a metric if the witnessed value range has changed.
   * @param metricValues a new observation of metric values.
   */
  private void maybeRebucket(double[] metricValues) {
    for (Map.Entry<MetricInfo, Integer> entry : _causalRelation.indexes().entrySet()) {
      MetricInfo metric = entry.getKey();
      int metricIndex = entry.getValue();
      MetricDiversity diversity =
          _diversityForMetrics.computeIfAbsent(metric, c -> new MetricDiversity(_numBuckets,
                                                                                _maxSamplesPerBucket));
      // Update the bucket info. If the bucket has chnanged, we need to run all the metrics through the diversity
      // again to rebucket them.
      if (diversity.updateBucketInfo(metricValues[metricIndex])) {
        diversity.clear();
        for (double[] values : _values.values()) {
          diversity.addValue(values[metricIndex]);
        }
        LOG.debug("Rebucketed for metric {}. Diversity: {}", metric, diversity);
      }
    }
  }
}
