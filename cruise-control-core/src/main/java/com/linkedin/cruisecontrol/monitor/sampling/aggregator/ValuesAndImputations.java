/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * The aggregated metrics for all the windows and the imputation information if there is any imputation used.
 */
public class ValuesAndImputations {
  private final AggregatedMetricValues _metricValues;
  private final Map<Integer, Imputation> _imputations;
  private List<Long> _windows;

  /**
   * Package private constructor.
   * @param metricValues the metric values. 
   * @param imputations the imputations by window indexes.
   */
  ValuesAndImputations(AggregatedMetricValues metricValues,
                       Map<Integer, Imputation> imputations) {
    _metricValues = metricValues;
    _imputations = imputations;
  }

  /**
   * Get the metric values for all the included windows.
   * <p>
   *   The returned metric values are store in a two-dimensional array. To get the time window associated with 
   *   each metric values, use the array index to look up in the time window array returned by calling 
   *   {@link #window(int)} with the index.
   * </p>
   * @return the {@link AggregatedMetricValues} for all the included windows.
   */
  public AggregatedMetricValues metricValues() {
    return _metricValues;
  }

  /**
   * Get the imputations for the values. The keys of the returned map are the indexes of the {@link AggregatedMetricValues}
   * returned by {@link #metricValues()}.
   * 
   * @return the {@link Imputation}s for the values if exist.
   */
  public Map<Integer, Imputation> imputations() {
    return _imputations;
  }

  /**
   * Get the window list for the metric values. The time window of metric value at <tt>index</tt> is the time window
   * at the same index of the array returned by this this method.
   *
   * @return the window time list associated with the metric values array in the {@link AggregatedMetricValues} returned
   * by {@link #metricValues()} 
   */
  public List<Long> windows() {
    return Collections.unmodifiableList(_windows);
  }

  /**
   * Get the time window of a specific index.
   *
   * @param index the index to get time window.
   * @return the time window of the given index.
   */
  public long window(int index) {
    return _windows.get(index);
  }

  /**
   * Package private method to set the windows array.
   * @param windows the windows for the values.
   */
  void setWindows(List<Long> windows) {
    _windows = windows;
  }

  static ValuesAndImputations empty(int numWindows, MetricDef metricDef) {
    Map<Integer, MetricValues> values = new HashMap<>();
    for (int i : metricDef.all().keySet()) {
      values.put(i, new MetricValues(numWindows));
    }
    Map<Integer, Imputation> imputations = new HashMap<>();
    for (int i = 0; i < numWindows; i++) {
      imputations.put(i, Imputation.NO_VALID_IMPUTATION);
    }
    return new ValuesAndImputations(new AggregatedMetricValues(values), imputations);
  }
}
