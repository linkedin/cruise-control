/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * The aggregated metrics for all the windows and the extrapolation information if there is any extrapolation used.
 */
public class ValuesAndExtrapolations {
  private final AggregatedMetricValues _metricValues;
  private final Map<Integer, Extrapolation> _extrapolations;
  private List<Long> _windows;

  /**
   * Construct the values and extrapolations.
   * @param metricValues the metric values.
   * @param extrapolations the extrapolations by corresponding metric value indices.
   */
  public ValuesAndExtrapolations(AggregatedMetricValues metricValues, Map<Integer, Extrapolation> extrapolations) {
    _metricValues = metricValues;
    _extrapolations = extrapolations;
  }

  /**
   * Get the metric values for all the included windows.
   * <p>
   *   The returned metric values are store in a two-dimensional array. To get the time window associated with
   *   each metric values, use the array index to look up in the time window array returned by calling
   *   {@link #window(int)} with the index.
   * </p>
   * @return The {@link AggregatedMetricValues} for all the included windows.
   */
  public AggregatedMetricValues metricValues() {
    return _metricValues;
  }

  /**
   * Get the extrapolations for the values. The keys of the returned map are the indices of the {@link AggregatedMetricValues}
   * returned by {@link #metricValues()}.
   *
   * @return The {@link Extrapolation}s for the values if exist.
   */
  public Map<Integer, Extrapolation> extrapolations() {
    return Collections.unmodifiableMap(_extrapolations);
  }

  /**
   * Get the window list for the metric values. The time window of metric value at {@code index} is the time window
   * at the same index of the array returned by this this method.
   *
   * @return The window time list associated with the metric values array in the {@link AggregatedMetricValues} returned
   * by {@link #metricValues()}
   */
  public List<Long> windows() {
    return Collections.unmodifiableList(_windows);
  }

  /**
   * Get the time window of a specific index.
   *
   * @param index the index to get time window.
   * @return The time window of the given index.
   */
  public long window(int index) {
    return _windows.get(index);
  }

  /**
   * Method to set the windows array.
   * @param windows the windows for the values.
   */
  public void setWindows(List<Long> windows) {
    _windows = windows;
  }

  /**
   * Create an empty ValuesAndExtrapolations.
   * @param numWindows the number of windows.
   * @param metricDef the metric definition.
   * @return An empty ValuesAndExtrapolations.
   */
  static ValuesAndExtrapolations empty(int numWindows, MetricDef metricDef) {
    Map<Short, MetricValues> values = new HashMap<>();
    for (short i = 0; i < metricDef.all().size(); i++) {
      values.put(i, new MetricValues(numWindows));
    }
    Map<Integer, Extrapolation> extrapolations = new HashMap<>();
    for (int i = 0; i < numWindows; i++) {
      extrapolations.put(i, Extrapolation.NO_VALID_EXTRAPOLATION);
    }
    return new ValuesAndExtrapolations(new AggregatedMetricValues(values), extrapolations);
  }
}
