/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;


/**
 * The aggregated metric values.
 */
public class AggregatedMetricValues implements Serializable {
  private static final long serialVersionUID = -6840253566423285966L;
  private final Map<Integer, MetricValues> _metricValues;

  public AggregatedMetricValues() {
    _metricValues = new HashMap<>();
  }

  public AggregatedMetricValues(Map<Integer, MetricValues> metricValues) {
    if (metricValues == null) {
      throw new IllegalArgumentException("The metric values cannot be null");
    }
    int length = -1;
    for (MetricValues values : metricValues.values()) {
      if (length < 0) {
        length = values.length();
      } else if (length != values.length()) {
        throw new IllegalArgumentException("The metric values must have the same length for each metric. Saw two "
                                               + "different lengths of " + length + " and " + values.length());
      }
    }
    _metricValues = metricValues;
  }

  public MetricValues valuesFor(int metricId) {
    return _metricValues.get(metricId);
  }

  public int length() {
    return _metricValues.isEmpty() ? 0 : _metricValues.values().iterator().next().length();
  }

  public boolean isEmpty() {
    return _metricValues.isEmpty();
  }

  public Set<Integer> metricIds() {
    return Collections.unmodifiableSet(_metricValues.keySet());
  }

  public void add(int metricId, MetricValues metricValuesToAdd) {
    if (metricValuesToAdd == null) {
      throw new IllegalArgumentException("The metric values to be added cannot be null");
    }
    if (!_metricValues.isEmpty() && metricValuesToAdd.length() != length()) {
      throw new IllegalArgumentException("The existing metric length is " + length() + " which is different from the"
                                             + " metric length of " + metricValuesToAdd.length() + " that is being added.");
    }
    MetricValues metricValues = _metricValues.computeIfAbsent(metricId, id -> new MetricValues(metricValuesToAdd.length()));
    metricValues.add(metricValuesToAdd);
  }

  public void add(AggregatedMetricValues other) {
    for (Map.Entry<Integer, MetricValues> entry : other.metricValues().entrySet()) {
      int metricId = entry.getKey();
      MetricValues otherValuesForMetric = entry.getValue();
      MetricValues valuesForMetric = _metricValues.computeIfAbsent(metricId, id -> new MetricValues(otherValuesForMetric.length()));
      if (valuesForMetric.length() != otherValuesForMetric.length()) {
        throw new IllegalStateException("The two values arrays have different lengths " + valuesForMetric.length()
                                            + " and " + otherValuesForMetric.length());
      }
      valuesForMetric.add(otherValuesForMetric);
    }
  }

  public void subtract(AggregatedMetricValues other) {
    for (Map.Entry<Integer, MetricValues> entry : other.metricValues().entrySet()) {
      int metricId = entry.getKey();
      MetricValues otherValuesForMetric = entry.getValue();
      MetricValues valuesForMetric = valuesFor(metricId);
      if (valuesForMetric == null) {
        throw new IllegalStateException("Cannot subtract a values from a non-existing MetricValues");
      }
      if (valuesForMetric.length() != otherValuesForMetric.length()) {
        throw new IllegalStateException("The two values arrays have different lengths " + valuesForMetric.length()
                                            + " and " + otherValuesForMetric.length());
      }
      valuesForMetric.subtract(otherValuesForMetric);
    }
  }

  public void clear() {
    _metricValues.clear();
  }

  public void writeTo(OutputStream out) throws IOException {
    OutputStreamWriter osw = new OutputStreamWriter(out, StandardCharsets.UTF_8);
    osw.write("{%n");
    for (Map.Entry<Integer, MetricValues> entry : _metricValues.entrySet()) {
      osw.write(String.format("metricId:\"%d\", values:\"", entry.getKey()));
      entry.getValue().writeTo(out);
      osw.write("\"");
    }
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner("\n", "{", "}");
    for (Map.Entry<Integer, MetricValues> entry : _metricValues.entrySet()) {
      joiner.add(String.format("metricId:\"%d\", values:\"%s\"", entry.getKey(), entry.getValue()));
    }
    return joiner.toString();
  }

  private Map<Integer, MetricValues> metricValues() {
    return _metricValues;
  }
}
