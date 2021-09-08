/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


/**
 * The aggregated metric values.
 */
public class AggregatedMetricValues {
  // Metric values by metric id.
  private final Map<Short, MetricValues> _metricValues;

  /**
   * Create an empty metric values.
   */
  public AggregatedMetricValues() {
    _metricValues = new HashMap<>();
  }

  /**
   * Create an AggregatedMetricValues with the given values by metric ids.
   * @param valuesByMetricId the values of the metrics. The key is the metric id.
   */
  public AggregatedMetricValues(Map<Short, MetricValues> valuesByMetricId) {
    validateNotNull(valuesByMetricId, "The metric values cannot be null");
    int length = valuesByMetricId.isEmpty() ? -1 : valuesByMetricId.values().iterator().next().length();
    for (MetricValues values : valuesByMetricId.values()) {
      if (length != values.length()) {
        throw new IllegalArgumentException("The metric values must have the same length for each metric. Saw two "
                                               + "different lengths of " + length + " and " + values.length());
      }
    }
    _metricValues = valuesByMetricId;
  }

  /**
   * Get the {@link MetricValues} for the given metric id
   *
   * @param metricId the metric id to get metric values.
   * @return The {@link MetricValues} for the given metric id.
   */
  public MetricValues valuesFor(short metricId) {
    return _metricValues.get(metricId);
  }

  /**
   * Get all the metric values for the given metric ids. If shareValueArray is set to true, the returned result shares
   * the same values with this AggregatedMetricValues. If the value changes, it will be seen by the returned
   * result as well as this AggregatedMetricValues. Otherwise the returned result is a copy of the values at the
   * cost of data copy.
   *
   * @param metricIds the interested metric ids.
   * @param shareValueArray whether the returned result should share the same value array with this class or not.
   *
   * @return An AggregatedMetricValues containing the given metric ids if they exist.
   */
  public AggregatedMetricValues valuesFor(Collection<Short> metricIds, boolean shareValueArray) {
    AggregatedMetricValues values = new AggregatedMetricValues();
    metricIds.forEach(id -> {
      MetricValues valuesForId = _metricValues.get(id);
      if (valuesForId == null) {
        throw new IllegalArgumentException("Metric id " + id + " does not exist.");
      }
      if (shareValueArray) {
        values.metricValues().put(id, valuesForId);
      } else {
        values.add(id, valuesForId);
      }
    });
    return values;
  }

  /**
   * Get a MetricValues which contains the sum of the values for all the metrics of a group in each corresponding
   * window. This is typically used to add up the metrics of the same kind, e.g. network IO might have multiple
   * more granular metrics, one may want to know what is the total network IO.
   *
   * @param group the group to get the metrics for.
   * @param metricDef the metric definitions
   * @param shareValueArray whether the returned result should share the same value array with this class or not when
   *                  possible.
   *
   * @return The sum of the metric values of the given metric ids.
   */
  public MetricValues valuesForGroup(String group, MetricDef metricDef, boolean shareValueArray) {
    Collection<MetricInfo> metricInfos = metricDef.metricInfoForGroup(group);
    if (metricInfos.size() == 1 && shareValueArray) {
      return _metricValues.get(metricInfos.iterator().next().id());
    } else {
      MetricValues metricValues = new MetricValues(length());
      metricInfos.forEach(info -> {
        MetricValues valuesForId = _metricValues.get(info.id());
        if (valuesForId == null) {
          throw new IllegalArgumentException("Metric " + info + " does not exist.");
        }
        metricValues.add(valuesForId);
      });
      return metricValues;
    }
  }

  /**
   * @return The array length of the metric values.
   */
  public int length() {
    return _metricValues.isEmpty() ? 0 : _metricValues.values().iterator().next().length();
  }

  /**
   * Check if the AggregatedMetricValues contains value for any metrics. Note that this call does not verify
   * if the values array for a particular metric is empty or not.
   *
   * @return {@code true} the aggregated metric values is empty, {@code false} otherwise.
   */
  public boolean isEmpty() {
    return _metricValues.isEmpty();
  }

  /**
   * @return The ids of all the metrics in this cluster.
   */
  public Set<Short> metricIds() {
    return Collections.unmodifiableSet(_metricValues.keySet());
  }

  /**
   * Add the metric value to the given metric id. If the metric values for the given metric already exists, it will
   * add the given metric values to it.
   * @param metricId the metric id the values associated with.
   * @param metricValuesToAdd the metric values to add.
   */
  public void add(short metricId, MetricValues metricValuesToAdd) {
    validateNotNull(metricValuesToAdd, "The metric values to be added cannot be null");
    if (!_metricValues.isEmpty() && metricValuesToAdd.length() != length()) {
      throw new IllegalArgumentException("The existing metric length is " + length() + " which is different from the"
                                             + " metric length of " + metricValuesToAdd.length() + " that is being added.");
    }
    MetricValues metricValues = _metricValues.computeIfAbsent(metricId, id -> new MetricValues(metricValuesToAdd.length()));
    metricValues.add(metricValuesToAdd);
  }

  /**
   * Add another AggregatedMetricValues to this one.
   *
   * @param other the other AggregatedMetricValues.
   */
  public void add(AggregatedMetricValues other) {
    for (Map.Entry<Short, MetricValues> entry : other.metricValues().entrySet()) {
      short metricId = entry.getKey();
      MetricValues otherValuesForMetric = entry.getValue();
      MetricValues valuesForMetric = _metricValues.computeIfAbsent(metricId, id -> new MetricValues(otherValuesForMetric.length()));
      if (valuesForMetric.length() != otherValuesForMetric.length()) {
        throw new IllegalStateException("The two values arrays have different lengths " + valuesForMetric.length()
                                        + " and " + otherValuesForMetric.length());
      }
      valuesForMetric.add(otherValuesForMetric);
    }
  }

  /**
   * Subtract another AggregatedMetricValues from this one.
   *
   * @param other the other AggregatedMetricValues to subtract from this one.
   */
  public void subtract(AggregatedMetricValues other) {
    for (Map.Entry<Short, MetricValues> entry : other.metricValues().entrySet()) {
      short metricId = entry.getKey();
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

  /**
   * Clear all the values in this AggregatedMetricValues.
   */
  public void clear() {
    _metricValues.clear();
  }

  /**
   * Write this AggregatedMetricValues to the output stream to avoid string conversion.
   *
   * @param out the OutputStream.
   * @throws IOException
   */
  public void writeTo(OutputStream out) throws IOException {
    OutputStreamWriter osw = new OutputStreamWriter(out, StandardCharsets.UTF_8);
    osw.write("{%n");
    for (Map.Entry<Short, MetricValues> entry : _metricValues.entrySet()) {
      osw.write(String.format("metricId:\"%d\", values:\"", entry.getKey()));
      entry.getValue().writeTo(out);
      osw.write("}\"");
    }
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner("\n", "{", "}");
    for (Map.Entry<Short, MetricValues> entry : _metricValues.entrySet()) {
      joiner.add(String.format("metricId:\"%d\", values:\"%s\"", entry.getKey(), entry.getValue()));
    }
    return joiner.toString();
  }

  private Map<Short, MetricValues> metricValues() {
    return _metricValues;
  }
}
