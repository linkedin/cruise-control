/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;

import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.io.IOException;
import java.io.OutputStream;

import java.io.Serializable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A class for representing load information for each resource. Each Load in a cluster must have the same number of
 * windows.
 */
public class Load implements Serializable {
  // load by their time.
  private List<Long> _windows;
  private final AggregatedMetricValues _metricValues;

  /**
   * Package constructor for load with given load properties.
   */
  public Load() {
    _windows = null;
    _metricValues = new AggregatedMetricValues();
  }

  /**
   * Get load by their window time.
   */
  public AggregatedMetricValues loadByWindows() {
    return _metricValues;
  }

  /**
   * Get the number of windows in the load.
   */
  public int numWindows() {
    return _metricValues.length();
  }

  /**
   * Get the windows list for the load.
   */
  public List<Long> windows() {
    return _windows;
  }

  /**
   * Get a single snapshot value that is representative for the given resource. The current algorithm uses
   * (1) the mean of the recent resource load for inbound network load, outbound network load, and cpu load
   * (2) the latest utilization for disk space usage.
   *
   * @param resource Resource for which the expected utilization will be provided.
   * @return A single representative utilization value on a resource.
   */
  public double expectedUtilizationFor(Resource resource) {
    if (_metricValues.isEmpty()) {
      return 0.0;
    }
    MetricValues metricValues = _metricValues.valuesFor(KafkaMetricDef.resourceToMetricId(resource));
    return resource.equals(Resource.DISK) ? metricValues.latest() : metricValues.avg();
  }

  /**
   * @return true if this load is empty, false otherwise.
   */
  boolean isEmpty() {
    return _metricValues.isEmpty();
  }

  /**
   * Overwrite the load for given resource with the given load.
   *
   * @param resource           Resource for which the load will be overwritten.
   * @param loadForResource Load for the given resource to overwrite the original load by snapshot time.
   */
  void setLoadFor(Resource resource, double[] loadForResource) {
    if (loadForResource.length != _metricValues.length()) {
      throw new IllegalArgumentException("Load to set and load for the resources must have exactly " +
                                         _metricValues.length() + " entries.");
    }

    MetricValues currentLoadForResource = _metricValues.valuesFor(KafkaMetricDef.resourceToMetricId(resource));
    for (int i = 0; i < loadForResource.length; i++) {
      currentLoadForResource.set(i, (float) loadForResource[i]);
    }
  }

  /**
   * Clear the utilization for given resource.
   *
   * @param resource Resource for which the utilization will be cleared.
   */
  void clearLoadFor(Resource resource) {
    MetricValues metricValues = _metricValues.valuesFor(KafkaMetricDef.resourceToMetricId(resource));
    if (metricValues != null) {
      metricValues.clear();
    }
  }

  /**
   * Initialize the metric values for this load. This method should only be called once for initialization.
   * This method is used for the entity load, which should be immutable for most cases.
   *
   * @param aggregatedMetricValues the metric values to set as initialization.
   * @param windows the list of windows corresponding to the metric values.
   */
  void initializeMetricValues(AggregatedMetricValues aggregatedMetricValues, List<Long> windows) {
    if (!_metricValues.isEmpty()) {
      throw new IllegalStateException("Metric values already exists, cannot set it again.");
    }
    _windows = windows;
    _metricValues.add(aggregatedMetricValues);
  }

  /**
   * Add the metric values to the existing metric values.
   * @param aggregatedMetricValues the metric values to add.
   * @param windows the windows list of the aggregated metric values.
   */
  void addMetricValues(AggregatedMetricValues aggregatedMetricValues, List<Long> windows) {
    if (_windows == null) {
      _windows = windows;
    }
    _metricValues.add(aggregatedMetricValues);
  }

  /**
   * Add the given load to this load.
   *
   * @param loadToAdd Load to add to this load.
   */
  void addLoad(Load loadToAdd) {
    _metricValues.add(loadToAdd.loadByWindows());
  }

  /**
   * Subtract the given load from this load.
   *
   * @param loadToSubtract Load to subtract from this load.
   */
  void subtractLoad(Load loadToSubtract) {
    _metricValues.subtract(loadToSubtract.loadByWindows());
  }

  /**
   * Add the given load for the given resource to this load.
   *
   * @param resource           Resource for which the given load will be added.
   * @param loadToAddByWindows Load to add to this load for the given resource.
   */
  void addLoadFor(Resource resource, double[] loadToAddByWindows) {
    if (!_metricValues.isEmpty()) {
      _metricValues.valuesFor(KafkaMetricDef.resourceToMetricId(resource)).add(loadToAddByWindows);
    }
  }

  /**
   * Subtract the given load for the given resource from this load.
   *
   * @param resource                Resource for which the given load will be subtracted.
   * @param loadToSubtractByWindows Load to subtract from this load for the given resource.
   */
  void subtractLoadFor(Resource resource, double[] loadToSubtractByWindows) {
    if (!_metricValues.isEmpty()) {
      _metricValues.valuesFor(KafkaMetricDef.resourceToMetricId(resource)).subtract(loadToSubtractByWindows);
    }
  }

  /**
   * Clear the content of the circular list for each resource.
   */
  void clearLoad() {
    _metricValues.clear();
  }

  /**
   * Get the load for the requested resource cross all the windows.
   *
   * @param resource Resource for which the load will be provided.
   * @return Load of the requested resource as a mapping from snapshot time to utilization for the given resource.
   */
  MetricValues loadFor(Resource resource) {
    MetricValues loadForResource = new MetricValues(_metricValues.length());
    loadForResource.add(_metricValues.valuesFor(KafkaMetricDef.resourceToMetricId(resource)));
    return loadForResource;
  }

  /**
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    MetricDef metricDef = KafkaMetricDef.commonMetricDef();
    Map<String, Object> loadMap = new HashMap<>();
    List<Object> metricValueList = new ArrayList<>();
    for (MetricInfo metricInfo : metricDef.all()) {
      MetricValues metricValues = _metricValues.valuesFor(metricInfo.id());
      if (metricValues != null) {
        Map<Long, Double> metricValuesMap = new HashMap<>();
        for (int i = 0; i < _windows.size(); i++) {
          metricValuesMap.put(_windows.get(i), metricValues.get(i));
        }
        metricValueList.add(metricValuesMap);
      }
    }
    loadMap.put("MetricValues", metricValueList);
    return loadMap;
  }

  /**
   * Output writing string representation of this class to the stream.
   * @param out the output stream.
   */
  public void writeTo(OutputStream out) throws IOException {
    out.write("<Load>".getBytes(StandardCharsets.UTF_8));
    _metricValues.writeTo(out);
    out.write("</Load>%n".getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Get string representation of Load in XML format.
   */
  @Override
  public String toString() {
    return "<Load>" + _metricValues.toString() + "</Load>%n";
  }
}
