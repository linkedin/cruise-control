/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.metricdef;

/**
 * The metric information including the name, id, the way of interpretation and the metric group name.
 */
public class MetricInfo {
  private final String _name;
  private final short _id;
  private final AggregationFunction _aggregationFunction;
  private final String _group;

  public MetricInfo(String name, short id, AggregationFunction aggregationFunction, String group) {
    _name = name;
    _id = id;
    _aggregationFunction = aggregationFunction;
    _group = group;
  }

  /**
   * @return The name of the metric.
   */
  public String name() {
    return _name;
  }

  /**
   * @return The id of the metric.
   */
  public short id() {
    return _id;
  }

  /**
   * @return The {@link AggregationFunction} of the metric.
   */
  public AggregationFunction aggregationFunction() {
    return _aggregationFunction;
  }

  /**
   * @return The metric group of this metric. The metric group is used to aggregate the metrics of the same kind.
   * @see MetricDef
   */
  public String group() {
    return _group;
  }

  @Override
  public String toString() {
    return String.format("(name=%s, id=%d, aggregationFunction=%s)", _name, _id, _aggregationFunction);
  }
}
