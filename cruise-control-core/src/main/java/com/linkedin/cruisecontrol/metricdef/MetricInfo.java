/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.metricdef;

/**
 * The metric information including the name, id and the way of interpretation.
 */
public class MetricInfo {
  private final String _name;
  private final int _id;
  private final AggregationFunction _strategy;

  public MetricInfo(String name, int id, AggregationFunction strategy) {
    _name = name;
    _id = id;
    _strategy = strategy;
  }

  public String name() {
    return _name;
  }

  public int id() {
    return _id;
  }

  public AggregationFunction strategy() {
    return _strategy;
  }

  @Override
  public String toString() {
    return String.format("(name=%s, id=%d, strategy=%s)", _name, _id, _strategy);
  }
}
