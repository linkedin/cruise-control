/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.StringJoiner;


/**
 * A class hosting the values of a particular metric.
 */
public class MetricValues implements Serializable {

  private final float[] _values;
  private volatile float _max;
  private volatile double _sumForAvg;

  public MetricValues(int numWindows) {
    _values = new float[numWindows];
    _sumForAvg = 0;
    _max = Float.MIN_VALUE;
  }

  public void set(int index, double value) {
    if (_max == _values[index] && value < _max) {
      _max = -1.0f;
    }
    _sumForAvg += value - _values[index];
    _values[index] = (float) value;
  }

  public double get(int index) {
    return _values[index];
  }

  public void clear() {
    Arrays.fill(_values, 0);
    _sumForAvg = 0;
    _max = Float.MIN_VALUE;
  }

  public int length() {
    return _values.length;
  }

  public void add(double[] values) {
    _max = Float.MIN_VALUE;
    for (int i = 0; i < _values.length; i++) {
      double toAdd = values[i];
      _values[i] += toAdd;
      _sumForAvg += toAdd;
      _max = Math.max(_max, _values[i]);
    }
  }

  public void add(MetricValues metricValues) {
    _max = Float.MIN_VALUE;
    for (int i = 0; i < _values.length; i++) {
      double toAdd = metricValues.get(i);
      _values[i] += toAdd;
      _sumForAvg += toAdd;
      _max = Math.max(_max, _values[i]);
    }
  }

  public void subtract(double[] metricValues) {
    _max = Float.MIN_VALUE;
    for (int i = 0; i < _values.length; i++) {
      double toDeduct = metricValues[i];
      _values[i] -= toDeduct;
      _sumForAvg -= toDeduct;
      _max = Math.max(_max, _values[i]);
    }
  }

  public void subtract(MetricValues metricValues) {
    _max = Float.MIN_VALUE;
    for (int i = 0; i < _values.length; i++) {
      double toDeduct = metricValues.get(i);
      _values[i] -= toDeduct;
      _sumForAvg -= toDeduct;
      _max = Math.max(_max, _values[i]);
    }
  }

  public float avg() {
    return (float) (_sumForAvg / _values.length);
  }

  public float max() {
    if (_max >= 0) {
      return _max;
    } else {
      return updateMax();
    }
  }

  public float latest() {
    return _values[0];
  }

  public double[] doubleArray() {
    double[] result = new double[_values.length];
    for (int i = 0; i < _values.length; i++) {
      result[i] = _values[i];
    }
    return result;
  }

  public void writeTo(OutputStream out) throws IOException {
    out.write(String.format("{avg:\"%.3f\", max:\"%.3f\", {", avg(), max()).getBytes(StandardCharsets.UTF_8));
    for (int i = 0; i < _values.length - 1; i++) {
      out.write((i + ":" + _values[i] + ", ").getBytes(StandardCharsets.UTF_8));
    }
    out.write(((_values.length - 1) + ":" + _values[_values.length - 1] + "}}").getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(", ", "{", "}");
    for (int i = 0; i < _values.length; i++) {
      joiner.add(i + ":" + _values[i]);
    }
    return String.format("{avg:%f, max:%f, %s}", avg(), max(), joiner.toString());
  }

  private float updateMax() {
    _max = _values[0];
    for (int i = 1; i < _values.length; i++) {
      _max = Math.max(_max, _values[i]);
    }
    return _max;
  }

}
