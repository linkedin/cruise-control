/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.StringJoiner;


/**
 * A class hosting the values of a particular metric.
 */
public class MetricValues {
  // Values are sorted from the newest to the oldest -- i.e. the newest value is in index 0.
  private final float[] _values;
  private float _max;
  private double _sumForAvg;

  /**
   * Construct a MetricValues.
   *
   * @param numWindows the number of windows the metric values should contain (i.e the length of the value array).
   */
  public MetricValues(int numWindows) {
    _values = new float[numWindows];
    _sumForAvg = 0;
    _max = Float.MIN_VALUE;
  }

  /**
   * Set the value at the given index.
   * @param index the index to set the value.
   * @param value the value to use.
   */
  public void set(int index, double value) {
    if (_max == _values[index] && value < _max) {
      _max = Float.MIN_VALUE;
    }
    _sumForAvg += value - _values[index];
    _values[index] = (float) value;
  }

  /**
   * Get value at the given index.
   * We use double for calculation precision even if the stored value is float.
   *
   * @param index the index to get value from.
   * @return The value at the given index.
   */
  public double get(int index) {
    return _values[index];
  }

  /**
   * Clear the entire MetricValues.
   */
  public void clear() {
    Arrays.fill(_values, 0);
    _sumForAvg = 0;
    _max = Float.MIN_VALUE;
  }

  /**
   * The length of the value array. i.e. the number of windows kept by this MetricValues.
   * @return The length of the value array.
   */
  public int length() {
    return _values.length;
  }

  /**
   * Add a value array to the existing value array. The length of the two arrays must be the same.
   *
   * @param values the value array to add.
   */
  public void add(double[] values) {
    if (values.length != _values.length) {
      throw new IllegalArgumentException(String.format("The value array of length %d cannot be added to the "
                                                           + "MetricValue with length %d",
                                                       values.length, _values.length));
    }
    _max = Float.MIN_VALUE;
    for (int i = 0; i < _values.length; i++) {
      double toAdd = values[i];
      _values[i] += toAdd;
      _sumForAvg += toAdd;
      _max = Math.max(_max, _values[i]);
    }
  }

  /**
   * Add another MetricValue to this MetricValues. The length of the two MetricValues must be the same.
   *
   * @param metricValues the MetricValues to add.
   */
  public void add(MetricValues metricValues) {
    if (metricValues.length() != _values.length) {
      throw new IllegalArgumentException(String.format("The value array of length %d cannot be added to the "
                                                           + "MetricValue with length %d",
                                                       metricValues.length(), _values.length));
    }
    _max = Float.MIN_VALUE;
    for (int i = 0; i < _values.length; i++) {
      double toAdd = metricValues.get(i);
      _values[i] += toAdd;
      _sumForAvg += toAdd;
      _max = Math.max(_max, _values[i]);
    }
  }

  /**
   * Subtract a value array from the existing value array. The length of the two arrays must be the same.
   *
   * @param values the value array to add.
   */
  public void subtract(double[] values) {
    if (values.length != _values.length) {
      throw new IllegalArgumentException(String.format("The value array of length %d cannot be subtracted from the "
                                                           + "MetricValue with length %d",
                                                       values.length, _values.length));
    }
    _max = Float.MIN_VALUE;
    for (int i = 0; i < _values.length; i++) {
      double toDeduct = values[i];
      _values[i] -= toDeduct;
      _sumForAvg -= toDeduct;
      _max = Math.max(_max, _values[i]);
    }
  }

  /**
   * Subtract another MetricValue from this MetricValues. The length of the two MetricValues must be the same.
   *
   * @param metricValues the MetricValues to add.
   */
  public void subtract(MetricValues metricValues) {
    if (metricValues.length() != _values.length) {
      throw new IllegalArgumentException(String.format("The value array of length %d cannot be subtracted from the "
                                                           + "MetricValue with length %d",
                                                       metricValues.length(), _values.length));
    }
    _max = Float.MIN_VALUE;
    for (int i = 0; i < _values.length; i++) {
      double toDeduct = metricValues.get(i);
      _values[i] -= toDeduct;
      _sumForAvg -= toDeduct;
      _max = Math.max(_max, _values[i]);
    }
  }

  /**
   * @return The average value of all the values in this MetricValues.
   */
  public float avg() {
    return (float) (_sumForAvg / _values.length);
  }

  /**
   * @return The max value of all the values in this MetricValues.
   */
  public float max() {
    if (_max >= 0) {
      return _max;
    } else {
      return updateMax();
    }
  }

  /**
   * @return The last value of all the values in this MetricValues.
   */
  public float latest() {
    return _values[0];
  }

  /**
   * @return The value array in double precision.
   */
  public double[] doubleArray() {
    double[] result = new double[_values.length];
    for (int i = 0; i < _values.length; i++) {
      result[i] = _values[i];
    }
    return result;
  }

  /**
   * Write the MetricValues directly into a OutputStream to avoid string conversion.
   * @param out the output stream to write to.
   * @throws IOException
   */
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
    return String.format("{avg:%f, max:%f, %s}", avg(), max(), joiner);
  }

  private float updateMax() {
    _max = _values[0];
    for (int i = 1; i < _values.length; i++) {
      _max = Math.max(_max, _values[i]);
    }
    return _max;
  }

}
