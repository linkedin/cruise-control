/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.regression;

/**
 * The bucket information for a causal metric. The bucket size is dynamically adjusted according to the
 * given number of bucket and the value range.
 */
class BucketInfo {
  private final int _numBuckets;
  private double _max = Double.MIN_VALUE;
  private double _min = Double.MAX_VALUE;
  private double _bucketSize = 0;

  BucketInfo(int numBuckets) {
    _numBuckets = numBuckets;
  }

  /**
   * @return the min value.
   */
  double minValue() {
    return _min;
  }

  /**
   * @return the max value.
   */
  double maxValue() {
    return _max;
  }

  /**
   * Record the value for a causal metric.
   * @param value the causal metric value.
   * @return Whether tbe bucket info has changed or not.
   */
  boolean record(double value) {
    if (value < _min || value > _max) {
      _max = Math.max(_max, value);
      _min = Math.min(_min, value);
      _bucketSize = (_max - _min) / _numBuckets;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Get the bucket number for a value.
   * @param value the causal metric value.
   * @return the corresponding bucket of the the causal metric value.
   */
  int bucket(double value) {
    // Update max or min if needed.
    record(value);
    return _bucketSize > 0 ? Math.min((int) ((value - _min) / _bucketSize), _numBuckets - 1) : 0;
  }

  /**
   * @return the bcuket size.
   */
  double bucketSize() {
    return _bucketSize;
  }

  @Override
  public String toString() {
    return String.format("(range=[%.3f, %.3f], bucketSize=%.3f)", _min, _max, _bucketSize);
  }
}
