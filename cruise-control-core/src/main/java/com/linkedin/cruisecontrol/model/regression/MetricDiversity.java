/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.regression;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A class that helps maintain the observed value diversity of a single type of metric.
 *
 * The diversity is defined by the diversity of witnessed values. The witnessed value range is divided into a configured
 * number of buckets. So the buckets may change if the witnessed value range changes.
 *
 * Each value is then put into their corresponding bucket.
 *
 * To throttle memory consumption, if a bucket has more than maxSamplesPerBucket values, no more value observation is
 * needed for that bucket.
 *
 * This class is not thread safe.
 */
class MetricDiversity {
  private final int _maxCountPerBucket;
  private final BucketInfo _bucketInfo;
  private final SortedMap<Integer, Integer> _countsByBucket = new TreeMap<>();

  /**
   * Construct the diversity for a metric.
   *
   * @param numBuckets the number of bucket to use for the witnessed value range of the metric.

   * @param maxCountPerBucket the maximum number of samples to keep for each bucket. Once a bucket has reached this
   *                            number, the bucket no longer need any value observations.
   */
  MetricDiversity(int numBuckets, int maxCountPerBucket) {
    _bucketInfo = new BucketInfo(numBuckets);
    _maxCountPerBucket = maxCountPerBucket;
  }

  /**
   * Update the bucket information. If a the method returns true, the caller should call {@link #clear()} to
   * discard all the diversity information and rerun all the retained values by calling record one by one.
   *
   * @param value the value of the causal metrics.
   * @return true if the bucket information is changed, false otherwise.
   */
  boolean updateBucketInfo(double value) {
    return _bucketInfo.record(value);
  }

  /**
   * Clear the diversity information.
   */
  void clear() {
    _countsByBucket.clear();
  }

  /**
   * Check whether this value can be removed from the linear regression model.
   *
   * @param value the value to check
   * @return true if the value can be removed, false otherwise.
   */
  boolean shouldRemove(double value) {
    // If the bucket already has more samples than needed.
    int countForBucket = _countsByBucket.getOrDefault(_bucketInfo.bucket(value), 0);
    return countForBucket > _maxCountPerBucket;
  }

  /**
   * Record a sample value addition to the linear regression model.
   * @param value the value added.
   * @return true if the bucket the value belongs to has reached capacity, false otherwise.
   */
  boolean addValue(double value) {
    int bucket = _bucketInfo.bucket(value);
    return _countsByBucket.compute(bucket, (b, c) -> c == null ? 1 : c + 1) > _maxCountPerBucket;
  }

  /**
   * Record a sample value removal from the linear regression model.
   * @param value the value removed.
   */
  void removeSample(double value) {
    int bucket = _bucketInfo.bucket(value);
    // the value removal Should not change the maxSamplesToInclude.
    _countsByBucket.compute(bucket, (b, c) -> c - 1 == 0 ? null : c - 1);
  }

  /**
   * Get the value counts in each bucket. The returned map only contains the bucket that has at least one
   * value. So the size of the map can also be used as the number of non-empty buckets.
   *
   * @return the counts in each bucket.
   */
  Map<Integer, Integer> countsByBucket() {
    return Collections.unmodifiableMap(_countsByBucket);
  }

  /**
   * Get the bucket of a value. This method should have change the existing bucketing.
   * @param value the value to query.
   * @return the bucket the value belongs to.
   */
  int bucket(double value) {
    if (value < _bucketInfo.minValue() || value > _bucketInfo.maxValue()) {
      throw new IllegalStateException(String.format("value %f in DiversityForMetric.bucket() should not "
                                                        + "change the bucketing %s.", value, _bucketInfo));
    }
    return _bucketInfo.bucket(value);
  }

  String diversitySummary() {
    return diversitySummary(_countsByBucket);
  }

  String diversitySummary(Map<Integer, Integer> countsPerBucket) {
    StringBuilder sb = new StringBuilder();
    sb.append("BucketInfo: ").append(_bucketInfo).append("\n");
    sb.append("CountsPerBucket:").append("\n");
    for (Map.Entry<Integer, Integer> entry : countsPerBucket.entrySet()) {
      double bucketStart = entry.getKey() * _bucketInfo.bucketSize() + _bucketInfo.minValue();
      double bucketEnd = bucketStart + _bucketInfo.bucketSize();
      sb.append(String.format("%10.2f - %10.2f: %10d%n", bucketStart, bucketEnd, entry.getValue()));
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return String.format("BucketInfo: %s, countsPerBucket: %s", _bucketInfo, _countsByBucket);
  }
}
