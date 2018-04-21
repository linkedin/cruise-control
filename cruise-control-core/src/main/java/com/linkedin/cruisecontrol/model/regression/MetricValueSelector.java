/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.regression;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;


/**
 * A class that helps select metric values for linear regression.
 *
 * Ideally we should have same number of witnessed values in each of the buckets, i.e. uniform distribution.
 * In reality, it is unlikely that the raw witnessed values will distribute evenly. So we need discard some
 * of the values if the buckets those value belongs to already have enough observations.
 *
 * We define the sufficiency of observations as the following. Given N buckets sorted by the number of values in them in
 * descending order. Users can define a number M where M < N. Assume the M(th) bucket in the above sorted bucket
 * list has X values, if a bucket has more than X values it is considered to have enough observations, and any
 * further values to that bucket should not be included into the linear regression model to avoid one bucket
 * dominating the linear regression result.
 */
public class MetricValueSelector {
  private final int _numBucketsToCapForFairness;
  private final MetricDiversity _metricDiversity;
  private final Map<Integer, Integer> _includedCountsByBuckets;
  private int _fairCountCap;

  /**
   * @param numBucketsToCapForFairness the number of buckets with most observations to ensure the fairness. This is to
   *                                   handle the case that some of the buckets has much more observations than other
   *                                   buckets. For example, if bucket 0, 1, 2, 3 has 10, 100, 1000, 200 observations
   *                                   respectively, having numBucketsToCapForFairness set to 3 will try to let the top
   *                                   3 buckets has the same number of counts. i.e. bucket 2 and bucket 3 would be
   *                                   be capped to have only 100 values for linear regression. It is possible that
   *                                   there are more values included because some other values in the same observation
   *                                   is required by some other metrics.
   */
  MetricValueSelector(int numBucketsToCapForFairness, MetricDiversity metricDiversity) {
    _numBucketsToCapForFairness = numBucketsToCapForFairness;
    _metricDiversity = metricDiversity;
    _includedCountsByBuckets = new HashMap<>();
    updateMaxSamplesToInclude();
  }

  /**
   * @return the upper bound of value counts in a bucket for fairness.
   */
  int fairCountCap() {
    return _fairCountCap;
  }

  /**
   * Check whether a value should be included into the linear regression model.
   * @param value the value to check.
   * @return true if the value is considered as included in the linear regression, false otherwise.
   */
  boolean include(double value) {
    int bucket = _metricDiversity.bucket(value);
    int countForBucket = _includedCountsByBuckets.getOrDefault(bucket, 0);
    return countForBucket < _fairCountCap;
  }

  void included(double value) {
    int bucket = _metricDiversity.bucket(value);
    _includedCountsByBuckets.compute(bucket, (b, c) -> c == null ? 1 : c + 1);
  }

  Map<Integer, Integer> includedCountsByBuckets() {
    return Collections.unmodifiableMap(_includedCountsByBuckets);
  }

  private void updateMaxSamplesToInclude() {
    Map<Integer, Integer> countsByBucket = _metricDiversity.countsByBucket();
    // Not many buckets have data.
    if (countsByBucket.size() <= _numBucketsToCapForFairness) {
      _fairCountCap = Integer.MAX_VALUE;
      return;
    }
    // In case we have huge number of buckets, only keep the buckets that are needed to the priority queue.
    PriorityQueue<Integer> counts = new PriorityQueue<>();
    countsByBucket.values().forEach(count -> {
      if (counts.size() < _numBucketsToCapForFairness + 1) {
        counts.add(count);
      } else {
        if (!counts.isEmpty() && count > counts.peek()) {
          counts.poll();
          counts.add(count);
        }
      }
    });
    _fairCountCap = counts.poll();
  }
}
