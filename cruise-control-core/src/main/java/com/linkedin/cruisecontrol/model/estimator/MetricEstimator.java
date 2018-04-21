/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.estimator;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import java.util.Collection;


/**
 * An interface for the resultant metric estimator. The implementation should take in the provided training data
 * and estimate the resultant metrics when the causal metrics changes.
 */
public interface MetricEstimator extends CruiseControlConfigurable {

  /**
   * Provide the given {@link MetricSample MetricSamples} as training data to this estimator. The provided training
   * samples are from MetricSampler. It should contain the resultant metric that this estimator is going to estimate,
   * as well as the causal metrics related to this resultant metric.
   *
   * @param trainingSamples the {@link MetricSample MetricSamples} to train this estimator.
   */
  void addTrainingSamples(Collection<? extends MetricSample> trainingSamples);

  /**
   * Return the training progress of this estimator. The returned value should be between 0.0f and 1.0f, both inclusive.
   * If the the training is completed, 1.0f should be returned.
   *
   * @return the training progress of this estimator.
   */
  float trainingProgress();

  /**
   * Gives a string explaining the training progress in detail. This is for the users who queries the verbose state
   * of cruise control.
   *
   * @return String the detail explanation of the training progress.
   */
  String trainingProgressDescription();

  /**
   * Explicitly train the estimator with the currently available training data provided by
   * {@link #addTrainingSamples(Collection)}.
   */
  void train();

  /**
   * Given the values of the causal metrics, estimate the values of the resultant metric. The causal metrics
   * are provided in {@link AggregatedMetricValues}. It may contain multiple windows, The returned {@link MetricValues}
   * contains the estimated resultant metric in each metric window that are in the given causal metrics.
   *
   * @param entity the entity to estimate the resultant metric for.
   * @param resultantMetric the resultant metrics to estimate.
   * @param causalMetricValues the causal metrics for the resultant metric to estimate.
   * @return the estimated resultant metric values.
   */
  MetricValues estimate(Entity entity, MetricInfo resultantMetric, AggregatedMetricValues causalMetricValues);

  /**
   * Given the values of the causal metrics, and potential additions to the causal metrics, estimate the values
   * of the resultant metric. The causal metrics and potential addtions are provided in {@link AggregatedMetricValues}.
   * They may contain multiple windows, The returned {@link MetricValues} contains the estimated resultant metric
   * in each metric window that are in the given causal metrics.
   *
   * @param entity the entity to estimate the resultant metric for.
   * @param resultantMetric the resultant metrics to estimate.
   * @param causalMetricValues the causal metrics for the resultant metric to estimate.
   * @param causalMetricValueChanges the changes to the causal metric values.
   * @param changeType the action of the change, i.e. add or subtract.
   * @return the estimated resultant metric values.
   */
  MetricValues estimate(Entity entity,
                        MetricInfo resultantMetric,
                        AggregatedMetricValues causalMetricValues,
                        AggregatedMetricValues causalMetricValueChanges,
                        ChangeType changeType);

  /**
   * Reset the state of the estimator. All the previous training samples and training result should be discarded.
   */
  void reset();
  
  /**
   * <p>
   *   An enum used by {@link #estimate(Entity, MetricInfo, AggregatedMetricValues, AggregatedMetricValues, ChangeType)}
   *   which indicates whether the given causal metrics changes should added or subtracted from the given causal metric
   *   values.
   * </p>
   * <p>
   *   A simplified API would have avoided this enum, as the subtraction can be represented by negative values in the
   *   causal metric values changes. We use ChangeAction so that the caller do not need to negate the values changes
   *   when the causal metric values of an entity is already available.
   * </p>
   */
  enum ChangeType {
    ADDITION, SUBTRACTION
  }
}
