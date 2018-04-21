/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.estimator.impl;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.model.estimator.MetricEstimator;
import com.linkedin.cruisecontrol.model.regression.LinearRegression;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.linkedin.cruisecontrol.model.estimator.impl.LinearRegressionEstimator.CAUSAL_RELATION_DEFINITION_FILE_CONFIG;


/**
 * An abstract linear regression estimator that supports heterogeneous entities. It is a wrapper around
 * {@link LinearRegressionEstimator} which is used for homogeneous entities.
 */
public abstract class HeterogeneousLinearRegressionEstimator implements MetricEstimator {
  private final ConcurrentMap<String, LinearRegressionEstimator> _estimatorByEntityType;
  private String _causalRelationDefinitionFile;

  public HeterogeneousLinearRegressionEstimator() {
    _estimatorByEntityType = new ConcurrentHashMap<>();
  }

  /**
   * @return the metric definition to use for linear regression.
   */
  protected abstract MetricDef metricDef();

  /**
   * Get the type of the entity. The entities of the same type will share the same {@link LinearRegression}s defined
   * in the causal relation.
   *
   * @return a string describing the type of the entity.
   */
  protected abstract String entityType(Entity entity);

  @Override
  public void addTrainingSamples(Collection<? extends MetricSample> trainingSamples) {
    Map<String, List<MetricSample>> metricSamplesByType = getMetricSamplesByType(trainingSamples);
    metricSamplesByType.forEach((type, samples) -> {
      LinearRegressionEstimator homogeneousEstimator =
          _estimatorByEntityType.computeIfAbsent(type, t -> createHomogeneousEstimator());
      homogeneousEstimator.addTrainingSamples(samples);
    });
  }

  @Override
  public float trainingProgress() {
    float result = 0.0f;
    for (LinearRegressionEstimator homogeneousEstimator : _estimatorByEntityType.values()) {
      result += homogeneousEstimator.trainingProgress();
    }
    return result / _estimatorByEntityType.size();
  }

  @Override
  public String trainingProgressDescription() {
    StringJoiner sj = new StringJoiner("\n");
    for (Map.Entry<String, LinearRegressionEstimator> entry : _estimatorByEntityType.entrySet()) {
      sj.add(String.format("Progress for entity type %s: %n%s",
                           entry.getKey(), entry.getValue().trainingProgressDescription()));
    }
    return sj.toString();
  }

  @Override
  public void train() {
    _estimatorByEntityType.values().forEach(LinearRegressionEstimator::train);
  }

  @Override
  public MetricValues estimate(Entity entity, MetricInfo resultantMetric, AggregatedMetricValues causalMetricValues) {
    String entityType = entityType(entity);
    LinearRegressionEstimator estimator = _estimatorByEntityType.get(entityType);
    if (estimator == null) {
      throw new IllegalStateException(String.format("Cannot estimate %s for %s because the estimator does not exist "
                                                        + "for type %s", resultantMetric.name(), entity, entityType));
    }
    return estimator.estimate(entity, resultantMetric, causalMetricValues);
  }

  @Override
  public MetricValues estimate(Entity entity,
                               MetricInfo resultantMetric,
                               AggregatedMetricValues causalMetricValues,
                               AggregatedMetricValues causalMetricValueChanges,
                               ChangeType changeType) {
    String entityType = entityType(entity);
    LinearRegressionEstimator estimator = _estimatorByEntityType.get(entityType);
    if (estimator == null) {
      throw new IllegalStateException(String.format("Cannot estimate %s for %s because the estimator does not exist "
                                                        + "for type %s", resultantMetric.name(), entity, entityType));
    }
    return estimator.estimate(entity, resultantMetric, causalMetricValues, causalMetricValueChanges, changeType);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    _causalRelationDefinitionFile = (String) configs.get(CAUSAL_RELATION_DEFINITION_FILE_CONFIG);
  }

  @Override
  public void reset() {
    _estimatorByEntityType.values().forEach(LinearRegressionEstimator::reset);
  }

  private LinearRegressionEstimator createHomogeneousEstimator() {
    Map<String, String> configs =
        Collections.singletonMap(LinearRegressionEstimator.CAUSAL_RELATION_DEFINITION_FILE_CONFIG,
                                 _causalRelationDefinitionFile);
    // create a homogeneous estimator and configure it.
    LinearRegressionEstimator homogeneousEstimator = new HomogeneousLinearRegressionEstimator(this);
    homogeneousEstimator.configure(configs);
    return homogeneousEstimator;
  }

  private Map<String, List<MetricSample>> getMetricSamplesByType(Collection<? extends MetricSample> metricSamples) {
    Map<String, List<MetricSample>> samplesByType = new HashMap<>();
    metricSamples.forEach(sample -> samplesByType.computeIfAbsent(entityType(sample.entity()), type -> new ArrayList<>())
                                                 .add(sample));
    return samplesByType;
  }

  /**
   * A static concrete class that provides the metric def of this heterogeneous linear regression.
   */
  private static class HomogeneousLinearRegressionEstimator extends LinearRegressionEstimator {
    private final HeterogeneousLinearRegressionEstimator _heterogeneousEstimator;

    private HomogeneousLinearRegressionEstimator(HeterogeneousLinearRegressionEstimator heterogeneousEstimator) {
      _heterogeneousEstimator = heterogeneousEstimator;
    }

    @Override
    protected MetricDef metricDef() {
      return _heterogeneousEstimator.metricDef();
    }

  }
}
