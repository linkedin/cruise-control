/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.common.LongGenerationed;
import com.linkedin.cruisecontrol.model.Entity;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * The aggregation result of {@link MetricSampleAggregator#aggregate(long, long, AggregationOptions)}.
 *
 * <p>
 *   In the aggregation result, each entity will be represented with a {@link ValuesAndExtrapolations}. It contains
 *   the values of each metric in each window. For memory efficiency the metric values are stored in a two-dimensional
 *   array. To get the window associated with each value, users may use the time window array returned by
 *   {@link ValuesAndExtrapolations#windows()}, or call {@link ValuesAndExtrapolations#window(int)} to get the time window
 *   in milliseconds for the index.
 * </p>
 *
 * @param <G> The entity group class. Note that the entity group will be used as a key to HashMaps, so it must have
 *           a valid {@link Object#hashCode()} and {@link Object#equals(Object)} implementation.
 * @param <E> The entity class. Note that the entity will be used as a key to HashMaps, so it must have
 *           a valid {@link Object#hashCode()} and {@link Object#equals(Object)} implementation.
 */
public class MetricSampleAggregationResult<G, E extends Entity<G>> extends LongGenerationed {
  private final Map<E, ValuesAndExtrapolations> _entityValuesAndExtrapolations;
  private final Set<E> _invalidEntities;
  private final MetricSampleCompleteness<G, E> _completeness;

  public MetricSampleAggregationResult(long generation, MetricSampleCompleteness<G, E> completeness) {
    super(generation);
    _entityValuesAndExtrapolations = new HashMap<>();
    _invalidEntities = new HashSet<>();
    _completeness = completeness;
  }

  /**
   * Get the aggregated metric values and extrapolations (if any) of each entity.
   *
   * @return A mapping from entity to aggregated metric values and potential extrapolations.
   */
  public Map<E, ValuesAndExtrapolations> valuesAndExtrapolations() {
    return Collections.unmodifiableMap(_entityValuesAndExtrapolations);
  }

  /**
   * Get the entities that are not valid. The aggregation result contains all the entities that were specified in
   * the {@link AggregationOptions} when {@link MetricSampleAggregator#aggregate(long, long, AggregationOptions)}
   * is invoked. Some of those entities may not be valid (see {@link MetricSampleAggregator}). Those entities
   * are considered as invalid entities.
   *
   * <p>
   *   The invalid entities returned by this method is not a complementary set of the entities returned by
   *   {@link MetricSampleCompleteness#validEntities()}. The covered entities in {@link MetricSampleCompleteness}
   *   are the entities that meet the completeness requirement in {@link AggregationOptions}. It is possible for
   *   an entity to be valid but excluded from the {@link MetricSampleCompleteness#validEntities()}. For example,
   * </p>
   * <p>
   *   If the {@link AggregationOptions} specifies aggregation granularity to be
   *   {@link AggregationOptions.Granularity#ENTITY_GROUP} and an {@code entity} belongs to a group which has
   *   other invalid entities. In this case, {@code entity} itself is still a valid entity therefore it will
   *   not be in the set returned by this method. But since the entity group does not meet the completeness
   *   requirement, the entire entity group is not considered as <i>"covered"</i>. So {@code entity} will not
   *   be included in the {@link MetricSampleCompleteness#validEntities()} either.
   * </p>
   *
   * @return The invalid entity set for this aggregation.
   */
  public Set<E> invalidEntities() {
    return Collections.unmodifiableSet(_invalidEntities);
  }

  /**
   * @return The valid entity ratio of the underlying {@link #_completeness}.
   */
  public float validEntityRatioOfCompleteness() {
    return _completeness.validEntityRatio();
  }

  // package private for modification.
  void addResult(E entity, ValuesAndExtrapolations valuesAndExtrapolations) {
    _entityValuesAndExtrapolations.put(entity, valuesAndExtrapolations);
  }

  void recordInvalidEntity(E entity) {
    _invalidEntities.add(entity);
  }

  @Override
  public void setGeneration(Long generation) {
    throw new RuntimeException("The generation of the MetricSampleAggregationResult is immutable.");
  }
}
