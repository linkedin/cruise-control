/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.common.LongGenerationed;
import com.linkedin.cruisecontrol.model.Entity;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;


/**
 * This class contains the completeness information of the {@link MetricSampleAggregatorState}.
 * The completeness information is based on a given {@link AggregationOptions}
 * <p>
 *   The completeness describes the confidence level of the metric samples that are aggregated by
 *   the {@link MetricSampleAggregator}.
 * </p>
 * <p>
 *   See method java doc for details.
 * </p>
 * @see MetricSampleAggregator
 */
public class MetricSampleCompleteness<G, E extends Entity<G>> extends LongGenerationed {
  private final SortedMap<Long, Float> _validEntityRatioByWindowIndex;
  private final SortedMap<Long, Float> _validEntityRatioWithGroupGranularityByWindowIndex;
  private final SortedMap<Long, Float> _validEntityGroupRatioByWindowIndex;
  private final SortedMap<Long, Float> _extrapolatedEntitiesByWindowIndex;
  private final SortedSet<Long> _validWindowIndices;
  private final long _windowMs;
  private final Set<E> _validEntities;
  private final Set<G> _validEntityGroups;
  private float _validEntityRatio;
  private float _validEntityGroupRatio;

  public MetricSampleCompleteness(long generation, long windowMs) {
    super(generation);
    _validEntityRatioByWindowIndex = new TreeMap<>(Collections.reverseOrder());
    _validEntityRatioWithGroupGranularityByWindowIndex = new TreeMap<>(Collections.reverseOrder());
    _validEntityGroupRatioByWindowIndex = new TreeMap<>(Collections.reverseOrder());
    _extrapolatedEntitiesByWindowIndex = new TreeMap<>(Collections.reverseOrder());
    _validWindowIndices = new TreeSet<>(Collections.reverseOrder());
    _validEntities = new HashSet<>();
    _validEntityGroups = new HashSet<>();
    _validEntityRatio = 0.0f;
    _validEntityGroupRatio = 0.0f;
    _windowMs = windowMs;
  }

  void addValidEntityRatio(long windowIndex, float validEntityRatio) {
    _validEntityRatioByWindowIndex.put(windowIndex, validEntityRatio);
  }

  void addValidEntityRatioWithGroupGranularity(long windowIndex, float validEntityRatio) {
    _validEntityRatioWithGroupGranularityByWindowIndex.put(windowIndex, validEntityRatio);
  }

  void addValidEntityGroupRatio(long windowIndex, float validEntityGroupRatio) {
    _validEntityGroupRatioByWindowIndex.put(windowIndex, validEntityGroupRatio);
  }

  void addExtrapolationEntityRatio(long windowIndex, float extrapolatedEntityRatio) {
    _extrapolatedEntitiesByWindowIndex.put(windowIndex, extrapolatedEntityRatio);
  }

  void addValidWindowIndex(long windowIndex) {
    _validWindowIndices.add(windowIndex);
  }

  void setValidEntityRatio(float validEntityRatio) {
    _validEntityRatio = validEntityRatio;
  }

  void setValidEntityGroupRatio(float validEntityGroupRatio) {
    _validEntityGroupRatio = validEntityGroupRatio;
  }

  void addValidEntities(Set<E> coveredEntities) {
    _validEntities.addAll(coveredEntities);
  }

  void addValidEntityGroups(Set<G> coveredEntityGroups) {
    _validEntityGroups.addAll(coveredEntityGroups);
  }

  void retainAllValidEntities(Set<E> coveredEntitiesToRetain) {
    _validEntities.retainAll(coveredEntitiesToRetain);
  }

  void retainAllValidEntityGroups(Set<G> coveredEntityGroupsToRetain) {
    _validEntityGroups.retainAll(coveredEntityGroupsToRetain);
  }

  /**
   * Get the valid entity ratio of independent entities for each window.
   * <p>The ratio is</p>
   * <pre>NUM_VALID_ENTITIES / NUM_ALL_ENTITIES_TO_INCLUDE</pre>
   *
   * @return The ratio of independent entities for each window.
   */
  public SortedMap<Long, Float> validEntityRatioByWindowIndex() {
    return _validEntityRatioByWindowIndex;
  }

  /**
   * Get the ratio of entities whose entity group has complete metric sample data.
   * <p>The ratio is</p>
   * <pre>NUM_ENTITIES_IN_VALID_ENTITY_GROUP / NUM_ALL_ENTITIES_TO_INCLUDE</pre>
   *
   * @return The ratio of entity groups that has complete metric sample data.
   */
  public SortedMap<Long, Float> validEntityRatioWithGroupGranularityByWindowIndex() {
    return _validEntityRatioWithGroupGranularityByWindowIndex;
  }

  /**
   * Get the ratio of the entity groups.
   * <p>The ratio is</p>
   * <pre>NUM_VALID_ENTITY_GROUPS / NUM_ALL_ENTITY_GROUPS.</pre>
   *
   * @return The ratio of entity groups by window index.
   */
  public SortedMap<Long, Float> validEntityGroupRatioByWindowIndex() {
    return _validEntityGroupRatioByWindowIndex;
  }

  /**
   * Get the number of extrapolated entities.
   * @return The number of extrapolated entities by window index.
   */
  public SortedMap<Long, Float> extrapolatedEntitiesByWindowIndex() {
    return _extrapolatedEntitiesByWindowIndex;
  }

  /**
   * Get the valid window indices. A window starts from <I>{@code (windowIndex - 1) * windowMs}</I> and ends at
   * <I>{@code windowIndex * windowMs}</I>.
   * The entity and valid entity group ratio requirements can still meet the requirement after all these windows
   * are included.
   *
   * @return A sorted set of valid window indices.
   */
  public SortedSet<Long> validWindowIndices() {
    return _validWindowIndices;
  }

  /**
   * @return The set of valid entities based on the specified granularity.
   */
  public Set<E> validEntities() {
    return Collections.unmodifiableSet(_validEntities);
  }

  /**
   * @return The set of valid entity groups.
   */
  public Set<G> validEntityGroups() {
    return Collections.unmodifiableSet(_validEntityGroups);
  }

  /**
   * @return The actual valid entity ratio after including all the {@link #validWindowIndices()}
   */
  public float validEntityRatio() {
    return _validEntityRatio;
  }

  /**
   * @return The actual valid entity group ratio after including all the {@link #validWindowIndices()}
   */
  public float validEntityGroupRatio() {
    return _validEntityGroupRatio;
  }

  /**
   * @return The first window of this completeness info.
   */
  public long firstWindowIndex() {
    // The map is in the reverse order.
    return _validEntityRatioByWindowIndex.lastKey();
  }

  /**
   * @return The last window of this completeness info.
   */
  public long lastWindowIndex() {
    // The map is in the reverse order.
    return _validEntityRatioByWindowIndex.firstKey();
  }

  /**
   * @return The window size in milliseconds.
   */
  public long windowMs() {
    return _windowMs;
  }

  @Override
  public void setGeneration(Long generation) {
    throw new RuntimeException("The generation is immutable");
  }
}
