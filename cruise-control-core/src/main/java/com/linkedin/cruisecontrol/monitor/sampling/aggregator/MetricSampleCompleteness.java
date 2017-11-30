/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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
 */
public class MetricSampleCompleteness<G, E extends Entity<G>> extends LongGenerationed {
  private final SortedMap<Long, Float> _entityCoverageByWindowIndex;
  private final SortedMap<Long, Float> _entityCoverageWithGroupGranularityByWindowIndex;
  private final SortedMap<Long, Float> _entityGroupCoverageByWindowIndex;
  private final SortedSet<Long> _validWindowIndexes;
  private final long _windowMs;
  private final Set<E> _coveredEntities;
  private final Set<G> _coveredEntityGroups;
  private float _entityCoverage;
  private float _entityGroupCoverage;

  public MetricSampleCompleteness(long generation, long windowMs) {
    super(generation);
    _entityCoverageByWindowIndex = new TreeMap<>(Collections.reverseOrder());
    _entityCoverageWithGroupGranularityByWindowIndex = new TreeMap<>(Collections.reverseOrder());
    _entityGroupCoverageByWindowIndex = new TreeMap<>(Collections.reverseOrder());
    _validWindowIndexes = new TreeSet<>(Collections.reverseOrder());
    _coveredEntities = new HashSet<>();
    _coveredEntityGroups = new HashSet<>();
    _entityCoverage = 0.0f;
    _entityGroupCoverage = 0.0f;
    _windowMs = windowMs;
  }

  void addEntityCoverage(long windowIndex, float coverage) {
    _entityCoverageByWindowIndex.put(windowIndex, coverage);
  }

  void addEntityCoverageWithGroupGranularity(long windowIndex, float coverage) {
    _entityCoverageWithGroupGranularityByWindowIndex.put(windowIndex, coverage);
  }

  void addEntityGroupCoverage(long windowIndex, float coverage) {
    _entityGroupCoverageByWindowIndex.put(windowIndex, coverage);
  }

  void addValidWindowIndex(long windowIndex) {
    _validWindowIndexes.add(windowIndex);
  }

  void setEntityCoverage(float entityCoverage) {
    _entityCoverage = entityCoverage;
  }

  void setEntityGroupCoverage(float entityGroupCoverage) {
    _entityGroupCoverage = entityGroupCoverage;
  }

  void addCoveredEntities(Set<E> coveredEntities) {
    _coveredEntities.addAll(coveredEntities);
  }

  void addCoveredEntityGroups(Set<G> coveredEntityGroups) {
    _coveredEntityGroups.addAll(coveredEntityGroups);
  }

  void retainAllCoveredEntities(Set<E> coveredEntitiesToRetain) {
    _coveredEntities.retainAll(coveredEntitiesToRetain);
  }

  void retainAllCoveredEntityGroups(Set<G> coveredEntityGroupsToRetain) {
    _coveredEntityGroups.retainAll(coveredEntityGroupsToRetain);
  }

  /**
   * Get the coverage of independent entities for each window.
   * <p>The coverage is</p>
   * <pre>NUM_VALID_ENTITIES / NUM_ALL_ENTITIES_TO_INCLUDE</pre>
   *
   * @return The coverage of independent entities for each window.
   */
  public SortedMap<Long, Float> entityCoverageByWindowIndex() {
    return _entityCoverageByWindowIndex;
  }

  /**
   * Get the coverage of entities whose entity group has complete metric sample data.
   * <p>The coverage is</p>
   * <pre>NUM_ENTITIES_IN_VALID_ENTITY_GROUP / NUM_ALL_ENTITIES_TO_INCLUDE</pre>
   *
   * @return The coverage of entity groups that has complete metric sample data.
   */
  public SortedMap<Long, Float> entityCoverageWithGroupGranularityByWindowIndex() {
    return _entityCoverageWithGroupGranularityByWindowIndex;
  }

  /**
   * Get the coverage of the entity groups.
   * <p>The coverage is</p>
   * <pre>NUM_VALID_ENTITY_GROUPS / NUM_ALL_ENTITY_GROUPS.</pre>
   *
   * @return The coverage of entity groups.
   */
  public SortedMap<Long, Float> entityGroupCoverageByWindowIndex() {
    return _entityGroupCoverageByWindowIndex;
  }

  /**
   * Get the valid window indexes. The entity and entity group coverage requirements can still meet the requirement
   * after all these windows are included.
   * @return A sorted set of valid window indexes.
   */
  public SortedSet<Long> validWindowIndexes() {
    return _validWindowIndexes;
  }

  /**
   * @return the set of valid entities based on the specified granularity.
   */
  public Set<E> coveredEntities() {
    return _coveredEntities;
  }

  /**
   * @return the set of valid entity groups.
   */
  public Set<G> coveredEntityGroups() {
    return _coveredEntityGroups;
  }
  
  /**
   * @return the actual entity coverage after including all the {@link #validWindowIndexes()}
   */
  public float entityCoverage() {
    return _entityCoverage;
  }

  /**
   * @return the actual entity group coverage after including all the {@link #validWindowIndexes()}
   */
  public float entityGroupCoverage() {
    return _entityGroupCoverage;
  }

  /**
   * @return the first window of this completeness info.
   */
  public long firstWindowIndex() {
    // The map is in the reverse order.
    return _entityCoverageByWindowIndex.lastKey();
  }

  /**
   * @return the last window of this completeness info.
   */
  public long lastWindowIndex() {
    // The map is in the reverse order.
    return _entityCoverageByWindowIndex.firstKey();
  }

  /**
   * @return the window size in milliseconds.
   */
  public long windowMs() {
    return _windowMs;
  }

  @Override
  public void setGeneration(Long generation) {
    throw new RuntimeException("The generation is immutable");
  }
}
