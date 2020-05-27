/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.model.Entity;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;


/**
 * The metric sample aggregation options.
 */
public class AggregationOptions<G, E extends Entity<G>> {
  private final double _minValidEntityRatio;
  private final double _minValidEntityGroupRatio;
  private final int _minValidWindows;
  private final int _maxAllowedExtrapolationsPerEntity;
  private final Set<E> _interestedEntities;
  private final Granularity _granularity;
  private final boolean _includeInvalidEntities;
  private final Set<G> _interestedEntityGroups;

  /**
   * Construct an AggregationOptions. The aggregation options are used to instruct {@link MetricSampleAggregator} how
   * to aggregate the metrics.
   *
   * @param minValidEntityRatio The minimum required percentage of valid entities out of all the interestedEntities.
   * @param minValidEntityGroupRatio The minimum required percentage of the valid entity groups out of all the interested
   *                               entities groups (the groups of interested entities).
   * @param minValidWindows The minimum required number of valid windows required in the result. A valid window
   *                        is a window within which both {@code Min Valid Entity Ratio} and
   *                        {@code Min Valid Entity Group Ratio} are met.
   * @param maxAllowedExtrapolationsPerEntity the maximum allowed {@link Extrapolation}s per entity in the aggregation.
   * @param interestedEntities All the entities to include in this aggregation. Sometimes not all the entities are
   *                           interested. This option allows users to aggregate only part of the entities.
   * @param granularity The granularity of the aggregation.
   * @param includeInvalidEntities Whether to include invalid entities in the aggregation result as well. The
   *                               metric values of the invalid entities are provided at the best effort. When no
   *                               metric value is available, 0 will be used.
   *
   * @see MetricSampleAggregator
   * @see MetricSampleAggregationResult
   * @see MetricSampleCompleteness
   */
  public AggregationOptions(double minValidEntityRatio,
                            double minValidEntityGroupRatio,
                            int minValidWindows,
                            int maxAllowedExtrapolationsPerEntity,
                            Set<E> interestedEntities,
                            Granularity granularity,
                            boolean includeInvalidEntities) {
    if (minValidWindows < 1) {
      throw new IllegalArgumentException("The minimum valid windows must be at least 1");
    }
    _minValidEntityRatio = minValidEntityRatio;
    _minValidEntityGroupRatio = minValidEntityGroupRatio;
    _minValidWindows = minValidWindows;
    _maxAllowedExtrapolationsPerEntity = maxAllowedExtrapolationsPerEntity;
    _interestedEntities = interestedEntities == null ? Collections.emptySet() : interestedEntities;
    _granularity = granularity == null ? Granularity.ENTITY : granularity;
    _includeInvalidEntities = includeInvalidEntities;
    _interestedEntityGroups = new HashSet<>();
    _interestedEntities.forEach(entity -> _interestedEntityGroups.add(entity.group()));
  }

  /**
   * @return The minimum required percentage of valid entities out of all the interestedEntities.
   */
  public double minValidEntityRatio() {
    return _minValidEntityRatio;
  }

  /**
   * @return The minimum required percentage of the valid entity groups of all entity groups in the interested entities.
   */
  public double minValidEntityGroupRatio() {
    return _minValidEntityGroupRatio;
  }

  /**
   * @return The minimum required number of valid windows required in the result.
   */
  public int minValidWindows() {
    return _minValidWindows;
  }

  /**
   * @return The maximum allowed extrapolations per entity.
   */
  public int maxAllowedExtrapolationsPerEntity() {
    return _maxAllowedExtrapolationsPerEntity;
  }

  /**
   * @return All the entities to include in this aggregation.
   */
  public Set<E> interestedEntities() {
    return Collections.unmodifiableSet(_interestedEntities);
  }

  /**
   * @return The granularity of the aggregation.
   */
  public Granularity granularity() {
    return _granularity;
  }

  /**
   * @return whether to include invalid entities in the aggregation result as well.
   */
  public boolean includeInvalidEntities() {
    return _includeInvalidEntities;
  }

  /**
   * @return get all the entity groups in the interested entities.
   */
  public Set<G> interestedEntityGroups() {
    return Collections.unmodifiableSet(_interestedEntityGroups);
  }

  /**
   * The granularity of the aggregation. When set to {@link Granularity#ENTITY}, if an entity group contains both
   * valid and invalid entities, the aggregation will still consider the valid entities as valid.
   * When set to {@link Granularity#ENTITY_GROUP}, when there are invalid entities in an entity group, all the
   * entities in the same entity group are considered invalid.
   */
  public enum Granularity {
    ENTITY, ENTITY_GROUP
  }

  @Override
  public int hashCode() {
    // We do not compare the full interested entities here to avoid expensive hashCode computation.
    return Objects.hash(_minValidEntityRatio, _minValidEntityGroupRatio, _minValidWindows, _granularity, _interestedEntities
        .size());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof AggregationOptions)) {
      return false;
    }
    AggregationOptions other = (AggregationOptions) obj;
    if (_minValidEntityRatio != other.minValidEntityRatio()) {
      return false;
    } else if (_minValidEntityGroupRatio != other.minValidEntityGroupRatio()) {
      return false;
    } else if (_minValidWindows != other.minValidWindows()) {
      return false;
    } else if (_granularity != other.granularity()) {
      return false;
    } else if (_maxAllowedExtrapolationsPerEntity != other.maxAllowedExtrapolationsPerEntity()) {
      return false;
    } else if (_interestedEntities.size() != other.interestedEntities().size()) {
      return false;
    } else if (!_interestedEntities.containsAll(other.interestedEntities())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return String.format("(minValidEntityRatio=%.2f, minValidEntityGroupRatio=%.2f, minValidWindows=%d, "
                             + "numEntitiesToInclude=%d, granularity=%s)", _minValidEntityRatio,
                         _minValidEntityGroupRatio,
                         _minValidWindows, _interestedEntities.size(), _granularity);
  }
}
