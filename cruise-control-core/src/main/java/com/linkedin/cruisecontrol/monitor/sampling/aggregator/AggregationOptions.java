/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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
  private final double _minEntityCoverage;
  private final double _minEntityGroupCoverage;
  private final int _minValidWindows;
  private final Set<E> _entitiesToInclude;
  private final Granularity _granularity;
  private final boolean _includeInvalidEntities;
  private final Set<G> _entityGroups;

  public AggregationOptions(double minEntityCoverage,
                            double minEntityGroupCoverage,
                            int minValidWindows,
                            Set<E> interestedEntities,
                            Granularity granularity) {
    this(minEntityCoverage, minEntityGroupCoverage, minValidWindows, interestedEntities, granularity, true);
  }
  
  public AggregationOptions(double minEntityCoverage, 
                            double minEntityGroupCoverage, 
                            int minValidWindows,
                            Set<E> interestedEntities,
                            Granularity granularity,
                            boolean includeInvalidEntities) {
    if (minValidWindows < 1) {
      throw new IllegalArgumentException("The minimum valid windows must be at least 1");
    }
    _minEntityCoverage = minEntityCoverage;
    _minEntityGroupCoverage = minEntityGroupCoverage;
    _minValidWindows = minValidWindows;
    _entitiesToInclude = interestedEntities == null ? Collections.emptySet() : interestedEntities;
    _granularity = granularity == null ? Granularity.ENTITY : granularity;
    _includeInvalidEntities = includeInvalidEntities;
    _entityGroups = new HashSet<>();
    _entitiesToInclude.forEach(entity -> _entityGroups.add(entity.group()));
  }

  public double minEntityCoverage() {
    return _minEntityCoverage;
  }

  public double minEntityGroupCoverage() {
    return _minEntityGroupCoverage;
  }

  public int minValidWindows() {
    return _minValidWindows;
  }

  public Set<E> interestedEntities() {
    return Collections.unmodifiableSet(_entitiesToInclude);
  }

  public Granularity granularity() {
    return _granularity;
  }
  
  public boolean includeInvalidEntities() {
    return _includeInvalidEntities;
  }
  
  public Set<G> entityGroups() {
    return Collections.unmodifiableSet(_entityGroups);
  }

  public enum Granularity {
    ENTITY, ENTITY_GROUP
  }

  @Override
  public int hashCode() {
    return Objects.hash(_minEntityCoverage, _minEntityGroupCoverage, _minValidWindows, _granularity, _entitiesToInclude.size());
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
    if (_minEntityCoverage != other.minEntityCoverage()) {
      return false;
    } else if (_minEntityGroupCoverage != other.minEntityGroupCoverage()) {
      return false;
    } else if (_minValidWindows != other.minValidWindows()) {
      return false;
    } else if (_granularity != other.granularity()) {
      return false;
    } else if (_entitiesToInclude.size() != other.interestedEntities().size()) {
      return false;
    } else if (!_entitiesToInclude.containsAll(other.interestedEntities())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return String.format("(minEntityCoverage=%f, minEntityGroupCoverage=%f, minValidWindows=%d, " 
                             + "numEntitiesToInclude=%d, granularity=%s)", _minEntityCoverage, _minEntityGroupCoverage, 
                         _minValidWindows, _entitiesToInclude.size(), _granularity);
  }
}
