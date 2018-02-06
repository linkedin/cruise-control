/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.common.LongGenerationed;
import com.linkedin.cruisecontrol.model.Entity;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregationOptions.Granularity.ENTITY;


/**
 * A class that helps maintain the information of a window.
 */
public class WindowState<G, E extends Entity<G>> extends LongGenerationed {
  private final Set<E> _validEntities;

  public WindowState(long generation) {
    super(generation);
    _validEntities = new HashSet<>();
  }

  void addValidEntities(E entity) {
    _validEntities.add(entity);
  }

  void maybeInclude(long windowIndex,
                    MetricSampleCompleteness<G, E> completeness,
                    AggregationOptions<G, E> options) {
    Set<G> validGroupsForWindow = fillInCoverage(windowIndex, completeness, options);
    Set<E> validEntitiesForWindow =
        options.granularity() == ENTITY ? _validEntities : validEntitiesWithGroupGranularity(validGroupsForWindow);
    boolean meetEntityCoverageAfterMerge = meetEntityCoverageAfterMerge(completeness, validEntitiesForWindow, options);
    boolean meetGroupCoverageAfterMerge = meetEntityGroupCoverageAfterMerge(completeness, validGroupsForWindow, options);

    if (meetEntityCoverageAfterMerge && meetGroupCoverageAfterMerge) {
      completeness.retainAllCoveredEntities(validEntitiesForWindow);
      completeness.retainAllCoveredEntityGroups(validGroupsForWindow);
      completeness.addValidWindowIndex(windowIndex);
    }
  }

  private boolean meetEntityCoverageAfterMerge(MetricSampleCompleteness<G, E> completeness,
                                               Set<E> validEntitiesForWindow,
                                               AggregationOptions<G, E> options) {
    int totalNumEntities = options.interestedEntities().size();
    int numValidEntitiesAfterMerge = numValidElementsAfterMerge(completeness.coveredEntities(), validEntitiesForWindow);
    return (float) numValidEntitiesAfterMerge / totalNumEntities >= options.minEntityCoverage();
  }

  private boolean meetEntityGroupCoverageAfterMerge(MetricSampleCompleteness<G, E> completeness,
                                                    Set<G> validEntityGroupForWindow,
                                                    AggregationOptions<G, E> options) {
    int totalNumEntityGroups = options.entityGroups().size();
    int numValidEntityGroupsAfterMerge =
        numValidElementsAfterMerge(completeness.coveredEntityGroups(), validEntityGroupForWindow);
    return (float) numValidEntityGroupsAfterMerge / totalNumEntityGroups >= options.minEntityGroupCoverage();
  }

  private Set<G> fillInCoverage(long windowIndex,
                                MetricSampleCompleteness<G, E> completeness,
                                AggregationOptions<G, E> options) {
    int validEntities = 0;
    Map<G, Integer> numValidEntitiesInGroups = new HashMap<>();
    Set<G> invalidGroups = new HashSet<>();
    for (E entity : options.interestedEntities()) {
      if (_validEntities.contains(entity)) {
        validEntities++;
        numValidEntitiesInGroups.compute(entity.group(), (g, v) -> v == null ? 1 : v + 1);
      } else {
        invalidGroups.add(entity.group());
      }
    }
    int validEntitiesWithGroupGranularity = validEntities;
    for (G group : invalidGroups) {
      Integer count = numValidEntitiesInGroups.remove(group);
      if (count != null) {
        validEntitiesWithGroupGranularity -= count;
      }
    }
    Set<G> validGroups = numValidEntitiesInGroups.keySet();
    int totalNumEntities = options.interestedEntities().size();
    completeness.addEntityCoverage(windowIndex, (float) validEntities / totalNumEntities);
    completeness.addEntityCoverageWithGroupGranularity(windowIndex, (float) validEntitiesWithGroupGranularity / totalNumEntities);
    completeness.addEntityGroupCoverage(windowIndex, (float) validGroups.size() / options.entityGroups().size());
    return validGroups;
  }

  private int numValidElementsAfterMerge(Set<?> validElements, Set<?> validElementsToMerge) {
    int numValidElements = 0;
    for (Object entity : validElements) {
      if (validElementsToMerge.contains(entity)) {
        numValidElements++;
      }
    }
    return numValidElements;
  }

  private Set<E> validEntitiesWithGroupGranularity(Set<G> validGroups) {
    Set<E> result = new HashSet<>(_validEntities);
    result.removeIf(entity -> !validGroups.contains(entity.group()));
    return result;
  }
}
