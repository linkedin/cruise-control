/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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
    Set<G> validGroupsForWindow = fillInValidRatios(windowIndex, completeness, options);
    Set<E> validEntitiesForWindow =
        options.granularity() == ENTITY ? _validEntities : validEntitiesWithGroupGranularity(validGroupsForWindow);

    if (meetValidEntityRatioAfterMerge(completeness, validEntitiesForWindow, options)
        && meetValidEntityGroupRatioAfterMerge(completeness, validGroupsForWindow, options)) {
      completeness.retainAllValidEntities(validEntitiesForWindow);
      completeness.retainAllValidEntityGroups(validGroupsForWindow);
      completeness.addValidWindowIndex(windowIndex);
    }
  }

  private boolean meetValidEntityRatioAfterMerge(MetricSampleCompleteness<G, E> completeness,
                                                 Set<E> validEntitiesForWindow,
                                                 AggregationOptions<G, E> options) {
    int totalNumEntities = options.interestedEntities().size();
    int numValidEntitiesAfterMerge = numValidElementsAfterMerge(completeness.validEntities(), validEntitiesForWindow);
    return (float) numValidEntitiesAfterMerge / totalNumEntities >= options.minValidEntityRatio();
  }

  private boolean meetValidEntityGroupRatioAfterMerge(MetricSampleCompleteness<G, E> completeness,
                                                      Set<G> validEntityGroupForWindow,
                                                      AggregationOptions<G, E> options) {
    int totalNumEntityGroups = options.interestedEntityGroups().size();
    int numValidEntityGroupsAfterMerge =
        numValidElementsAfterMerge(completeness.validEntityGroups(), validEntityGroupForWindow);
    return (float) numValidEntityGroupsAfterMerge / totalNumEntityGroups >= options.minValidEntityGroupRatio();
  }

  /**
   * Fill in the valid entity ratio and valid entity group ratio for the given window index and aggregation option.
   */
  private Set<G> fillInValidRatios(long windowIndex,
                                   MetricSampleCompleteness<G, E> completeness,
                                   AggregationOptions<G, E> options) {
    int numValidEntities = 0;
    Map<G, Integer> numValidEntitiesByGroup = new HashMap<>();
    Set<G> invalidGroups = new HashSet<>();
    // Get the total number of valid entities and the number of entities in each group. Also find the invalid groups.
    for (E entity : options.interestedEntities()) {
      if (_validEntities.contains(entity)) {
        numValidEntities++;
        numValidEntitiesByGroup.compute(entity.group(), (g, v) -> v == null ? 1 : v + 1);
      } else {
        invalidGroups.add(entity.group());
      }
    }
    // The valid entities at group granularity is the total number of valid entities excluding those belonging
    // to an invalid group (a group containing invalid entities).
    int validEntitiesWithGroupGranularity = numValidEntities;
    for (G group : invalidGroups) {
      Integer count = numValidEntitiesByGroup.remove(group);
      if (count != null) {
        validEntitiesWithGroupGranularity -= count;
      }
    }
    Set<G> validGroups = numValidEntitiesByGroup.keySet();
    int totalNumEntities = options.interestedEntities().size();
    completeness.addValidEntityRatio(windowIndex, (float) numValidEntities / totalNumEntities);
    completeness.addValidEntityRatioWithGroupGranularity(windowIndex, (float) validEntitiesWithGroupGranularity / totalNumEntities);
    completeness.addValidEntityGroupRatio(windowIndex, (float) validGroups.size() / options.interestedEntityGroups().size());
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
