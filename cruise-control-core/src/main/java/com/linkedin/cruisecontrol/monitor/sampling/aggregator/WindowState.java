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

import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregationOptions.Granularity.ENTITY_GROUP;


/**
 * A class that helps maintain the information of a window.
 */
public class WindowState<G, E extends Entity<G>> extends LongGenerationed {
  private final Set<E> _validEntities;
  private final Set<E> _extrapolatedEntities;

  public WindowState(long generation) {
    super(generation);
    _validEntities = new HashSet<>();
    _extrapolatedEntities = new HashSet<>();
  }

  void addValidEntities(E entity) {
    _validEntities.add(entity);
  }

  void addExtrapolatedEntities(E entity) {
    _extrapolatedEntities.add(entity);
  }

  void maybeInclude(long windowIndex,
                    MetricSampleCompleteness<G, E> completeness,
                    Map<E, Integer> includedEntityExtrapolations,
                    AggregationOptions<G, E> options) {
    Set<E> validEntitiesForWindow = new HashSet<>();
    Set<G> validGroupsForWindow = new HashSet<>();
    // Get all the valid groups based all the valid entities in this window.
    fillInValidRatios(windowIndex, completeness, includedEntityExtrapolations, options,
                      validEntitiesForWindow, validGroupsForWindow);
    // Depending on the aggregation granularity, get the valid entities.
    if (options.granularity() == ENTITY_GROUP) {
      validEntitiesForWindow.removeIf(e -> !validGroupsForWindow.contains(e.group()));
    }
    if (meetValidEntityRatioAfterMerge(completeness, validEntitiesForWindow, options)
        && meetValidEntityGroupRatioAfterMerge(completeness, validGroupsForWindow, options)) {
      completeness.retainAllValidEntities(validEntitiesForWindow);
      completeness.retainAllValidEntityGroups(validGroupsForWindow);
      completeness.addValidWindowIndex(windowIndex);
      validEntitiesForWindow.forEach(entity -> {
        if (_extrapolatedEntities.contains(entity)) {
          includedEntityExtrapolations.compute(entity, (e, c) -> c == null ? 1 : c + 1);
        }
      });
    }
  }

  private boolean meetValidEntityRatioAfterMerge(MetricSampleCompleteness<G, E> completeness,
                                                 Set<E> validEntitiesForWindow,
                                                 AggregationOptions<G, E> options) {
    int totalNumEntities = options.interestedEntities().size();
    int numValidEntitiesAfterMerge =
        numValidElementsAfterMerge(completeness.validEntities(), validEntitiesForWindow);
    return numValidEntitiesAfterMerge > 0 && (float) numValidEntitiesAfterMerge / totalNumEntities >= options.minValidEntityRatio();
  }

  private boolean meetValidEntityGroupRatioAfterMerge(MetricSampleCompleteness<G, E> completeness,
                                                      Set<G> validEntityGroupForWindow,
                                                      AggregationOptions<G, E> options) {
    int totalNumEntityGroups = options.interestedEntityGroups().size();
    int numValidEntityGroupsAfterMerge =
        numValidElementsAfterMerge(completeness.validEntityGroups(), validEntityGroupForWindow);
    return numValidEntityGroupsAfterMerge > 0 && (float) numValidEntityGroupsAfterMerge / totalNumEntityGroups >= options.minValidEntityGroupRatio();
  }

  /**
   * Fill in the valid entity ratio and valid entity group ratio for the given window index and aggregation option.
   *
   * Also get valid entities and groups.
   *
   * @param windowIndex Window index.
   * @param completeness Metric sample completeness.
   * @param includedExtrapolationsByEntity The number of extrapolations per corresponding entity.
   * @param options The {@link AggregationOptions}
   * @param validEntitiesForOption A set to be populated with valid entities.
   * @param validGroupsForOption A set to be populated with valid groups.
   */
  private void fillInValidRatios(long windowIndex,
                                 MetricSampleCompleteness<G, E> completeness,
                                 Map<E, Integer> includedExtrapolationsByEntity,
                                 AggregationOptions<G, E> options,
                                 Set<E> validEntitiesForOption,
                                 Set<G> validGroupsForOption) {
    int numExtrapolatedEntitiesForWindow = 0;
    Map<G, Integer> numValidEntitiesByGroupForOption = new HashMap<>();
    Set<G> invalidGroupsForOption = new HashSet<>();
    // Get the total number of valid entities and the number of entities in each group. Also find the invalid groups.
    for (E entity : options.interestedEntities()) {
      if (_validEntities.contains(entity)) {
        int extrapolationAddition = 0;
        if (_extrapolatedEntities.contains(entity)) {
          numExtrapolatedEntitiesForWindow++;
          extrapolationAddition = 1;
        }
        // Ensure this window can add more extrapolations.
        int includedExtrapolations = includedExtrapolationsByEntity.getOrDefault(entity, 0);
        if (includedExtrapolations + extrapolationAddition <= options.maxAllowedExtrapolationsPerEntity()) {
          validEntitiesForOption.add(entity);
          numValidEntitiesByGroupForOption.compute(entity.group(), (g, v) -> v == null ? 1 : v + 1);
        } else {
          invalidGroupsForOption.add(entity.group());
        }
      } else {
        invalidGroupsForOption.add(entity.group());
      }
    }
    // The valid entities at group granularity is the total number of valid entities excluding those belonging
    // to an invalid group (a group containing invalid entities).
    int validEntitiesWithGroupGranularity = validEntitiesForOption.size();
    for (G group : invalidGroupsForOption) {
      Integer count = numValidEntitiesByGroupForOption.remove(group);
      if (count != null) {
        validEntitiesWithGroupGranularity -= count;
      }
    }
    validGroupsForOption.addAll(numValidEntitiesByGroupForOption.keySet());
    int totalNumEntities = options.interestedEntities().size();
    completeness.addValidEntityRatio(windowIndex, (float) validEntitiesForOption.size() / totalNumEntities);
    completeness.addValidEntityRatioWithGroupGranularity(windowIndex, (float) validEntitiesWithGroupGranularity / totalNumEntities);
    completeness.addExtrapolationEntityRatio(windowIndex, (float) numExtrapolatedEntitiesForWindow / totalNumEntities);
    completeness.addValidEntityGroupRatio(windowIndex,
                                          (float) numValidEntitiesByGroupForOption.size() / options.interestedEntityGroups().size());
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

  private boolean canExtrapolate(E entity,
                                 Map<E, Integer> includedEntityExtrapolations,
                                 AggregationOptions<G, E> options) {
    int additionalExtrapolation = _extrapolatedEntities.contains(entity) ? 1 : 0;
    int includedExtrapolations = includedEntityExtrapolations.getOrDefault(entity, 0);
    return includedExtrapolations + additionalExtrapolation <= options.maxAllowedExtrapolationsPerEntity();
  }
}
