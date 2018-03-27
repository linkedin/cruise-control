/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.common.LongGenerationed;
import com.linkedin.cruisecontrol.model.Entity;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;


/**
 * The state of the {@link MetricSampleAggregator}.
 */
class MetricSampleAggregatorState<G, E extends Entity<G>> extends LongGenerationed {
  private final Map<AggregationOptions<G, E>, MetricSampleCompleteness<G, E>> _completenessCache;
  private final SortedMap<Long, WindowState<G, E>> _windowStates;
  private final SortedMap<Long, Long> _windowGenerations;
  private final long _windowMs;

  MetricSampleAggregatorState(long generation, long windowMs, int completenessCacheSize) {
    super(generation);
    // Only keep as many as _windowStates.size() completeness caches.
    _completenessCache = new LinkedHashMap<AggregationOptions<G, E>, MetricSampleCompleteness<G, E>>() {
      @Override
      protected boolean removeEldestEntry(Map.Entry<AggregationOptions<G, E>, MetricSampleCompleteness<G, E>> eldest) {
        return _windowStates.size() > completenessCacheSize;
      }
    };
    _windowStates = new TreeMap<>(Collections.reverseOrder());
    _windowGenerations = new TreeMap<>(Collections.reverseOrder());
    _windowMs = windowMs;
  }

  /**
   * Update the generation for a particular window.
   * @param windowIndex the index of the window to update.
   * @param generation the new generation of the window.
   */
  synchronized void updateWindowGeneration(long windowIndex, long generation) {
    _windowGenerations.compute(windowIndex, (w, g) -> (g == null || g < generation) ? generation : g);
  }

  /**
   * Update the state of a window.
   * @param windowIdx the index of the window to update.
   * @param windowState the new state of the window.
   */
  synchronized void updateWindowState(long windowIdx, WindowState<G, E> windowState) {
    _windowStates.put(windowIdx, windowState);
  }

  /**
   * Get the list of window indexes that need to be updated based on the current generation.
   * This method also removes the windows that are older than the oldestWindowIndex from the the internal state
   * of this class.
   *
   * @param generation the current generation of the MetricSampleAggregator.
   * @param oldestWindowIndex the index of the oldest window in the MetricSampleAggregator.
   * @param currentWindowIndex the index fo the current window in the MetricSampleAggregator.
   * @return A list of window indexes that need to be updated.
   */
  synchronized List<Long> windowIndexesToUpdate(long generation, long oldestWindowIndex, long currentWindowIndex) {
    List<Long> windowIndexesToUpdate = new ArrayList<>();
    if (this.generation() < generation) {
      for (long windowIdx = oldestWindowIndex; windowIdx < currentWindowIndex; windowIdx++) {
        WindowState windowState = _windowStates.get(windowIdx);
        Long windowGeneration = _windowGenerations.get(windowIdx);
        if (windowState == null || windowGeneration == null || windowGeneration > windowState.generation()) {
          windowIndexesToUpdate.add(windowIdx);
        }
      }
    }
    // remove old keys.
    while (!_windowStates.isEmpty() && _windowStates.lastKey() < oldestWindowIndex) {
      _windowStates.remove(_windowStates.lastKey());
    }
    while (!_windowGenerations.isEmpty() && _windowGenerations.lastKey() < oldestWindowIndex) {
      _windowGenerations.remove(_windowGenerations.lastKey());
    }
    return windowIndexesToUpdate;
  }

  /**
   * Get the completeness of the MetricSampleAggregator based on the given {@link AggregationOptions} for
   * a given time range.
   *
   * @param fromWindowIndex the index of the starting window (inclusive)
   * @param toWindowIndex the index of the end window (inclusive)
   * @param options the {@link AggregationOptions}
   * @return the {@link MetricSampleCompleteness} for the given parameters.
   */
  synchronized MetricSampleCompleteness<G, E> completeness(long fromWindowIndex,
                                                           long toWindowIndex,
                                                           AggregationOptions<G, E> options) {
    MetricSampleCompleteness<G, E> completeness = _completenessCache.get(options);
    if (completeness == null
        || completeness.generation() < generation()
        || fromWindowIndex != completeness.firstWindowIndex()
        || toWindowIndex != completeness.lastWindowIndex()) {
      completeness = computeCompleteness(fromWindowIndex, toWindowIndex, options);
      // We only cache the completeness if the completeness covers all the windows.
      // This is because in most cases, the completeness covering all the windows are more likely to be a
      // repeating query while a partial windows query is more likely an ad-hoc query.
      // Note that _windowStates is in reverse order.
      if (_windowStates.lastKey() == completeness.firstWindowIndex()
          && _windowStates.firstKey() == completeness.lastWindowIndex()) {
        _completenessCache.put(options, completeness);
      }
    }
    return completeness;
  }

  /**
   * @return The state of all the windows.
   */
  synchronized Map<Long, WindowState<G, E>> windowStates() {
    return _windowStates;
  }

  /**
   * @return the generation of all the windows.
   */
  synchronized Map<Long, Long> windowGenerations() {
    return _windowGenerations;
  }

  /**
   * Clear all the states.
   */
  synchronized void clear() {
    _completenessCache.clear();
    _windowStates.clear();
    _windowGenerations.clear();
  }

  private MetricSampleCompleteness<G, E> computeCompleteness(long fromWindowIndex,
                                                             long toWindowIndex,
                                                             AggregationOptions<G, E> options) {
    MetricSampleCompleteness<G, E> completeness = new MetricSampleCompleteness<>(generation(), _windowMs);
    completeness.addValidEntities(new HashSet<>(options.interestedEntities()));
    completeness.addValidEntityGroups(new HashSet<>(options.interestedEntityGroups()));

    for (Map.Entry<Long, WindowState<G, E>> entry : _windowStates.entrySet()) {
      long windowIdx = entry.getKey();
      if (windowIdx > toWindowIndex) {
        continue;
      } else if (windowIdx < fromWindowIndex) {
        break;
      }
      WindowState<G, E> windowState = entry.getValue();
      windowState.maybeInclude(windowIdx, completeness, options);
    }
    // No window is included. We need to clear the valid entity and entity group. Otherwise we keep them.
    if (completeness.validWindowIndexes().isEmpty()) {
      completeness.retainAllValidEntities(Collections.emptySet());
      completeness.retainAllValidEntityGroups(Collections.emptySet());
    }
    completeness.setValidEntityRatio((float) completeness.validEntities().size() / options.interestedEntities().size());
    completeness.setValidEntityGroupRatio((float) completeness.validEntityGroups().size() / options.interestedEntityGroups().size());
    return completeness;
  }
}
