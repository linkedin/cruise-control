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
public class MetricSampleAggregatorState<G, E extends Entity<G>> extends LongGenerationed {
  private final Map<AggregationOptions<G, E>, MetricSampleCompleteness<G, E>> _completenessCache;
  private final SortedMap<Long, WindowState<G, E>> _windowStates;
  private final SortedMap<Long, Long> _windowGenerations;
  private final long _windowMs;

  MetricSampleAggregatorState(long generation, long windowMs) {
    super(generation);
    // Only keep 5 completeness caches.
    _completenessCache = new LinkedHashMap<AggregationOptions<G, E>, MetricSampleCompleteness<G, E>>() {
      @Override
      protected boolean removeEldestEntry(Map.Entry<AggregationOptions<G, E>, MetricSampleCompleteness<G, E>> eldest) {
        return _windowStates.size() > 5;
      }
    };
    _windowStates = new TreeMap<>(Collections.reverseOrder());
    _windowGenerations = new TreeMap<>(Collections.reverseOrder());
    _windowMs = windowMs;
  }

  synchronized void updateWindowGeneration(long windowIndex, long generation) {
    _windowGenerations.compute(windowIndex, (w, g) -> (g == null || g < generation) ? generation : g);
  }

  void updateWindowState(long windowIdx, WindowState<G, E> windowState) {
    _windowStates.put(windowIdx, windowState);
  }

  List<Long> windowIndexesToUpdate(long generation, long oldestWindowIndex, long currentWindowIndex) {
    List<Long> windowIndexesToUpdate = new ArrayList<>();
    if (this.generation() < generation) {
      for (long windowIdx = oldestWindowIndex; windowIdx < currentWindowIndex; windowIdx++) {
        WindowState windowState = _windowStates.get(windowIdx);
        Long windowGeneration = _windowGenerations.get(windowIdx);
        if (windowState == null || windowGeneration > windowState.generation()) {
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

  MetricSampleCompleteness<G, E> completeness(long fromWindowIndex,
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
      // regular query while a partial windows query is more likely an ad-hoc query.
      if (_windowStates.lastKey() == completeness.firstWindowIndex()
          && _windowStates.firstKey() == completeness.lastWindowIndex()) {
        _completenessCache.put(options, completeness);
      }
    }
    return completeness;
  }

  Map<Long, WindowState<G, E>> windowStates() {
    return _windowStates;
  }

  Map<Long, Long> windowGenerations() {
    return _windowGenerations;
  }
  
  void clear() {
    _completenessCache.clear();
    _windowStates.clear();
    _windowGenerations.clear();
  }

  private MetricSampleCompleteness<G, E> computeCompleteness(long fromWindowIndex,
                                                             long toWindowIndex,
                                                             AggregationOptions<G, E> options) {
    MetricSampleCompleteness<G, E> completeness = new MetricSampleCompleteness<>(generation(), _windowMs);
    completeness.addCoveredEntities(new HashSet<>(options.interestedEntities()));
    completeness.addCoveredEntityGroups(new HashSet<>(options.entityGroups()));

    for (Map.Entry<Long, WindowState<G, E>> entry : _windowStates.entrySet()) {
      long windowIdx = entry.getKey();
      if (windowIdx < fromWindowIndex || windowIdx > toWindowIndex) {
        continue;
      }
      WindowState<G, E> windowState = entry.getValue();
      windowState.maybeInclude(windowIdx, completeness, options);
    }
    // No window is included. We need to clear the covered entity and entity group. Otherwise we keep them.
    if (completeness.validWindowIndexes().isEmpty()) {
      completeness.retainAllCoveredEntities(Collections.emptySet());
      completeness.retainAllCoveredEntityGroups(Collections.emptySet());
    }
    completeness.setEntityCoverage((float) completeness.coveredEntities().size() / options.interestedEntities().size());
    completeness.setEntityGroupCoverage((float) completeness.coveredEntityGroups().size() / options.entityGroups().size());
    return completeness;
  }
}
