/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.common.WindowIndexedArrays;
import com.linkedin.cruisecontrol.model.Entity;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The state of the {@link MetricSampleAggregator}.
 */
class MetricSampleAggregatorState<G, E extends Entity<G>> extends WindowIndexedArrays {
  private static final Logger LOG = LoggerFactory.getLogger(MetricSampleAggregatorState.class);
  // A map stores the cached metric sample completeness.
  private final Map<AggregationOptions<G, E>, MetricSampleCompleteness<G, E>> _completenessCache;
  // A concurrent navigable map that stores the window state in reverse-chronological order.
  private final ConcurrentNavigableMap<Long, WindowState<G, E>> _windowStates;
  // An array that stores the generation of the MetricSampleAggregator when each window was last updated.
  private final MyAtomicLong[] _windowGenerations;
  // The window size.
  private final long _windowMs;

  @Override
  protected int length() {
    return _windowGenerations.length;
  }

  /**
   * Construct the MetricSampleAggregatorState.
   */
  MetricSampleAggregatorState(int numWindows, long windowMs, int completenessCacheSize) {
    super();
    // Only keep as many as _windowStates.size() completeness caches.
    _completenessCache = new LinkedHashMap<AggregationOptions<G, E>, MetricSampleCompleteness<G, E>>() {
      @Override
      protected boolean removeEldestEntry(Map.Entry<AggregationOptions<G, E>, MetricSampleCompleteness<G, E>> eldest) {
        return this.size() > completenessCacheSize;
      }
    };
    _windowStates = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
    _windowGenerations = new MyAtomicLong[numWindows];
    for (int i = 0; i < numWindows; i++) {
      _windowGenerations[i] = new MyAtomicLong(0);
    }
    _windowMs = windowMs;
  }

  /**
   * Update the generation for a particular window.
   * This method does not grab the lock for the entire MetricSampleState as it is called very frequently,
   * and it does not roll out new windows.
   *
   * @param windowIndex the index of the window to update.
   * @param generation the new generation of the window.
   */
  void updateWindowGeneration(long windowIndex, long generation) {
    int arrayIndex = arrayIndex(windowIndex);
    // Synchronize on the window generation before updating the generation.
    synchronized (_windowGenerations[arrayIndex]) {
      if (windowIndex >= _oldestWindowIndex) {
        _windowGenerations[arrayIndex].set(generation);
        LOG.debug("Updated window {} generation to {}", windowIndex * _windowMs, generation);
      }
    }
  }

  /**
   * Update the state of a window.
   *
   * @param windowIdx the index of the window to update.
   * @param windowState the new state of the window.
   */
  synchronized void updateWindowState(long windowIdx, WindowState<G, E> windowState) {
    if (windowIdx >= _oldestWindowIndex) {
      _windowStates.put(windowIdx, windowState);
    }
  }

  /**
   * Clear the state of a given number of windows starting at the given window index.
   *
   * @param startingWindowIndex the starting index of the windows to reset.
   * @param numWindowIndexesToReset the number of windows to reset.
   */
  synchronized void resetWindowIndexes(long startingWindowIndex, int numWindowIndexesToReset) {
    if (inValidRange(startingWindowIndex)
        || inValidRange(startingWindowIndex + numWindowIndexesToReset - 1)) {
      throw new IllegalStateException("Should never reset a window index that is in the valid range");
    }
    LOG.debug("Resetting window index [{}, {}]", startingWindowIndex,
              startingWindowIndex + numWindowIndexesToReset - 1);
    // We are resetting all the data here.
    for (long wi = startingWindowIndex; wi < startingWindowIndex + numWindowIndexesToReset; wi++) {
      // It is important to synchronize on all the window generation here, so that no thread will miss the reset.
      // The assumption is that in the MetricSampleAggregator, the oldest window has been updated, and the
      // new windows have not been rolled out yet, so resetting these windows will be safe, i.e. none of the
      // threads can update these window generation or state anymore after the reset.
      int arrayIndex = arrayIndex(wi);
      synchronized (_windowGenerations[arrayIndex]) {
        _windowGenerations[arrayIndex].set(0);
        _windowStates.remove(wi);
      }
    }
  }

  /**
   * Get the list of window indexes that need to be updated based on the current generation.
   * This method also removes the windows that are older than the oldestWindowIndex from the the internal state
   * of this class.
   *
   * @param oldestWindowIndex the index of the oldest window in the MetricSampleAggregator.
   * @param currentWindowIndex the index fo the current window in the MetricSampleAggregator.
   * @return A list of window indexes that need to be updated.
   */
  synchronized List<Long> windowIndexesToUpdate(long oldestWindowIndex, long currentWindowIndex) {
    List<Long> windowIndexesToUpdate = new ArrayList<>();
    for (long windowIdx = oldestWindowIndex; windowIdx < currentWindowIndex; windowIdx++) {
      WindowState windowState = _windowStates.get(windowIdx);
      int arrayIndex = arrayIndex(windowIdx);
      if (windowState == null || _windowGenerations[arrayIndex].get() > windowState.generation()) {
        windowIndexesToUpdate.add(windowIdx);
      }
    }
    while (!_windowStates.isEmpty() && _windowStates.lastKey() < oldestWindowIndex) {
      _windowStates.remove(_windowStates.lastKey());
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
                                                           AggregationOptions<G, E> options,
                                                           long currentGeneration) {
    MetricSampleCompleteness<G, E> completeness = _completenessCache.get(options);
    if (completeness == null
        || completeness.generation() < currentGeneration
        || fromWindowIndex != completeness.firstWindowIndex()
        || toWindowIndex != completeness.lastWindowIndex()) {
      completeness = computeCompleteness(fromWindowIndex, toWindowIndex, options, currentGeneration);
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
    Map<Long, Long> windowGenerations = new TreeMap<>(Comparator.reverseOrder());
    for (long wi = _oldestWindowIndex; wi < currentWindowIndex(); wi++) {
      int arrayIndex = arrayIndex(wi);
      windowGenerations.put(wi, _windowGenerations[arrayIndex].get());
    }
    return windowGenerations;
  }

  /**
   * Clear all the states.
   */
  synchronized void clear() {
    _oldestWindowIndex = 0;
    _completenessCache.clear();
    _windowStates.clear();
    for (AtomicLong generation : _windowGenerations) {
      generation.set(0);
    }
  }

  private MetricSampleCompleteness<G, E> computeCompleteness(long fromWindowIndex,
                                                             long toWindowIndex,
                                                             AggregationOptions<G, E> options,
                                                             long currentGeneration) {
    MetricSampleCompleteness<G, E> completeness = new MetricSampleCompleteness<>(currentGeneration, _windowMs);
    Map<E, Integer> entityExtrapolations = new HashMap<>();
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
      windowState.maybeInclude(windowIdx, completeness, entityExtrapolations, options);
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

  /**
   * A fake class to make find bugs happy.
   */
  private static class MyAtomicLong extends AtomicLong {
    MyAtomicLong(long initialValue) {
      super(initialValue);
    }
  }
}
