/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import com.linkedin.cruisecontrol.monitor.sampling.Snapshot;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is an abstract class that helps organize the metric samples.
 */
public class MetricSampleAggregator<E> {
  private static final Logger LOG = LoggerFactory.getLogger(MetricSampleAggregator.class);

  protected final int _numSnapshotWindows;
  protected final int _numSnapshotsToKeep;
  protected final long _snapshotWindowMs;
  // This is map is used to make sure we only keep one instance of each entity in the aggregator.
  protected final Map<E, E> _identityEntityMap;
  protected final ConcurrentNavigableMap<Long, MetricsForWindow<E>> _windowedAggregatedEntityMetrics;
  protected final int _minSamplesPerSnapshot;
  // A flag indicating whether we are collecting the metrics. If the flag is on, no snapshot window will be evicted from
  // the maintained windowed aggregated metrics.
  protected final AtomicInteger _snapshotCollectionInProgress;
  // A generation for the aggregate result at a particular time. Whenever the aggregation result changes, the generation
  // id is bumped. This is to identify whether a cached aggregation result is still valid or not.
  protected final AtomicLong _generation;

  protected volatile long _activeSnapshotWindow;

  /**
   * Construct the abstract metric sample aggregator.
   * @param numSnapshotWindows the number of snapshot windows needed.
   * @param snapshotWindowMs the size of each snapshot window in milliseconds
   * @param minSamplesPerSnapshot minimum samples per snapshot window.
   */
  public MetricSampleAggregator(int numSnapshotWindows,
                                long snapshotWindowMs,
                                int minSamplesPerSnapshot) {
    _windowedAggregatedEntityMetrics = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
    // We keep twice as many the snapshot windows.
    _numSnapshotWindows = numSnapshotWindows;
    _numSnapshotsToKeep = _numSnapshotWindows;
    _snapshotWindowMs = snapshotWindowMs;
    _minSamplesPerSnapshot = minSamplesPerSnapshot;
    _activeSnapshotWindow = -1L;
    _snapshotCollectionInProgress = new AtomicInteger(0);
    _identityEntityMap = new ConcurrentHashMap<>();
    _generation = new AtomicLong(0);
  }

  /**
   * Add a sample to the metric aggregator. This method is thread safe.
   *
   * @param sample The metric sample to add.
   *
   * @return true if the sample is accepted, false if the sample is ignored.
   */
  public boolean addSample(MetricSample<E> sample) {
    // Find the snapshot window
    long snapshotWindow = toSnapshotWindow(sample.sampleTime(), _snapshotWindowMs);
    MetricsForWindow<E> metricsForWindow = _windowedAggregatedEntityMetrics.get(snapshotWindow);
    if (metricsForWindow == null) {
      metricsForWindow = rollOutNewSnapshotWindow(snapshotWindow);
    }
    maybeEvictOldSnapshots();
    // If we are inserting metric samples into some past windows, invalidate the aggregation result cache and
    // bump up aggregation result generation.
    if (snapshotWindow != _activeSnapshotWindow) {
      _generation.incrementAndGet();
    }
    metricsForWindow.addSample(sample, currentGeneration());
    LOG.trace("Added sample {} to aggregated metrics window {}.", sample, metricsForWindow);
    return true;
  }

  /**
   * Get the metrics aggregation result for the windows in the given range.
   *
   * @param from start time of the range
   * @param to end time of the range.
   * @param entitiesToInclude the entities that should be included in the aggregation result. If there is no data for
   *                          the entity, an {@link SnapshotAndImputation.Imputation#NO_VALID_IMPUTATION} would be
   *                          returned with a zero snapshot for the entity.
   * @return A list of aggregation result of each window in the range.
   */
  public List<WindowAggregationResult<E>> windowAggregationResults(long from, long to, Set<E> entitiesToInclude) {
    if (entitiesToInclude == null) {
      throw new IllegalArgumentException("The entities to exclude cannot be null");
    }
    if (from > to) {
      throw new IllegalArgumentException(String.format("Invalid range [%d, %d]", from, to));
    }
    long requestedLowerBoundWindow = toSnapshotWindow(Math.max(from, 0), _snapshotWindowMs);
    long requestedUpperBoundWindow = toSnapshotWindow(Math.max(to, 0), _snapshotWindowMs) - _snapshotWindowMs;
    long actualLowerBoundWindow;
    long actualUpperBoundWindow;
    try {
      // Synchronize with addSamples() here.
      synchronized (this) {
        // Disable the snapshot window eviction to avoid inconsistency of data.
        _snapshotCollectionInProgress.incrementAndGet();
        // Have a local variable in case the active snapshot window changes.
        actualLowerBoundWindow = Math.max(firstAvailableWindow(), requestedLowerBoundWindow);
        actualUpperBoundWindow = Math.max(actualLowerBoundWindow, requestedUpperBoundWindow);
      }
      LOG.debug("Get aggregation result from {} to {} with {} entities",
                actualLowerBoundWindow, requestedUpperBoundWindow, entitiesToInclude.size());
      Map<Long, MetricsForWindow<E>> targetWindows = 
          Collections.unmodifiableMap(_windowedAggregatedEntityMetrics.subMap(actualUpperBoundWindow, true, 
                                                                              actualLowerBoundWindow, true));
      long activeWindow = _activeSnapshotWindow;

      List<WindowAggregationResult<E>> result = new ArrayList<>();
      for (long window : targetWindows.keySet()) {
        if (window != activeWindow) {
          WindowAggregationResult<E> resultForWindow = windowAggregationResult(window, entitiesToInclude);
          result.add(resultForWindow);
        }
      }
      return result;
    } finally {
      _snapshotCollectionInProgress.decrementAndGet();
    }
  }

  /**
   * Get the {@link WindowAggregationResult} of a snapshot window.
   *
   * @param snapshotWindow the snapshot window to get aggregation result.
   * @param entitiesToInclude the entities that should be included in the aggregation result. If there is no data for
   *                          the entity, an {@link SnapshotAndImputation.Imputation#NO_VALID_IMPUTATION} would be
   *                          returned with a zero snapshot for the entity.
   * @return the {@link WindowAggregationResult} for the given window.
   */
  public WindowAggregationResult<E> windowAggregationResult(long snapshotWindow, Set<E> entitiesToInclude) {
    MetricsForWindow<E> metricsForWindow = _windowedAggregatedEntityMetrics.get(snapshotWindow);
    if (metricsForWindow == null) {
      throw new IllegalArgumentException("Snapshot window " + snapshotWindow + " does not exist.");
    }
    long prevWindow = snapshotWindow - _snapshotWindowMs;
    long nextWindow = snapshotWindow + _snapshotWindowMs;
    MetricsForWindow<E> prevMetricsForWindow = _windowedAggregatedEntityMetrics.get(prevWindow);
    MetricsForWindow<E> nextMetricsForWindow = nextWindow < _activeSnapshotWindow ?
        _windowedAggregatedEntityMetrics.get(nextWindow) : null;
    return metricsForWindow.aggregationResult(_minSamplesPerSnapshot,
                                              prevMetricsForWindow,
                                              nextMetricsForWindow,
                                              entitiesToInclude);
  }

  /**
   * Get the current aggregation result generation. An aggregation result is still valid if its generation equals
   * to the current aggregation result generation.
   */
  public long currentGeneration() {
    return _generation.get();
  }

  /**
   * Get the total number of samples in the aggregator.
   */
  public int totalNumSamples() {
    int numSamples = 0;
    for (MetricsForWindow<E> metricsForWindow : _windowedAggregatedEntityMetrics.values()) {
        numSamples += metricsForWindow.numSamples();
    }
    return numSamples;
  }

  /**
   * Get the earliest snapshot window.
   */
  public Long earliestSnapshotWindow() {
    return _windowedAggregatedEntityMetrics.isEmpty() ? null : _windowedAggregatedEntityMetrics.lastKey();
  }

  /**
   * Get all the monitored snapshot windows
   */
  public List<Long> allSnapshotWindows() {
    List<Long> windows = new ArrayList<>(_windowedAggregatedEntityMetrics.size());
    windows.addAll(_windowedAggregatedEntityMetrics.keySet());
    return windows;
  }

  /**
   * Get the number of monitored snapshot windows in the metric sample aggregator.
   */
  public int numSnapshotWindows() {
    return _windowedAggregatedEntityMetrics.size();
  }

  /**
   * Get a list available monitored snapshot windows in the metric sample aggregator in the given range.
   */
  public List<Long> availableSnapshotWindows(long from, long to) {
    return new ArrayList<>(_windowedAggregatedEntityMetrics.subMap(Math.min(to, _activeSnapshotWindow), false,
                                                                   Math.max(from, firstAvailableWindow()), true)
                                                           .keySet());
  }

  /**
   * Get the available snapshot windows. The visible snapshot windows are the most recent snapshot windows
   * up to amount of num.load.snapshots.
   */
  public List<Long> availableSnapshotWindows() {
    return availableSnapshotWindows(-1, Long.MAX_VALUE);
  }

  /**
   * Clear the entire metric sample aggregator.
   */
  public void clear() {
    // Synchronize with addSample() and recentSnapshots()
    synchronized (this) {
      // Wait for the snapshot collection to finish if needed.
      while (_snapshotCollectionInProgress.get() > 0) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          //
        }
      }
      _windowedAggregatedEntityMetrics.clear();
      _activeSnapshotWindow = -1L;
    }
  }

  /**
   * The current state.  This is not synchronized and so this may show an inconsistent state.
   * @return a non-null map sorted by snapshot window.  Mutating this map will not mutate the internals of this class.
   */
  public SortedMap<Long, Map<E, Snapshot>> currentSnapshots() {
    SortedMap<Long, Map<E, Snapshot>> currentSnapshots = new TreeMap<>();
    for (Map.Entry<Long, MetricsForWindow<E>> snapshotWindowAndData : _windowedAggregatedEntityMetrics
        .entrySet()) {
      Long snapshotWindow = snapshotWindowAndData.getKey();
      Map<E, AggregatedMetrics> aggregatedMetricsMap = snapshotWindowAndData.getValue().entityMetrics();
      Map<E, Snapshot> snapshotMap = new HashMap<>(aggregatedMetricsMap.size());
      currentSnapshots.put(snapshotWindow, snapshotMap);
      for (Map.Entry<E, AggregatedMetrics> metricsForPartition : aggregatedMetricsMap.entrySet()) {
        E entity = metricsForPartition.getKey();
        Snapshot snapshot = metricsForPartition.getValue().toSnapshot(snapshotWindow);
        snapshotMap.put(entity, snapshot);
      }
    }

    return currentSnapshots;
  }

  private MetricsForWindow<E> rollOutNewSnapshotWindow(long window) {
    MetricsForWindow<E> metricsForWindow;
    // The synchronization is needed so we don't remove a snapshot that is being collected.
    synchronized (this) {
      // This is the first sample of the snapshot window, so we should create one.
      metricsForWindow = new MetricsForWindow<>(_identityEntityMap, window);
      MetricsForWindow<E> oldValue =
          _windowedAggregatedEntityMetrics.putIfAbsent(window, metricsForWindow);
      // Ok... someone has created one for us, use it.
      if (oldValue != null) {
        metricsForWindow = oldValue;
      }

      if (_activeSnapshotWindow < window && oldValue == null) {
        LOG.info("Rolled out new snapshot window {}, number of snapshots = {}", metricsForWindow,
                 _windowedAggregatedEntityMetrics.size());
        _activeSnapshotWindow = window;
        _generation.incrementAndGet();
      }
    }
    return metricsForWindow;
  }

  private void maybeEvictOldSnapshots() {
    if (_windowedAggregatedEntityMetrics.size() > _numSnapshotsToKeep + 1) {
      synchronized (this) {
        // We only keep N ready snapshots and one active snapshot, evict the old ones if needed. But do not do this
        // when snapshot collection is in progress. We can do it later.
        while (_snapshotCollectionInProgress.get() == 0
            && _windowedAggregatedEntityMetrics.size() > _numSnapshotsToKeep + 1) {
          Long oldestSnapshotWindow = _windowedAggregatedEntityMetrics.lastKey();
          _windowedAggregatedEntityMetrics.remove(oldestSnapshotWindow);
          LOG.info("Removed snapshot window {}, number of snapshots = {}", oldestSnapshotWindow,
                   _windowedAggregatedEntityMetrics.size());
        }
      }
    }
  }

  private long firstAvailableWindow() {
    int i = 0;
    long firstAvailableWindow = -1L;
    for (long window : _windowedAggregatedEntityMetrics.keySet()) {
      if (window != _activeSnapshotWindow) {
        i++;
        firstAvailableWindow = window;
      }
      if (i == _numSnapshotWindows) {
        break;
      }
    }
    return firstAvailableWindow;
  }

  /**
   * Convert from the timestamp to corresponding snapshot window. A snapshot window is identified by its cut off time.
   * i.e. snapshot window <tt>W</tt> means the snapshot of time range [W - T, W], where <tt>T</tt> is the snapshot
   * window size in milliseconds.
   */
  private static long toSnapshotWindow(long time, long snapshotWindowMs) {
    return (time / snapshotWindowMs + 1) * snapshotWindowMs;
  }
}
