/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.common.LongGenerationed;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import com.linkedin.cruisecontrol.monitor.sampling.Snapshot;
import com.linkedin.cruisecontrol.resource.Resource;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation.Imputation.AVG_ADJACENT;
import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation.Imputation.AVG_AVAILABLE;
import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation.Imputation.FORCED_INSUFFICIENT;
import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation.Imputation.NONE;
import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation.Imputation.NO_VALID_IMPUTATION;


/**
 * A class hosting all the metric samples for certain snapshot window.
 */
public class MetricsForWindow<E> extends LongGenerationed {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsForWindow.class);
  private final Map<E, E> _identityEntityMap;
  private final long _window;
  private final Map<E, AggregatedMetrics> _entityMetrics;
  private final AtomicLong _numSamples;
  private volatile WindowAggregationResult<E> _cachedAggregationResult;
  private volatile long _prevWindowGenerationForCache;
  private volatile long _nextWindowGenerationForCache;

  MetricsForWindow(Map<E, E> identityEntityMap, long window) {
    super(-1);
    _identityEntityMap = identityEntityMap;
    _window = window;
    _entityMetrics = new ConcurrentHashMap<>();
    _numSamples = new AtomicLong(0);
    _cachedAggregationResult = null;
    _prevWindowGenerationForCache = -1L;
    _nextWindowGenerationForCache = -1L;
  }

  long window() {
    return _window;
  }

  long numSamples() {
    return _numSamples.get();
  }

  Map<E, AggregatedMetrics> entityMetrics() {
    return Collections.unmodifiableMap(_entityMetrics);
  }

  /**
   * Add a sample to this window
   * @param sample the sample to add.
   */
  void addSample(MetricSample<E> sample, Long generation) {
    AggregatedMetrics aggMetrics =
        _entityMetrics.computeIfAbsent(_identityEntityMap.computeIfAbsent(sample.entity(),
                                                                          v -> v == null ? sample.entity() : v),
                                       entity -> new AggregatedMetrics());
    setGeneration(generation);
    aggMetrics.addSample(sample);
    _numSamples.incrementAndGet();
  }

  /**
   * Get the aggregated snapshot result for this window. The result is returned from the cache if the cache is valid.
   * Otherwise the result is computed.
   * @param minSamplesPerSnapshot the minimum required samples in a snapshot.
   * @param prevWindow the metrics in the previous Window
   * @param nextWindow the metrics in the next window
   * @return the aggregation result for this window.
   */
  WindowAggregationResult<E> aggregationResult(int minSamplesPerSnapshot,
                                               MetricsForWindow<E> prevWindow,
                                               MetricsForWindow<E> nextWindow,
                                               Set<E> entitiesToInclude) {
    boolean hasCache = _cachedAggregationResult != null;
    boolean thisWindowChanged = compareGeneration(_cachedAggregationResult) != 0;
    boolean hasImputation = hasCache && !_cachedAggregationResult.entityWithImputations().isEmpty();
    boolean prevWindowChanged = prevWindow != null && prevWindow.compareGeneration(_prevWindowGenerationForCache) != 0;
    boolean nextWindowChanged = nextWindow != null && nextWindow.compareGeneration(_nextWindowGenerationForCache) != 0;
    boolean adjacentWindowsChanged = prevWindowChanged || nextWindowChanged;
    if (hasCache && !thisWindowChanged && (!hasImputation || !adjacentWindowsChanged)) {
      boolean allEntitiesIncluded = _cachedAggregationResult.snapshots().keySet().containsAll(entitiesToInclude);
      if (allEntitiesIncluded) {
        return _cachedAggregationResult;
      }
    }
    _cachedAggregationResult = aggregateMetrics(minSamplesPerSnapshot, prevWindow, nextWindow, entitiesToInclude);
    return _cachedAggregationResult;
  }

  /**
   * Aggregate the metrics to a {@link WindowAggregationResult}
   */
  private WindowAggregationResult<E> aggregateMetrics(int minSamplesPerSnapshot,
                                                      MetricsForWindow<E> prevWindow,
                                                      MetricsForWindow<E> nextWindow,
                                                      Set<E> entitiesToInclude) {
    // Cache the generation before generating the result to ensure we are not going to miss changes.
    WindowAggregationResult<E> result = new WindowAggregationResult<>(_window, generation());
    _prevWindowGenerationForCache = prevWindow == null ? -1L : prevWindow.generation();
    _nextWindowGenerationForCache = nextWindow == null ? -1L : nextWindow.generation();

    // If no entities to include is provided, use available entities. The if/else block is to make findBugs happy.
    if (entitiesToInclude != null && !entitiesToInclude.isEmpty()) {
      for (E entity : entitiesToInclude) {
        AggregatedMetrics metricsForEntity = _entityMetrics.get(entity);
        SnapshotAndImputation snapshotAndImputation =
            snapshotForEntity(entity, metricsForEntity, minSamplesPerSnapshot, prevWindow, nextWindow);
        result.addSnapshot(entity, snapshotAndImputation);
      }
    } else {
      for (Map.Entry<E, AggregatedMetrics> entry : _entityMetrics.entrySet()) {
        SnapshotAndImputation snapshotAndImputation =
            snapshotForEntity(entry.getKey(), entry.getValue(), minSamplesPerSnapshot, prevWindow, nextWindow);
        result.addSnapshot(entry.getKey(), snapshotAndImputation);
      }
    }

    return result;
  }

  /**
   * This function helps find the snapshot of a partition for a given snapshotWindow. If the partition's data is not
   * sufficient in this snapshotWindow, there are a few imputations that could be taken. The actions are defined
   * in {@link SnapshotAndImputation.Imputation}.
   *
   * @param entity the entity to get snapshot.
   * @param aggMetricsForEntity the aggregated metrics for the entity.
   * @param minSamplesPerSnapshot the minimum required samples in a snapshot.
   * @param prevWindow the metrics in the previous Window
   * @param nextWindow the metrics in the next window
   * @return the snapshot of the partition for the snapshot window and the imputation made if any. If no imputation
   * was made, the imputation would be {@link SnapshotAndImputation.Imputation#NONE}.
   */
  private SnapshotAndImputation snapshotForEntity(E entity,
                                                  AggregatedMetrics aggMetricsForEntity,
                                                  int minSamplesPerSnapshot,
                                                  MetricsForWindow<E> prevWindow,
                                                  MetricsForWindow<E> nextWindow) {
    SnapshotAndImputation.Imputation imputation = NO_VALID_IMPUTATION;
    // First try use the valid snapshot.
    Snapshot snapshot = snapshotForEntity(aggMetricsForEntity, minSamplesPerSnapshot);
    if (snapshot != null) {
      imputation = NONE;
    }

    // Then try use imputation.
    if (snapshot == null) {
      // Allow imputation.
      // Check if we have some samples.
      if (aggMetricsForEntity != null && aggMetricsForEntity.numSamples() >= minSamplesPerSnapshot / 2) {
        // We have more than half of the required samples but not sufficient
        snapshot = aggMetricsForEntity.toSnapshot(_window);
        imputation = AVG_AVAILABLE;
        LOG.debug("Not enough metric samples for {} in snapshot window {}. Required {}, got {} samples. Falling back to use {}",
                  entity, _window, minSamplesPerSnapshot, aggMetricsForEntity.numSamples(), AVG_AVAILABLE);
      }

      // We do not have enough samples, fall back to use adjacent windows.
      if (snapshot == null) {
        snapshot = avgAdjacentSnapshots(entity, aggMetricsForEntity, minSamplesPerSnapshot, prevWindow, nextWindow);
        if (snapshot != null) {
          imputation = AVG_ADJACENT;
          LOG.debug("No metric samples for {} in snapshot window {}. Required {}, falling back to use {}", entity,
                    _window, minSamplesPerSnapshot, AVG_ADJACENT);
        }
      }

      // We only have partial data from this window, use it.
      if (snapshot == null && aggMetricsForEntity != null) {
        imputation = FORCED_INSUFFICIENT;
        snapshot = aggMetricsForEntity.toSnapshot(_window);
      }
    }

    // Lastly if user just wants the snapshot and we do not have any data, return a zero snapshot.
    if (snapshot == null) {
      imputation = NO_VALID_IMPUTATION;
      snapshot = new Snapshot(_window, 0.0, 0.0, 0.0, 0.0);
      LOG.debug("No metric samples for {} in snapshot window {}. Required {}. No imputation strategy works.", entity,
                _window, minSamplesPerSnapshot);
    }

    return new SnapshotAndImputation(snapshot, imputation);
  }

  /**
   * Get the snapshot for entity without imputation. Return null if the snapshot is not available.
   */
  private Snapshot snapshotForEntity(E entity, int minSamplesPerSnapshot) {
    AggregatedMetrics aggMetricsForEntity = _entityMetrics.get(entity);
    return snapshotForEntity(aggMetricsForEntity, minSamplesPerSnapshot);
  }

  /**
   * Get the snapshot for entity without imputation. Return null if the snapshot is not available.
   */
  private Snapshot snapshotForEntity(AggregatedMetrics aggMetricsForEntity, int minSamplesPerSnapshot) {
    // We have sufficient samples
    boolean goodAggMetrics = aggMetricsForEntity != null && aggMetricsForEntity.enoughSamples(minSamplesPerSnapshot);
    return goodAggMetrics ? aggMetricsForEntity.toSnapshot(_window) : null;
  }

  /**
   * Get the snapshot for entity using adjacent snapshots. Return null if the snapshot is not available.
   */
  private Snapshot avgAdjacentSnapshots(E entity,
                                        AggregatedMetrics aggregatedMetricsForEntity,
                                        int minSamplesPerSnapshot,
                                        MetricsForWindow<E> prevWindow,
                                        MetricsForWindow<E> nextWindow) {
    // The aggregated metrics for the current window should never be null. We have checked that earlier.
    Snapshot currSnapshot = aggregatedMetricsForEntity == null ? null : aggregatedMetricsForEntity.toSnapshot(_window);
    Snapshot prevSnapshot = prevWindow == null ? null : prevWindow.snapshotForEntity(entity, minSamplesPerSnapshot);
    Snapshot nextSnapshot = nextWindow == null ? null : nextWindow.snapshotForEntity(entity, minSamplesPerSnapshot);
    if (prevSnapshot != null && nextSnapshot != null) {
      return new Snapshot(_window,
                          averageNullableUtilization(Resource.CPU, prevSnapshot, currSnapshot, nextSnapshot),
                          averageNullableUtilization(Resource.NW_IN, prevSnapshot, currSnapshot, nextSnapshot),
                          averageNullableUtilization(Resource.NW_OUT, prevSnapshot, currSnapshot, nextSnapshot),
                          averageNullableUtilization(Resource.DISK, prevSnapshot, currSnapshot, nextSnapshot));
    } else {
      return null;
    }
  }

  private double averageNullableUtilization(Resource resource, Snapshot... snapshots) {
    if (snapshots.length == 0) {
      throw new IllegalArgumentException("The snapshot list cannot be empty");
    }
    double total = 0.0;
    int numNull = 0;
    for (Snapshot snapshot : snapshots) {
      if (snapshot != null) {
        total += snapshot.utilizationFor(resource);
      } else {
        numNull++;
      }
    }
    // In our case it is impossible that all the snapshots are null, so no need to worry about divided by 0.
    return total / (snapshots.length - numNull);
  }

  @Override
  public void setGeneration(Long generation) {
    // Ensure the generation is monotonically increasing.
    long currentGeneration;
    do {
      currentGeneration = generation();
    } while (currentGeneration < generation && !_generation.compareAndSet(currentGeneration, generation));
  }
}
