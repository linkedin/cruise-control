/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.common.LongGenerationed;
import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Another metric sample aggregator which has less memory consumption.
 *
 * <p>This class is thread safe.</p>
 *
 * @param <G> The entity group class. Note that the entity group will be used as a key to HashMaps, so it must have
 *           a valid {@link Object#hashCode()} and {@link Object#equals(Object)} implementation.
 * @param <E> The entity class. Note that the entity will be used as a key to HashMaps, so it must have
 *           a valid {@link Object#hashCode()} and {@link Object#equals(Object)} implementation.
 */
public class MetricSampleAggregator<G, E extends Entity<G>> extends LongGenerationed {
  private static final Logger LOG = LoggerFactory.getLogger(MetricSampleAggregator.class);

  private final ConcurrentMap<E, RawMetricValues> _rawMetrics;
  private final MetricSampleAggregatorState<G, E> _aggregatorState;
  private final ReentrantLock _windowRollingLock;

  protected final ConcurrentMap<E, E> _identityEntityMap;
  protected final int _numWindows;
  protected final int _minSamplesPerWindow;
  protected final int _numWindowsToKeep;
  protected final long _windowMs;
  protected final int _maxAllowedImputations;
  protected final MetricDef _metricDef;
  
  private volatile long _currentWindowIndex;
  private volatile long _oldestWindowIndex;

  /**
   * Construct the abstract metric sample aggregator.
   *
   * @param numWindows the number of snapshot windows needed.
   * @param windowMs the size of each snapshot window in milliseconds
   * @param minSamplesPerWindow minimum samples per snapshot window.
   * @param maxAllowedImputations the maximum allowed number of imputations for an entity if some windows miss data.
   * @param metricDef metric definitions.
   */
  public MetricSampleAggregator(int numWindows,
                                long windowMs,
                                int minSamplesPerWindow,
                                int maxAllowedImputations,
                                MetricDef metricDef) {
    super(0);
    _identityEntityMap = new ConcurrentHashMap<>();
    _rawMetrics = new ConcurrentHashMap<>();
    _numWindows = numWindows;
    _windowMs = windowMs;
    // We keep one more window for the active window.
    _numWindowsToKeep = _numWindows + 1;
    _minSamplesPerWindow = minSamplesPerWindow;
    _windowRollingLock = new ReentrantLock();
    _maxAllowedImputations = maxAllowedImputations;
    _metricDef = metricDef;
    _aggregatorState = new MetricSampleAggregatorState<>(_generation.get(), _windowMs);
    _oldestWindowIndex = 0L;
    _currentWindowIndex = 0L;
  }

  /**
   * Add a sample to the metric aggregator.
   *
   * @param sample The metric sample to add.
   *
   * @return true if the sample is accepted, false if the sample is ignored.
   */
  public boolean addSample(MetricSample<G, E> sample) {
    if (!sample.isValid(_metricDef)) {
      LOG.warn("The metric sample is discarded due to missing metrics. Sample: {}", sample);
      return false;
    }
    long windowIndex = windowIndex(sample.sampleTime());
    // Skip the samples that are too old.
    if (windowIndex < _oldestWindowIndex) {
      return false;
    }
    boolean newWindowRolledOut = rollOutNewWindowIfNeeded(windowIndex);
    RawMetricValues rawMetricValues =
        _rawMetrics.computeIfAbsent(identity(sample.entity()), k -> {
          RawMetricValues rawValues = new RawMetricValues(_numWindowsToKeep, _minSamplesPerWindow);
          rawValues.updateOldestWindowIndex(_oldestWindowIndex);
          return rawValues;
        });
    LOG.trace("Adding sample {} to window index {}", sample, windowIndex);
    rawMetricValues.addSample(sample, windowIndex, _metricDef);
    if (newWindowRolledOut || windowIndex != _currentWindowIndex) {
      long generation = _generation.incrementAndGet();
      // Data has been inserted to an old window.
      if (windowIndex != _currentWindowIndex) {
        _aggregatorState.updateWindowGeneration(windowIndex, generation);
      }
    }
    return true;
  }

  /**
   * Aggregate the metric samples in the given period into a {@link MetricSampleAggregationResult} based on the
   * specified {@link AggregationOptions}.
   *
   * <p>
   *   The aggregation result contains all the entities in {@link AggregationOptions#interestedEntities()}.
   *   For the entities that are completely missing, an empty result is added with all the value set to 0.0 and
   *   all the window indexes marked as {@link Imputation#NO_VALID_IMPUTATION}.
   * </p>
   *
   * @param from the starting timestamp of the aggregation period in milliseconds.
   * @param to the end timestamp of the aggregation period in milliseconds.
   * @param options the {@link AggregationOptions} used to perform the aggregation.
   * @return An {@link MetricSampleAggregationResult} which contains the
   * @throws NotEnoughValidWindowsException
   */
  public MetricSampleAggregationResult<G, E> aggregate(long from, long to, AggregationOptions<G, E> options)
      throws NotEnoughValidWindowsException {
    // prevent window rolling.
    _windowRollingLock.lock();
    try {
      // Ensure the range is valid.
      long fromWindowIndex = Math.max(windowIndex(from), _oldestWindowIndex);
      long toWindowIndex = Math.min(windowIndex(to), _currentWindowIndex - 1);
      if (fromWindowIndex > _currentWindowIndex || toWindowIndex < _oldestWindowIndex) {
        throw new NotEnoughValidWindowsException(String.format("There is no window available in range [%d, %d]",
                                                               from, to));
      }

      // Get and verify the completeness.
      updateAggregatorStateIfNeeded();
      AggregationOptions<G, E> interpretedOptions = interpretAggregationOptions(options);
      MetricSampleCompleteness<G, E> completeness =
          _aggregatorState.completeness(fromWindowIndex, toWindowIndex, interpretedOptions);
      // We use the original time from and to here because they are only for logging purpose.
      validateCompleteness(from, to, completeness, options);

      // Perform the aggregation.
      List<Long> windows = toWindows(completeness.validWindowIndexes());
      MetricSampleAggregationResult<G, E> result = new MetricSampleAggregationResult<>(generation(), completeness);
      Set<E> entitiesToInclude = 
          options.includeInvalidEntities() ? interpretedOptions.interestedEntities() : completeness.coveredEntities();
      for (E entity : entitiesToInclude) {
        RawMetricValues rawValues = _rawMetrics.get(entity);
        if (rawValues == null) {
          ValuesAndImputations valuesAndImputations = ValuesAndImputations.empty(completeness.validWindowIndexes().size(), _metricDef);
          valuesAndImputations.setWindows(windows);
          result.addResult(entity, valuesAndImputations);
          result.recordInvalidEntity(entity);
        } else {
          ValuesAndImputations valuesAndImputations = rawValues.aggregate(completeness.validWindowIndexes(), _metricDef);
          valuesAndImputations.setWindows(windows);
          result.addResult(entity, valuesAndImputations);
          if (!rawValues.isValid(_maxAllowedImputations)) {
            result.recordInvalidEntity(entity);
          }
        }
      }
      return result;
    } finally {
      _windowRollingLock.unlock();
    }
  }

  public MetricSampleCompleteness<G, E> completeness(long from, long to, AggregationOptions<G, E> options) {
    _windowRollingLock.lock();
    try {
      long fromWindowIndex = Math.max(windowIndex(from), _oldestWindowIndex);
      long toWindowIndex = Math.min(windowIndex(to), _currentWindowIndex - 1);
      if (fromWindowIndex > _currentWindowIndex || toWindowIndex < _oldestWindowIndex) {
        return new MetricSampleCompleteness<>(generation(), _windowMs);
      }
      updateAggregatorStateIfNeeded();
      return _aggregatorState.completeness(windowIndex(from), windowIndex(to), interpretAggregationOptions(options));
    } finally {
      _windowRollingLock.unlock();
    }
  }

  public List<Long> availableWindows() {
    return getWindowList(_oldestWindowIndex, _currentWindowIndex - 1);
  }
  
  public int numAvailableWindows() {
    return numAvailableWindows(-1, Long.MAX_VALUE);
  }

  public int numAvailableWindows(long from, long to) {
    long fromWindowIndex = Math.max(windowIndex(from), _oldestWindowIndex);
    long toWindowIndex = Math.min(windowIndex(to), _currentWindowIndex - 1);
    return Math.max(0, (int) (toWindowIndex - fromWindowIndex + 1));
  }

  public List<Long> allWindows() {
    return getWindowList(_oldestWindowIndex, _currentWindowIndex);
  }

  public Long earliestWindow() {
    return _rawMetrics.isEmpty() ? null : _oldestWindowIndex * _windowMs;
  }

  public int numSamples() {
    int count = 0;
    for (RawMetricValues rawValues : _rawMetrics.values()) {
      count += rawValues.numSamples();
    }
    return count;
  }

  public void retainEntities(Set<E> entities) {
    _rawMetrics.entrySet().removeIf(entry -> !entities.contains(entry.getKey()));
    _generation.incrementAndGet();
  }

  public void removeEntities(Set<E> entities) {
    _rawMetrics.entrySet().removeIf(entry -> entities.contains(entry.getKey()));
    _generation.incrementAndGet();
  }

  public void retainEntityGroup(Set<G> entityGroups) {
    _rawMetrics.entrySet().removeIf(entry -> !entityGroups.contains(entry.getKey().group()));
    _generation.incrementAndGet();
  }

  public void removeEntityGroup(Set<G> entityGroups) {
    _rawMetrics.entrySet().removeIf(entry -> entityGroups.contains(entry.getKey().group()));
    _generation.incrementAndGet();
  }
  
  public void clear() {
    _windowRollingLock.lock();
    try {
      _rawMetrics.clear();
      _aggregatorState.clear();
      _generation.incrementAndGet();
    } finally {
      _windowRollingLock.unlock();
    }
  }

  MetricSampleAggregatorState<G, E> aggregatorState() {
    updateAggregatorStateIfNeeded();
    return _aggregatorState;
  }

  private List<Long> getWindowList(long fromWindowIndex, long toWindowIndex) {
    _windowRollingLock.lock();
    try {
      if (_rawMetrics.isEmpty()) {
        return Collections.emptyList();
      }
      List<Long> windows = new ArrayList<>((int) (toWindowIndex - fromWindowIndex + 1));
      for (long i = fromWindowIndex; i <= toWindowIndex; i++) {
        windows.add(i * _windowMs);
      }
      return windows;
    } finally {
      _windowRollingLock.unlock();
    }
  }

  private void updateAggregatorStateIfNeeded() {
    long currentGeneration = generation();
    if (_aggregatorState.generation() < currentGeneration) {
      for (long windowIdx : _aggregatorState.windowIndexesToUpdate(currentGeneration, _oldestWindowIndex, _currentWindowIndex)) {
        _aggregatorState.updateWindowState(windowIdx, getWindowState(windowIdx, currentGeneration));
      }
      _aggregatorState.setGeneration(currentGeneration);
    }
  }

  private WindowState<G, E> getWindowState(long windowIdx, long currentGeneration) {
    WindowState<G, E> windowState = new WindowState<>(currentGeneration);
    for (Map.Entry<E, RawMetricValues> entry : _rawMetrics.entrySet()) {
      E entity = entry.getKey();
      RawMetricValues rawValues = entry.getValue();
      if (rawValues.isValidAtWindowIndex(windowIdx) && rawValues.numImputations() <= _maxAllowedImputations) {
        windowState.addValidEntities(entity);
      }
    }
    return windowState;
  }

  private boolean rollOutNewWindowIfNeeded(long index) {
    if (_currentWindowIndex < index) {
      _windowRollingLock.lock();
      try {
        if (_currentWindowIndex < index) {
          // find out how many windows we need to reset in the raw metrics.
          int numWindowsToRollOut = (int) (index - _currentWindowIndex);
          // First set the oldest window index so no newly coming older samples will be added.
          // The first valid window is actually 1 instead of 0.
          long prevOldestWindowIndex = _oldestWindowIndex;
          _oldestWindowIndex = Math.max(1, index - _numWindows);
          int numOldWindowIndexesToReset = (int) Math.min(_numWindowsToKeep, _oldestWindowIndex - prevOldestWindowIndex);
          // Reset all the data. After this point no old samples cannot get into the raw metric values.
          // We only need to reset the index if we the new index is at least _numWindows;
          if (numOldWindowIndexesToReset > 0) {
            resetIndexes(prevOldestWindowIndex, numOldWindowIndexesToReset);
          }
          // Set the generation of the old current window.
          _aggregatorState.updateWindowGeneration(_currentWindowIndex, generation());
          // Lastly update current window.
          _currentWindowIndex = index;
          LOG.info("Rolled out {} new windows, current window range [{}, {}]",
                   numWindowsToRollOut, _oldestWindowIndex * _windowMs, _currentWindowIndex * _windowMs);
          return true;
        }
      } finally {
        _windowRollingLock.unlock();
      }
    }
    return false;
  }

  private void resetIndexes(long startingWindowIndex, int numIndexesToReset) {
    for (RawMetricValues rawValues : _rawMetrics.values()) {
      rawValues.updateOldestWindowIndex(startingWindowIndex + numIndexesToReset);
      rawValues.resetWindowIndexes(startingWindowIndex, numIndexesToReset);
    }
  }

  private void validateCompleteness(long from,
                                    long to,
                                    MetricSampleCompleteness completeness,
                                    AggregationOptions<G, E> options)
      throws NotEnoughValidWindowsException {
    if (completeness.validWindowIndexes().size() < options.minValidWindows()) {
      throw new NotEnoughValidWindowsException(String.format("There are only %d valid windows "
                                                                 + "when aggregating in range [%d, %d] for aggregation options %s",
                                                             completeness.validWindowIndexes().size(), from, to, options));
    }
    if (completeness.entityCoverage() < options.minEntityCoverage()) {
      throw new IllegalStateException(String.format("The entity coverage %.3f in range [%d, %d] for option %s"
                                                        + " does not meet requirement.",
                                                    completeness.entityCoverage(), from, to, options));
    }
    if (completeness.entityGroupCoverage() < options.minEntityGroupCoverage()) {
      throw new IllegalStateException(String.format("The entity group coverage %.3f in range [%d, %d] for option %s"
                                                        + " does not meet requirement.",
                                                    completeness.entityGroupCoverage(), from, to, options));
    }
  }
  
  private List<Long> toWindows(SortedSet<Long> windowIndexes) {
    List<Long> windows = new ArrayList<>(windowIndexes.size());
    windowIndexes.forEach(i -> windows.add(i * _windowMs));
    return windows;
  }

  private long windowIndex(long time) {
    return time / _windowMs + 1;
  }

  /**
   * Interpret the aggregation options so that the interestedEntities contains the objects in the identity entity map.
   * We do this to ensure we will only have one object of each entity in memory.
   *
   * @param options the {@link AggregationOptions} to interpret.
   * @return A new {@link AggregationOptions} that only refers to the entities and groups in the identity entity map.
   */
  private AggregationOptions<G, E> interpretAggregationOptions(AggregationOptions<G, E> options) {
    Set<E> entitiesToInclude = new HashSet<>();
    if (options.interestedEntities().isEmpty()) {
      entitiesToInclude.addAll(_rawMetrics.keySet());
    } else {
      for (E entity : options.interestedEntities()) {
        entitiesToInclude.add(identity(entity));
      }
    }
    return new AggregationOptions<>(options.minEntityCoverage(),
                                    options.minEntityGroupCoverage(),
                                    options.minValidWindows(),
                                    entitiesToInclude,
                                    options.granularity());
  }

  /**
   * Get the identity entity object.
   * @param entity the entity identity to look for.
   * @return the object of the entity in the identity entity map.
   */
  private E identity(E entity) {
    return _identityEntityMap.computeIfAbsent(entity, e -> entity);
  }
}
