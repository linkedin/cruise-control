/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.common.LongGenerationed;
import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is responsible for aggregate {@link MetricSample MetricSamples} for {@link Entity entities}. It uses
 * a cyclic buffer to keep track of the most recent N (configured) windows. The oldest windows are evicted when
 * the buffer is full.
 * <p>
 *   The {@link MetricSampleAggregator} aggregates the metrics of each entities in each window. The aggregation
 *   can be viewed as two dimensions: per entity and per window.
 * </p>
 * <p>
 *   From per entity's perspective, each entity would have sufficient metrics from all the windows when everything
 *   works fine. However, it is also possible that some metrics may be missing from the MetricSampleAggregator
 *   in one or more windows. If that happens, some {@link Extrapolation} will be used to fill in the missing data.
 *   If none of the {@link Extrapolation} works. We claim the entity is {@code invalid} in this window. With above,
 *   a given entity may have the following three states in any given window from the {@link MetricSampleAggregator}'s
 *   perspective:
 *   <ul>
 *     <li>Valid: The entity has sufficient metric samples in the window</li>
 *     <li>Extrapolated: The entity has some metric samples in the window, but the validity is less strong because
 *                       extrapolation was involved.</li>
 *     <li>Invalid: The entity has insufficient metric samples in the window and no extrapolation works.</li>
 *   </ul>
 *   The validity of an entity is determined by its validity in all the windows. An entity is considered as invalid
 *   if there is an invalid window or there are too many windows with extrapolations.
 *
 * <p>
 *   Furthermore, each entity belongs to an aggregation entity group. The aggregation entity group is only used
 *   for metric aggregation purpose. Users can specify the {@link AggregationOptions.Granularity} of the metric aggregation. The
 *   granularity could be one of the following:
 *   <ul>
 *     <li>
 *       {@link AggregationOptions.Granularity#ENTITY}: The validity of the entities in the same aggregation group
 *       are considered independently, i.e. an invalid entity in an aggregation group does not invalidate the other
 *       entities in the same aggregation entity group.
 *     </li>
 *     <li>
 *       {@link AggregationOptions.Granularity#ENTITY_GROUP}: The validity of the entities in the same aggregation entity group are
 *       considered as an entirety. i.e. a single invalid entity in the aggregation entity group invalidates all
 *       the entities in the same aggregation entity group.
 *     </li>
 *   </ul>
 *
 * <p>
 *   From per window's perspective, for each window, there is a given set of {@code valid} entities and entity
 *   groups as described above. The validity of a window depends on the requirements specified in the
 *   {@link AggregationOptions} during the aggregation. More specifically whether the entity coverage (valid entity
 *   ratio) and entity group coverage (valid entity group ratio) meet the requirements.
 * </p>
 *
 * <p>This class is thread safe.</p>
 *
 * @param <G> The aggregation entity group class. Note that the entity group will be used as a key to HashMaps,
 *           so it must have a valid {@link Object#hashCode()} and {@link Object#equals(Object)} implementation.
 * @param <E> The entity class. Note that the entity will be used as a key to HashMaps, so it must have
 *           a valid {@link Object#hashCode()} and {@link Object#equals(Object)} implementation.
 */
public class MetricSampleAggregator<G, E extends Entity<G>> extends LongGenerationed {
  private static final Logger LOG = LoggerFactory.getLogger(MetricSampleAggregator.class);

  private final ConcurrentMap<E, RawMetricValues> _rawMetrics;
  private final MetricSampleAggregatorState<G, E> _aggregatorState;
  private final ReentrantLock _windowRollingLock;
  private final ConcurrentMap<E, E> _identityEntityMap;

  protected final int _numWindows;
  protected final byte _minSamplesPerWindow;
  protected final int _numWindowsToKeep;
  protected final long _windowMs;
  protected final long _monitoringPeriodMs;
  protected final MetricDef _metricDef;
  protected SampleType _sampleType;

  private volatile long _currentWindowIndex;
  private volatile long _oldestWindowIndex;

  /**
   * Construct the metric sample aggregator.
   *
   * @param numWindows the number of windows needed.
   * @param windowMs the size of each window in milliseconds
   * @param minSamplesPerWindow minimum samples per window.
   * @param completenessCacheSize the completeness cache size, i.e. the number of recent completeness query result to
   *                              cache.
   * @param metricDef metric definitions.
   */
  public MetricSampleAggregator(int numWindows,
                                long windowMs,
                                byte minSamplesPerWindow,
                                int completenessCacheSize,
                                MetricDef metricDef) {
    super(0);
    _identityEntityMap = new ConcurrentHashMap<>();
    _rawMetrics = new ConcurrentHashMap<>();
    _numWindows = numWindows;
    _windowMs = windowMs;
    _monitoringPeriodMs = _numWindows * _windowMs;
    // We keep one more window for the active window.
    _numWindowsToKeep = _numWindows + 1;
    _minSamplesPerWindow = minSamplesPerWindow;
    _windowRollingLock = new ReentrantLock();
    _metricDef = metricDef;
    _aggregatorState = new MetricSampleAggregatorState<>(numWindows, _windowMs, completenessCacheSize);
    _oldestWindowIndex = 0L;
    _currentWindowIndex = 0L;
  }

  /**
   * Add a sample to the metric aggregator.
   *
   * @param sample The metric sample to add.
   *
   * @return {@code true} if the sample is accepted, {@code false} if the sample is ignored.
   */
  public boolean addSample(MetricSample<G, E> sample) {
    if (!sample.isValid(_metricDef)) {
      LOG.debug("The metric sample is discarded due to missing metrics. Sample: {}", sample);
      return false;
    }
    long windowIndex = windowIndex(sample.sampleTime());
    // Skip the samples that are too old.
    if (windowIndex < _oldestWindowIndex) {
      return false;
    }
    boolean newWindowsRolledOut = maybeRollOutNewWindow(windowIndex);
    RawMetricValues rawMetricValues =
        _rawMetrics.computeIfAbsent(identity(sample.entity()), k -> {
          // Need to grab the lock to make sure the raw value for this partition is updated correctly when
          // the raw values was created in an existing window while a new window is being rolled out.
          _windowRollingLock.lock();
          try {
            RawMetricValues rawValues = new RawMetricValues(_numWindowsToKeep, _minSamplesPerWindow, _metricDef.size());
            rawValues.updateOldestWindowIndex(_oldestWindowIndex);
            return rawValues;
          } finally {
            _windowRollingLock.unlock();
          }
        });
    LOG.trace("Adding sample {} to window index {}", sample, windowIndex);
    rawMetricValues.addSample(sample, windowIndex, _metricDef);
    if (newWindowsRolledOut || windowIndex != _currentWindowIndex) {
      // Either new window(s) rolled out or the data has been inserted to an old window. Both cases affect the historical
      // load information that Cruise Control is interested in. Hence, they require bumping up the generation of the
      // window index of the added sample.
      _aggregatorState.updateWindowGeneration(windowIndex, _generation.incrementAndGet());
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
   *   all the window indices marked as {@link Extrapolation#NO_VALID_EXTRAPOLATION}.
   * </p>
   *
   * @param from the starting timestamp of the aggregation period in milliseconds.
   * @param to the end timestamp of the aggregation period in milliseconds.
   * @param options the {@link AggregationOptions} used to perform the aggregation.
   * @return An {@link MetricSampleAggregationResult} based on the given AggregationOptions.
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
        throw new NotEnoughValidWindowsException(String.format("There is no window available in range [%d, %d] (index [%d, %d])."
                                                               + " Window index (current: %d, oldest: %d).", from, to,
                                                               fromWindowIndex, toWindowIndex, _currentWindowIndex, _oldestWindowIndex));
      }

      // Get and verify the completeness.
      maybeUpdateAggregatorState();
      AggregationOptions<G, E> interpretedOptions = interpretAggregationOptions(options);
      MetricSampleCompleteness<G, E> completeness =
          _aggregatorState.completeness(fromWindowIndex, toWindowIndex, interpretedOptions, generation());
      // We use the original time from and to here because they are only for logging purpose.
      validateCompleteness(from, to, completeness, interpretedOptions);

      // Perform the aggregation.
      List<Long> windows = toWindows(completeness.validWindowIndices());
      MetricSampleAggregationResult<G, E> result = new MetricSampleAggregationResult<>(generation(), completeness);
      Set<E> entitiesToInclude =
          interpretedOptions.includeInvalidEntities() ? interpretedOptions.interestedEntities() : completeness.validEntities();
      for (E entity : entitiesToInclude) {
        RawMetricValues rawValues = _rawMetrics.get(entity);
        if (rawValues == null) {
          ValuesAndExtrapolations
              valuesAndExtrapolations = ValuesAndExtrapolations.empty(completeness.validWindowIndices().size(), _metricDef);
          valuesAndExtrapolations.setWindows(windows);
          result.addResult(entity, valuesAndExtrapolations);
          result.recordInvalidEntity(entity);
        } else {
          ValuesAndExtrapolations
              valuesAndExtrapolations = rawValues.aggregate(completeness.validWindowIndices(), _metricDef);
          valuesAndExtrapolations.setWindows(windows);
          result.addResult(entity, valuesAndExtrapolations);
          if (!rawValues.isValid(options.maxAllowedExtrapolationsPerEntity())) {
            result.recordInvalidEntity(entity);
          }
        }
      }
      return result;
    } finally {
      _windowRollingLock.unlock();
    }
  }

  /**
   * Peek the information for all the available entities of the current window.
   *
   * @return A map from all the entities to their current metric values.
   */
  public Map<E, ValuesAndExtrapolations> peekCurrentWindow() {
    // prevent window rolling.
    _windowRollingLock.lock();
    try {
      Map<E, ValuesAndExtrapolations> result = new HashMap<>();
      _rawMetrics.forEach((entity, rawMetric) -> {
        ValuesAndExtrapolations vae = rawMetric.peekCurrentWindow(_currentWindowIndex, _metricDef);
        SortedSet<Long> currentWindows = new TreeSet<>(Collections.singleton(_currentWindowIndex));
        vae.setWindows(toWindows(currentWindows));
        result.put(entity, vae);
      });
      return result;
    } finally {
      _windowRollingLock.unlock();
    }
  }

  /**
   * Get the {@link MetricSampleCompleteness} of the MetricSampleAggregator with the given {@link AggregationOptions}
   * for a given period of time. The current active window is excluded.
   *
   * @param from starting time of the period to check.
   * @param to ending time of the period to check.
   * @param options the {@link AggregationOptions} to use for the completeness check.
   * @return The {@link MetricSampleCompleteness} of the MetricSampleAggregator.
   */
  public MetricSampleCompleteness<G, E> completeness(long from, long to, AggregationOptions<G, E> options) {
    _windowRollingLock.lock();
    try {
      long fromWindowIndex = Math.max(windowIndex(from), _oldestWindowIndex);
      long toWindowIndex = Math.min(windowIndex(to), _currentWindowIndex - 1);
      if (fromWindowIndex > _currentWindowIndex || toWindowIndex < _oldestWindowIndex) {
        return new MetricSampleCompleteness<>(generation(), _windowMs);
      }
      maybeUpdateAggregatorState();
      return _aggregatorState.completeness(fromWindowIndex,
                                           toWindowIndex,
                                           interpretAggregationOptions(options),
                                           generation());
    } finally {
      _windowRollingLock.unlock();
    }
  }

  /**
   * Get a list of available windows in the MetricSampleAggregator. The available windows may include the windows
   * that do not have any metric samples. It is just a consecutive list of windows starting from the oldest window
   * that hasn't been evicted (inclusive) until the current active window (exclusive).
   *
   * It should not be confused with valid windows.
   *
   * @return A list of available windows in the MetricSampleAggregator.
   */
  public List<Long> availableWindows() {
    return getWindowList(_oldestWindowIndex, _currentWindowIndex - 1);
  }

  /**
   * Get the number of available windows in the MetricSampleAggregator. The available windows may include the windows
   * that do not have any metric samples. It is just a consecutive list of windows starting from the oldest
   * window that hasn't been evicted (inclusive) until the current active window (exclusive).
   *
   * It should not be confused with valid windows.
   *
   * @return The number of available windows in the MetricSampleAggregator.
   */
  public int numAvailableWindows() {
    return numAvailableWindows(-1, Long.MAX_VALUE);
  }

  /**
   * Get the number of available windows in the given time range, excluding the current active window.
   * The available windows may include the windows that do not have any metric samples. It is just a consecutive
   * list of windows starting from the oldest window that hasn't been evicted (inclusive) until the current
   * active window (exclusive).
   *
   * It should not be confused with valid windows.
   *
   * @param from the starting time of the time range. (inclusive)
   * @param to the end time of the time range. (inclusive)
   * @return The number of the windows in the given time range.
   */
  public int numAvailableWindows(long from, long to) {
    long fromWindowIndex = Math.max(windowIndex(from), _oldestWindowIndex);
    long toWindowIndex = Math.min(windowIndex(to), _currentWindowIndex - 1);
    return Math.max(0, (int) (toWindowIndex - fromWindowIndex + 1));
  }

  /**
   * @return All the windows in the MetricSampleAggregator, including the current active window.
   */
  public List<Long> allWindows() {
    return getWindowList(_oldestWindowIndex, _currentWindowIndex);
  }

  /**
   * @return The earliest available window in the MetricSampleAggregator. Null is returned if there is
   * no window available at all.
   */
  public Long earliestWindow() {
    return _rawMetrics.isEmpty() ? null : _oldestWindowIndex * _windowMs;
  }

  /**
   * Get the total number of samples that is currently aggregated by the MetricSampleAggregator. The number
   * only includes the windows that are still maintained by the MetricSampleAggregator. The evicted windows
   * are not included.
   *
   * @return The number of samples aggregated by the MetricSampleAggregator.
   */
  public int numSamples() {
    return _rawMetrics.values().stream().mapToInt(RawMetricValues::numSamples).sum();
  }

  /**
   * Keep the given set of entities in the MetricSampleAggregator and remove the rest of the entities.
   *
   * @param entities the entities to retain.
   */
  public void retainEntities(Set<E> entities) {
    boolean anyElementsRemoved = _rawMetrics.entrySet().removeIf(entry -> !entities.contains(entry.getKey()));
    if (anyElementsRemoved) {
      _generation.incrementAndGet();
    }
  }

  /**
   * Remove the given set of entities from the MetricSampleAggregator.
   *
   * @param entities the entities to remove.
   */
  public void removeEntities(Set<E> entities) {
    boolean anyElementsRemoved = _rawMetrics.entrySet().removeIf(entry -> entities.contains(entry.getKey()));
    if (anyElementsRemoved) {
      _generation.incrementAndGet();
    }
  }

  /**
   * Keep the given set of entity groups in the MetricSampleAggregator and remove the reset of the entity groups.
   *
   * @param entityGroups the entity groups to retain.
   */
  public void retainEntityGroup(Set<G> entityGroups) {
    boolean anyElementsRemoved = _rawMetrics.entrySet().removeIf(entry -> !entityGroups.contains(entry.getKey().group()));
    if (anyElementsRemoved) {
      _generation.incrementAndGet();
    }
  }

  /**
   * Remove the given set of entity groups from the MetricSampleAggregator.
   *
   * @param entityGroups the entity groups to remove from the MetricSampleAggregator.
   */
  public void removeEntityGroup(Set<G> entityGroups) {
    boolean anyElementsRemoved = _rawMetrics.entrySet().removeIf(entry -> entityGroups.contains(entry.getKey().group()));
    if (anyElementsRemoved) {
      _generation.incrementAndGet();
    }
  }

  /**
   * Clear the MetricSampleAggregator.
   */
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

  /**
   * Package private for testing.
   * @return Metric sample aggregator state.
   */
  MetricSampleAggregatorState<G, E> aggregatorState() {
    maybeUpdateAggregatorState();
    return _aggregatorState;
  }

  // both from and to window indices are inclusive.
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

  private void maybeUpdateAggregatorState() {
    long currentGeneration = generation();
    for (long windowIndex : _aggregatorState.windowIndicesToUpdate(_oldestWindowIndex, _currentWindowIndex)) {
      _aggregatorState.updateWindowState(windowIndex, getWindowState(windowIndex, currentGeneration));
    }
  }

  private WindowState<G, E> getWindowState(long windowIndex, long currentGeneration) {
    WindowState<G, E> windowState = new WindowState<>(currentGeneration);
    for (Map.Entry<E, RawMetricValues> entry : _rawMetrics.entrySet()) {
      E entity = entry.getKey();
      RawMetricValues rawValues = entry.getValue();
      rawValues.sanityCheckWindowIndex(windowIndex);
      if (rawValues.isExtrapolatedAtWindowIndex(windowIndex)) {
        windowState.addExtrapolatedEntities(entity);
      }
      if (rawValues.isValidAtWindowIndex(windowIndex)) {
        windowState.addValidEntities(entity);
      }
    }
    return windowState;
  }

  private boolean maybeRollOutNewWindow(long windowIndex) {
    if (_currentWindowIndex < windowIndex) {
      _windowRollingLock.lock();
      try {
        if (_currentWindowIndex < windowIndex) {
          // find out how many windows we need to reset in the raw metrics.
          int numWindowsToRollOut = (int) (windowIndex - _currentWindowIndex);
          // First set the oldest window index so newly coming older samples will not be added.
          long prevOldestWindowIndex = _oldestWindowIndex;
          // The first possible window index is actually 1 instead of 0.
          _oldestWindowIndex = Math.max(1, windowIndex - _numWindows);
          int numOldWindowIndicesToReset = (int) Math.min(_numWindowsToKeep, _oldestWindowIndex - prevOldestWindowIndex);
          int numAbandonedSamples = 0;
          // Reset all the data starting from previous oldest window. After this point the old samples cannot get
          // into the raw metric values. We only need to reset the index if the new index is at least _numWindows;
          if (numOldWindowIndicesToReset > 0) {
            numAbandonedSamples = resetIndices(prevOldestWindowIndex, numOldWindowIndicesToReset);
          }
          // Set the generation of the old current window.
          _aggregatorState.updateWindowGeneration(_currentWindowIndex, generation());
          // Lastly update current window.
          _currentWindowIndex = windowIndex;
          LOG.info("{} Aggregator rolled out {} new windows, reset {} windows, current window range [{}, {}], abandon {} samples.",
                    _sampleType, numWindowsToRollOut, numOldWindowIndicesToReset, _oldestWindowIndex * _windowMs,
                    _currentWindowIndex * _windowMs, numAbandonedSamples);
          return true;
        }
      } finally {
        _windowRollingLock.unlock();
      }
    }
    return false;
  }

  private int resetRawValueIndices(long prevOldestWindowIndex, int numIndicesToReset, long currentOldestWindowIndex) {
    int numAbandonedSamples = 0;
    // Each member of _rawMetrics has the same window range; hence, a single sanity check for any member is sufficient.
    Iterator<RawMetricValues> iterator = _rawMetrics.values().iterator();
    if (iterator.hasNext()) {
      RawMetricValues rawValues = iterator.next();
      rawValues.updateOldestWindowIndex(currentOldestWindowIndex);
      rawValues.sanityCheckWindowRangeReset(prevOldestWindowIndex, numIndicesToReset);
      numAbandonedSamples += rawValues.resetWindowIndices(prevOldestWindowIndex, numIndicesToReset);
    }

    while (iterator.hasNext()) {
      RawMetricValues rawValues = iterator.next();
      rawValues.updateOldestWindowIndex(currentOldestWindowIndex);
      numAbandonedSamples += rawValues.resetWindowIndices(prevOldestWindowIndex, numIndicesToReset);
    }

    return numAbandonedSamples;
  }

  private int resetIndices(long prevOldestWindowIndex, int numIndicesToReset) {
    long currentOldestWindowIndex = _oldestWindowIndex;
    int numAbandonedSamples = resetRawValueIndices(prevOldestWindowIndex, numIndicesToReset, currentOldestWindowIndex);
    _aggregatorState.updateOldestWindowIndex(currentOldestWindowIndex);
    _aggregatorState.resetWindowIndices(prevOldestWindowIndex, numIndicesToReset);
    return numAbandonedSamples;
  }

  private void validateCompleteness(long from,
                                    long to,
                                    MetricSampleCompleteness completeness,
                                    AggregationOptions<G, E> options)
      throws NotEnoughValidWindowsException {
    if (completeness.validWindowIndices().size() < options.minValidWindows()) {
      throw new NotEnoughValidWindowsException(String.format("There are only %d valid windows "
                                                                 + "when aggregating in range [%d, %d] for aggregation options %s",
                                                             completeness.validWindowIndices().size(), from, to, options));
    }
    if (completeness.validEntityRatio() < options.minValidEntityRatio()) {
      throw new IllegalStateException(String.format("The entity coverage %.3f in range [%d, %d] for option %s"
                                                        + " does not meet requirement.",
                                                    completeness.validEntityRatio(), from, to, options));
    }
    if (completeness.validEntityGroupRatio() < options.minValidEntityGroupRatio()) {
      throw new IllegalStateException(String.format("The entity group coverage %.3f in range [%d, %d] for option %s"
                                                        + " does not meet requirement.",
                                                    completeness.validEntityGroupRatio(), from, to, options));
    }
  }

  private List<Long> toWindows(SortedSet<Long> windowIndices) {
    List<Long> windows = new ArrayList<>(windowIndices.size());
    windowIndices.forEach(i -> windows.add(i * _windowMs));
    return windows;
  }

  /**
   * @param timeMs Time in milliseconds.
   * @return The absolute window index of the given timestamp.
   */
  private long windowIndex(long timeMs) {
    return timeMs / _windowMs + 1;
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
    return new AggregationOptions<>(options.minValidEntityRatio(),
                                    options.minValidEntityGroupRatio(),
                                    options.minValidWindows(),
                                    options.maxAllowedExtrapolationsPerEntity(),
                                    entitiesToInclude,
                                    options.granularity(),
                                    options.includeInvalidEntities());
  }

  /**
   * Get the identity entity object.
   * @param entity the entity identity to look for.
   * @return The object of the entity in the identity entity map.
   */
  protected E identity(E entity) {
    return _identityEntityMap.computeIfAbsent(entity, e -> entity);
  }

  /**
   * Get the length of time aggregator keeps samples in memory.
   * @return length of time.
   */
  public long monitoringPeriodMs() {
    return _monitoringPeriodMs;
  }

  /**
   * The {@link MetricSample MetricSamples} type which the aggregator collects.
   */
  protected enum SampleType {
    BROKER,
    PARTITION
  }
}
