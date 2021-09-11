/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.common.WindowIndexedArrays;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 *   This class is responsible for bookkeeping raw values of each kind of metrics defined in the
 *   {@link MetricDef}. It also performs the {@link Extrapolation} if some of the values are missing from the
 *   metrics samples.
 * </p>
 */
public class RawMetricValues extends WindowIndexedArrays {
  private static final Logger LOG = LoggerFactory.getLogger(RawMetricValues.class);
  // The minimum required samples for a window to not involve any extrapolation.
  private final byte _minSamplesPerWindow;
  private final byte _halfMinRequiredSamples;
  // The metric id to value array mapping. The array is a cyclic buffer. Each array slot represents a window.
  private final Map<Short, float[]> _windowValuesByMetricId;
  // The number of samples per window. The array is a cyclic buffer. Each array slot represents a window.
  private final byte[] _counts;
  // A bit set to indicate whether a given window has extrapolation or not.
  private final BitSet _extrapolations;
  // A bit set to indicate whether a given window is valid or not.
  private final BitSet _validity;

  /**
   * Construct a RawMetricValues.
   *
   * @param numWindowsToKeep the total number of windows to keep track of.
   * @param minSamplesPerWindow the minimum required samples for a window to not involve any {@link Extrapolation}.
   * @param numMetricTypesInSample the total number of raw metric types stored by {@link #_windowValuesByMetricId}
   */
  public RawMetricValues(int numWindowsToKeep, byte minSamplesPerWindow, int numMetricTypesInSample) {
    if (numWindowsToKeep <= 1) {
      throw new IllegalArgumentException("The number of windows should be at least 2 because at least one available"
                                         + " window and one current window are needed.");
    }
    _windowValuesByMetricId = new HashMap<>();
    _counts = new byte[numWindowsToKeep];
    _extrapolations = new BitSet(numWindowsToKeep);
    _validity = new BitSet(numWindowsToKeep);
    _minSamplesPerWindow = minSamplesPerWindow;
    _halfMinRequiredSamples = (byte) Math.max(1, _minSamplesPerWindow / 2);
    _oldestWindowIndex = Long.MAX_VALUE;
  }

  @Override
  protected int length() {
    return _counts.length;
  }

  /**
   * Update {@link #_validity} and {@link #_extrapolations} for the previous and next array indices of the given arrayIndex.
   * @param arrayIndex Array index.
   */
  private void maybeUpdateValidityAndExtrapolationOfPrevAndNextFor(int arrayIndex) {
    if (_counts[arrayIndex] >= _minSamplesPerWindow) {
      // If this index has two left neighbour indices, we may need to update the extrapolation of the previous index
      // with AvgAdjacent. We need to exclude the current window index. It will be included when new windows get rolled out.
      if (arrayIndex != arrayIndex(currentWindowIndex()) && hasTwoLeftNeighbours(arrayIndex)) {
        int prevArrayIndex = prevArrayIndex(arrayIndex);
        if (_counts[prevArrayIndex] == 0) {
          updateAvgAdjacent(prevArrayIndex);
        }
      }
      // Adding sample to the last window index should not update extrapolation of the current window index.
      // Adding sample to the current window index has no next index to update.
      if (hasTwoRightNeighbours(arrayIndex)) {
        int nextArrayIndex = nextArrayIndex(arrayIndex);
        if (_counts[nextArrayIndex] == 0) {
          updateAvgAdjacent(nextArrayIndex);
        }
      }
    }
  }

  /**
   * Update the value and sample count of the window with the given index for all sample metrics.
   *
   * @param sample The metric sample to add.
   * @param windowIndex the window index of the metric sample.
   * @param metricDef the metric definitions.
   * @return Array index of the cyclic buffers used for {@link #_counts} and {@link #_windowValuesByMetricId}.
   */
  private int updateWindowValueAndCount(MetricSample<?, ?> sample, long windowIndex, MetricDef metricDef) {
    int arrayIndex = arrayIndex(windowIndex);
    for (Map.Entry<Short, Double> entry : sample.allMetricValues().entrySet()) {
      _windowValuesByMetricId.computeIfAbsent(entry.getKey(), k -> new float[_counts.length]);
      updateWindowValueForMetric(entry.getValue(), metricDef.metricInfo(entry.getKey()), arrayIndex);
    }
    // Update the count of samples in the window with the given index.
    _counts[arrayIndex]++;

    return arrayIndex;
  }

  /**
   * Add a {@link MetricSample} to the raw metric values.
   *
   * @param sample The metric sample to add.
   * @param windowIndex the window index of the metric sample.
   * @param metricDef the metric definitions.
   */
  public synchronized void addSample(MetricSample<?, ?> sample, long windowIndex, MetricDef metricDef) {
    // This sample is being added during window rolling.
    if (windowIndex < _oldestWindowIndex) {
      return;
    } else if (windowIndex > currentWindowIndex()) {
      throw new IllegalArgumentException("Cannot add sample to window index " + windowIndex + ", which is larger "
                                             + "than the current window index " + currentWindowIndex());
    }

    int arrayIndex = updateWindowValueAndCount(sample, windowIndex, metricDef);
    // Update the validity and extrapolation for this array index and the previous and next array indices.
    maybeUpdateValidityAndExtrapolationFor(arrayIndex);
    maybeUpdateValidityAndExtrapolationOfPrevAndNextFor(arrayIndex);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Added metric sample {} to window index {}, array index is {}, current count : {}",
                sample, windowIndex, arrayIndex, _counts[arrayIndex]);
    }
  }

  /**
   * Update the oldest window index. This usually happens when a new window is rolled out.
   * The oldest window index should be monotonically increasing.
   *
   * @param newOldestWindowIndex the new oldest window index.
   */
  public synchronized void updateOldestWindowIndex(long newOldestWindowIndex) {
    long prevLastWindowIndex = lastWindowIndex();
    _oldestWindowIndex = newOldestWindowIndex;
    // Advancing the oldest window index will make the previous current window index become available to its
    // neighbour index (i.e. the previous last index) for AVG_ADJACENT extrapolation. We don't need to update the
    // current window index because it would be up to date during the addSample call.
    if (prevLastWindowIndex >= _oldestWindowIndex) {
      maybeUpdateValidityAndExtrapolationFor(arrayIndex(prevLastWindowIndex));
    }
  }

  /**
   * Check whether this raw metric value is valid or not. The raw metric value is valid if:
   * 1. All the stable windows are {@code valid} (See {@link MetricSampleAggregator}, AND
   * 2. The number of windows with extrapolation is less than the given maxAllowedWindowsWithExtrapolation.
   *
   * @param maxAllowedWindowsWithExtrapolation the maximum number of allowed windows with extrapolation.
   * @return {@code true} if the raw metric value is valid, {@code false} otherwise.
   */
  public synchronized boolean isValid(int maxAllowedWindowsWithExtrapolation) {
    int currentArrayIndex = arrayIndex(currentWindowIndex());
    // The total number of valid window indices should exclude the current window index.
    int numValidIndicesAdjustment = _validity.get(currentArrayIndex) ? 1 : 0;
    boolean allIndicesValid = _validity.cardinality() - numValidIndicesAdjustment == _counts.length - 1;
    // All indices should be valid and should not have more than maxAllowedWindowsWithExtrapolation extrapolations.
    return allIndicesValid && numWindowsWithExtrapolation() <= maxAllowedWindowsWithExtrapolation;
  }

  /**
   * @return The number of stable windows with extrapolations.
   */
  public synchronized int numWindowsWithExtrapolation() {
    int currentArrayIndex = arrayIndex(currentWindowIndex());
    int numExtrapolationAdjustment = _extrapolations.get(currentArrayIndex) ? 1 : 0;
    return _extrapolations.cardinality() - numExtrapolationAdjustment;
  }

  /**
   * Check if the window at the given window index is valid. An extrapolated window is still considered as valid.
   * Assumes {@link #sanityCheckWindowIndex(long)} is called before this function.
   *
   * @param windowIndex the window index to check.
   * @return {@code true} if the given window is valid, {@code false} otherwise.
   */
  public synchronized boolean isValidAtWindowIndex(long windowIndex) {
    return _validity.get(arrayIndex(windowIndex));
  }

  /**
   * Check if the window at the given window index is extrapolated.
   * Assumes {@link #sanityCheckWindowIndex(long)} is called before this function.
   *
   * @param windowIndex the index of the window to check.
   * @return {@code true} if the window is extrapolated, {@code false} otherwise.
   */
  public synchronized boolean isExtrapolatedAtWindowIndex(long windowIndex) {
    return _extrapolations.get(arrayIndex(windowIndex));
  }

  /**
   * Assumes {@link #sanityCheckWindowIndex(long)} is called before this function.
   *
   * @param windowIndex Window index.
   * @return sample counts at window index.
   */
  public synchronized byte sampleCountsAtWindowIndex(long windowIndex) {
    return _counts[arrayIndex(windowIndex)];
  }

  public synchronized void sanityCheckWindowIndex(long windowIndex) {
    validateWindowIndex(windowIndex);
  }

  /**
   * Check whether the given window range can be reset.
   *
   * @param startingWindowIndex the starting index of the windows to reset.
   * @param numWindowIndicesToReset the number of windows to reset.
   */
  public synchronized void sanityCheckWindowRangeReset(long startingWindowIndex, int numWindowIndicesToReset) {
    if (inValidWindowRange(startingWindowIndex)
        || inValidWindowRange(startingWindowIndex + numWindowIndicesToReset - 1)) {
      throw new IllegalStateException("Should never reset a window index that is in the valid range");
    }
  }

  /**
   * Clear the state of a given number of windows starting at the given window index.
   * Assumes that {@link #sanityCheckWindowRangeReset(long, int)} is satisfied -- this prevents repetitive sanity checks
   * within this function.
   *
   * @param startingWindowIndex the starting index of the windows to reset.
   * @param numWindowIndicesToReset the number of windows to reset.
   * @return Number of samples abandoned in window clearing process. The abandoned samples are samples in the windows which get reset.
   */
  public synchronized int resetWindowIndices(long startingWindowIndex, int numWindowIndicesToReset) {
    // We are not resetting all the data here. The data will be interpreted to 0 if count is 0.
    int numAbandonedSamples = 0;
    for (long i = startingWindowIndex; i < startingWindowIndex + numWindowIndicesToReset; i++) {
      int arrayIndex = arrayIndex(i);
      numAbandonedSamples += _counts[arrayIndex];
      _counts[arrayIndex] = 0;
      resetValidityAndExtrapolation(arrayIndex);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Resetting window index [{}, {}], abandon {} samples.", startingWindowIndex,
                startingWindowIndex + numWindowIndicesToReset - 1, numAbandonedSamples);
    }
    return numAbandonedSamples;
  }

  /**
   * Get the aggregated values of the given sorted set of windows. The result {@link ValuesAndExtrapolations} contains
   * the windows in the same order.
   *
   * @param windowIndices the sorted set of windows to get values for.
   * @param metricDef the metric definitions.
   * @return The aggregated values and extrapolations of the given sorted set of windows in that order.
   */
  public synchronized ValuesAndExtrapolations aggregate(SortedSet<Long> windowIndices, MetricDef metricDef) {
    return aggregate(windowIndices, metricDef, true);
  }

  private ValuesAndExtrapolations aggregate(SortedSet<Long> windowIndices, MetricDef metricDef, boolean checkWindow) {
    if (_windowValuesByMetricId.isEmpty()) {
      return ValuesAndExtrapolations.empty(windowIndices.size(), metricDef);
    }
    Map<Short, MetricValues> aggValues = new HashMap<>();
    SortedMap<Integer, Extrapolation> extrapolations = new TreeMap<>();
    for (Map.Entry<Short, float[]> entry : _windowValuesByMetricId.entrySet()) {
      short metricId = entry.getKey();
      float[] values = entry.getValue();
      MetricInfo info = metricDef.metricInfo(metricId);

      MetricValues aggValuesForMetric = new MetricValues(windowIndices.size());
      aggValues.put(metricId, aggValuesForMetric);

      int resultIndex = 0;
      for (long windowIndex : windowIndices) {
        // When we query the latest window, we need to skip the window validation because the valid windows do not
        // include the current active window.
        if (checkWindow) {
          validateWindowIndex(windowIndex);
        }
        int arrayIndex = arrayIndex(windowIndex);
        // Sufficient samples
        if (_counts[arrayIndex] >= _halfMinRequiredSamples) {
          aggValuesForMetric.set(resultIndex, getValue(info, arrayIndex, values));
          if (_counts[arrayIndex] < _minSamplesPerWindow) {
            // Though not quite sufficient, but have some available.
            extrapolations.putIfAbsent(resultIndex, Extrapolation.AVG_AVAILABLE);
          }
          // Not sufficient, check the neighbors. The neighbors only exist when the index is not on the edge, i.e.
          // neither the first nor last index.
        } else if (arrayIndex != firstArrayIndex() && arrayIndex != lastArrayIndex()
                   && _counts[prevArrayIndex(arrayIndex)] >= _minSamplesPerWindow
                   && _counts[nextArrayIndex(arrayIndex)] >= _minSamplesPerWindow) {
          extrapolations.putIfAbsent(resultIndex, Extrapolation.AVG_ADJACENT);
          int prevArrayIndex = prevArrayIndex(arrayIndex);
          int nextArrayIndex = nextArrayIndex(arrayIndex);
          double total = _windowValuesByMetricId.get(metricId)[prevArrayIndex] + (_counts[arrayIndex] == 0 ? 0 : _windowValuesByMetricId
              .get(metricId)[arrayIndex])
                         + _windowValuesByMetricId.get(metricId)[nextArrayIndex];
          switch (info.aggregationFunction()) {
            case AVG:
              aggValuesForMetric.set(resultIndex, total / (_counts[prevArrayIndex] + _counts[arrayIndex] + _counts[nextArrayIndex]));
              break;
            case MAX:
            case LATEST:
              // for max and latest, we already only keep the largest or last value.
              aggValuesForMetric.set(resultIndex, total / (_counts[arrayIndex] > 0 ? 3 : 2));
              break;
            default:
              throw new IllegalStateException("Should never be here.");
          }
          // Neighbor not available, use the insufficient samples.
        } else if (_counts[arrayIndex] > 0) {
          aggValuesForMetric.set(resultIndex, getValue(info, arrayIndex, values));
          extrapolations.putIfAbsent(resultIndex, Extrapolation.FORCED_INSUFFICIENT);
          // Nothing is available, just return all 0 and NO_VALID_EXTRAPOLATION.
        } else {
          aggValuesForMetric.set(resultIndex, 0);
          extrapolations.putIfAbsent(resultIndex, Extrapolation.NO_VALID_EXTRAPOLATION);
        }
        resultIndex++;
      }
    }
    return new ValuesAndExtrapolations(new AggregatedMetricValues(aggValues), extrapolations);
  }

  /**
   * Peek the value for the current window.
   *
   * @param currentWindowIndex Current window index.
   * @param metricDef the metric definitions.
   * @return The aggregated values and extrapolations of the given sorted set of windows in that order.
   */
  public synchronized ValuesAndExtrapolations peekCurrentWindow(long currentWindowIndex, MetricDef metricDef) {
    SortedSet<Long> window = new TreeSet<>();
    window.add(currentWindowIndex);
    return aggregate(window, metricDef, false);
  }

  /**
   * @return The total number of samples added to this RawMetricValues.
   */
  public synchronized int numSamples() {
    int count = 0;
    for (byte i : _counts) {
      count += i;
    }
    return count;
  }

  private float getValue(MetricInfo info, int index, float[] values) {
    if (_counts[index] == 0) {
      return 0;
    }
    switch (info.aggregationFunction()) {
      case AVG:
        return values[index] / _counts[index];
      case MAX:
      case LATEST:
        return values[index];
      default:
        throw new IllegalStateException("Should never be here.");
    }
  }

  private void updateWindowValueForMetric(double newValue, MetricInfo info, int arrayIndex) {
    switch (info.aggregationFunction()) {
      case AVG:
        add(newValue, info.id(), arrayIndex);
        break;
      case MAX:
        max(newValue, info.id(), arrayIndex);
        break;
      case LATEST:
        latest(newValue, info.id(), arrayIndex);
        break;
      default:
        throw new IllegalStateException("Should never be here");
    }
  }

  private void add(double newValue, short metricId, int index) {
    _windowValuesByMetricId.get(metricId)[index] = (float) (_counts[index] == 0 ? newValue : _windowValuesByMetricId.get(metricId)[index] + newValue);
  }

  private void max(double newValue, short metricId, int index) {
    _windowValuesByMetricId.get(metricId)[index] = (float) (_counts[index] == 0 ? newValue : Math.max(
        _windowValuesByMetricId.get(metricId)[index], newValue));
  }

  private void latest(double newValue, short metricId, int index) {
    _windowValuesByMetricId.get(metricId)[index] = (float) newValue;
  }

  /**
   * Update {@link #_validity} and {@link #_extrapolations} for the given arrayIndex.
   * @param arrayIndex Array index.
   */
  private void maybeUpdateValidityAndExtrapolationFor(int arrayIndex) {
    if (!updateEnoughSamples(arrayIndex) && !_extrapolations.get(arrayIndex)
        && !updateForcedInsufficient(arrayIndex) && !updateAvgAdjacent(arrayIndex)) {
      resetValidityAndExtrapolation(arrayIndex);
    }
  }

  private void resetValidityAndExtrapolation(int arrayIndex) {
    _validity.clear(arrayIndex);
    _extrapolations.clear(arrayIndex);
  }

  /**
   * @param arrayIndex Array index.
   * @return if there are {@link #_minSamplesPerWindow}, then valid: {@code true}, extrapolation: {@code false}
   */
  private boolean updateEnoughSamples(int arrayIndex) {
    if (_counts[arrayIndex] == _minSamplesPerWindow) {
      _validity.set(arrayIndex);
      _extrapolations.clear(arrayIndex);
      return true;
    }
    return _counts[arrayIndex] >= _minSamplesPerWindow;
  }

  /**
   * @param arrayIndex Array index.
   * @return regardless of the number of samples in the given arrayIndex, if next and previous array indices are valid and have
   * more than {@link #_minSamplesPerWindow}, then valid: true, extrapolation: true
   */
  private boolean updateAvgAdjacent(int arrayIndex) {
    int prevArrayIndex = prevArrayIndex(arrayIndex);
    int nextArrayIndex = nextArrayIndex(arrayIndex);
    if (prevArrayIndex == INVALID_INDEX || nextArrayIndex == INVALID_INDEX) {
      return false;
    }
    if (_counts[prevArrayIndex] >= _minSamplesPerWindow && _counts[nextArrayIndex] >= _minSamplesPerWindow) {
      _validity.set(arrayIndex);
      _extrapolations.set(arrayIndex);
      return true;
    }
    return false;
  }

  /**
   * @param arrayIndex Array index.
   * @return if there is at least one sample, then valid: true, extrapolation: true
   */
  private boolean updateForcedInsufficient(int arrayIndex) {
    if (_counts[arrayIndex] > 0) {
      _validity.set(arrayIndex);
      _extrapolations.set(arrayIndex);
      return true;
    }
    return false;
  }

  private boolean hasTwoLeftNeighbours(int arrayIndex) {
    int prevIdx = prevArrayIndex(arrayIndex);
    return prevIdx != INVALID_INDEX && prevArrayIndex(prevIdx) != INVALID_INDEX;
  }

  private boolean hasTwoRightNeighbours(int arrayIndex) {
    int nextIdx = nextArrayIndex(arrayIndex);
    return nextIdx != INVALID_INDEX && nextArrayIndex(nextIdx) != INVALID_INDEX;
  }
}
