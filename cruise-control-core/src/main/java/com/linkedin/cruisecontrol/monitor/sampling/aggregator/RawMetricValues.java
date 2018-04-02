/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

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
public class RawMetricValues {
  private static final Logger LOG = LoggerFactory.getLogger(RawMetricValues.class);
  // The minimum required samples for a window to not involve any extrapolation.
  private final int _minSamplesPerWindow;
  // The metric id to value array mapping. The array is a cyclic buffer. Each array slot represents a window.
  private final Map<Integer, float[]> _valuesByMetricId;
  // The number of samples per window. The array is a cyclic buffer. Each array slot represents a window.
  private final short[] _counts;
  // A bit set to indicate whether a given window has extrapolation or not.
  private final BitSet _extrapolations;
  // A bit set to indicate whether a given window is valid or not.
  private final BitSet _validity;
  // The oldest window index.
  private long _oldestWindowIndex;

  /**
   * Construct a RawMetricValues.
   *
   * @param numWindowsToKeep the total number of windows to keep track of.
   * @param minSamplesPerWindow the minimum required samples for a window to not involve any {@link Extrapolation}.
   */
  public RawMetricValues(int numWindowsToKeep, int minSamplesPerWindow) {
    if (numWindowsToKeep <= 1) {
      throw new IllegalArgumentException("The number of windows should be at least 2 because at least one available"
                                             + " window and one current window are needed.");
    }
    _valuesByMetricId = new HashMap<>();
    _counts = new short[numWindowsToKeep];
    _extrapolations = new BitSet(numWindowsToKeep);
    _validity = new BitSet(numWindowsToKeep);
    _minSamplesPerWindow = minSamplesPerWindow;
    _oldestWindowIndex = Long.MAX_VALUE;
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
    int idx = (int) (windowIndex % _counts.length);
    for (Map.Entry<Integer, Double> entry : sample.allMetricValues().entrySet()) {
      _valuesByMetricId.computeIfAbsent(entry.getKey(), k -> new float[_counts.length]);
      updateValue(entry.getValue(), metricDef.metricInfo(entry.getKey()), idx);
    }
    _counts[idx]++;
    updateValidityAndExtrapolation(idx);
    if (_counts[idx] >= _minSamplesPerWindow) {
      _extrapolations.clear(idx);
      if (idx != firstIdx()) {
        int prevIdx = prevIdx(idx);
        if (_counts[prevIdx] < halfMinRequiredSamples()) {
          updateAvgAdjacent(prevIdx);
        }
      }
      // Adding sample to the last window index should not update extrapolation of the current window index.
      // Adding sample to the current window index has no next index to update.
      if (idx != currentWindowIndex() && idx != lastIdx()) {
        int nextIdx = nextIdx(idx);
        if (_counts[nextIdx] < halfMinRequiredSamples()) {
          updateAvgAdjacent(nextIdx);
        }
      }
    }
    LOG.trace("Added metric sample {} to window index {}, actual index is {}, current count : {}",
              sample, windowIndex, idx, _counts[idx]);
  }

  /**
   * Update the oldest window index. This usually happens when a new window is rolled out.
   * The oldest window index should be monotonically increasing.
   *
   * @param newOldestWindowIndex the new oldest window index.
   */
  public synchronized void updateOldestWindowIndex(long newOldestWindowIndex) {
    long prevLastWindowIndex = currentWindowIndex() - 1;
    _oldestWindowIndex = newOldestWindowIndex;
    // Advancing the oldest window index will make the previous current window index become available to its
    // neighbour index (i.e. the previous last index) for AVG_ADJACENT extrapolation. We don't need to update the
    // current window index because it would be up to date during the addSample call.
    if (prevLastWindowIndex >= _oldestWindowIndex) {
      updateValidityAndExtrapolation(handleWrapping(prevLastWindowIndex));
    }
  }

  /**
   * Check whether this raw metric value is valid or not. The raw metric value is valid if:
   * 1. All the windows is <tt>valid</tt> or <tt>WithExtrapolation</tt>. (See {@link MetricSampleAggregator}, AND
   * 2. The number of windows with extrapolation is no larger than the max allowed number.
   *
   * @param maxAllowedWindowsWithExtrapolation the maximum number of allowed windows with extrapolation.
   * @return true if the raw metric value is valid, false otherwise.
   */
  public synchronized boolean isValid(int maxAllowedWindowsWithExtrapolation) {
    int currentIdx = handleWrapping(currentWindowIndex());
    // The total number of valid window indexes should exclude the current window index.
    int numValidIndexesAdjustment = _validity.get(handleWrapping(currentIdx)) ? 1 : 0;
    boolean allIndexesValid = _validity.cardinality() - numValidIndexesAdjustment == _counts.length - 1;
    // All indexes should be valid and should not have too many extrapolations.
    return allIndexesValid && numWindowsWithExtrapolation() <= maxAllowedWindowsWithExtrapolation;
  }

  /**
   * @return the number of windows with extrapolations.
   */
  public synchronized int numWindowsWithExtrapolation() {
    int currentIdx = handleWrapping(currentWindowIndex());
    int numExtrapolationAdjustment = _extrapolations.get(currentIdx) ? 1 : 0;
    return _extrapolations.cardinality() - numExtrapolationAdjustment;
  }

  /**
   * Check if the window at the given window index is valid. A extrapolated window is still considered as
   * valid.
   *
   * @param windowIndex the window index to check.
   * @return true if the given window is valid, false otherwise.
   */
  public synchronized boolean isValidAtWindowIndex(long windowIndex) {
    validateIndex(windowIndex);
    return _validity.get(handleWrapping(windowIndex));
  }

  /**
   * Check if the window at the given window index is extrapolated
   * @param windowIndex the index of the window to check.
   * @return true if the window is extrapolated, false otherwise.
   */
  public synchronized boolean isExtrapolatedAtWindowIndex(long windowIndex) {
    validateIndex(windowIndex);
    return _extrapolations.get(handleWrapping(windowIndex));
  }

  public synchronized int sampleCountsAtWindowIndex(long windowIndex) {
    validateIndex(windowIndex);
    return _counts[handleWrapping(windowIndex)];
  }

  /**
   * Clear the state of a given number of windows starting at the given window index.
   *
   * @param startingWindowIndex the starting index of the windows to reset.
   * @param numWindowIndexesToReset the number of windows to reset.
   */
  public synchronized void resetWindowIndexes(long startingWindowIndex, int numWindowIndexesToReset) {
    if (inValidRange(startingWindowIndex)
        || inValidRange(startingWindowIndex + numWindowIndexesToReset - 1)) {
      throw new IllegalStateException("Should never reset a window index that is in the valid range");
    }
    LOG.debug("Resetting window index [{}, {}]", startingWindowIndex,
              startingWindowIndex + numWindowIndexesToReset - 1);
    // We are not resetting all the data here. The data will be interpreted to 0 if count is 0.
    for (long i = startingWindowIndex; i < startingWindowIndex + numWindowIndexesToReset; i++) {
      int index = handleWrapping(i);
      _counts[index] = 0;
      _validity.clear(index);
      _extrapolations.clear(index);
    }
  }

  /**
   * Get the aggregated values of the given sorted set of windows. The result {@link ValuesAndExtrapolations} contains
   * the windows in the same order.
   *
   * @param windowIndexes the sorted set of windows to get values for.
   * @param metricDef the metric definitions.
   * @return the aggregated values and extrapolations of the given sorted set of windows in that order.
   */
  public synchronized ValuesAndExtrapolations aggregate(SortedSet<Long> windowIndexes, MetricDef metricDef) {
    return aggregate(windowIndexes, metricDef, true);
  }

  /**
   * Peek the value for the current window.
   *
   * @param metricDef the metric definitions.
   * @return the aggregated values and extrapolations of the given sorted set of windows in that order.
   */
  public synchronized ValuesAndExtrapolations peekCurrentWindow(long currentWindowIndex, MetricDef metricDef) {
    SortedSet<Long> window = new TreeSet<>();
    window.add(currentWindowIndex);
    return aggregate(window, metricDef, false);
  }

  private ValuesAndExtrapolations aggregate(SortedSet<Long> windowIndexes, MetricDef metricDef, boolean checkWindow) {
    if (_valuesByMetricId.isEmpty()) {
      return ValuesAndExtrapolations.empty(windowIndexes.size(), metricDef);
    }
    Map<Integer, MetricValues> aggValues = new HashMap<>();
    SortedMap<Integer, Extrapolation> extrapolations = new TreeMap<>();
    for (Map.Entry<Integer, float[]> entry : _valuesByMetricId.entrySet()) {
      int metricId = entry.getKey();
      float[] values = entry.getValue();
      MetricInfo info = metricDef.metricInfo(metricId);

      MetricValues aggValuesForMetric = new MetricValues(windowIndexes.size());
      aggValues.put(metricId, aggValuesForMetric);

      int resultIndex = 0;
      for (long windowIndex : windowIndexes) {
        // When we query the latest window, we need to skip the window validation because the valid windows do not
        // include the current active window.
        if (checkWindow) {
          validateIndex(windowIndex);
        }
        int idx = handleWrapping(windowIndex);
        // Sufficient samples
        if (_counts[idx] >= _minSamplesPerWindow) {
          aggValuesForMetric.set(resultIndex, getValue(info, idx, values));
        // Not quite sufficient, but have some available.
        } else if (_counts[idx] >= halfMinRequiredSamples()) {
          extrapolations.putIfAbsent(resultIndex, Extrapolation.AVG_AVAILABLE);
          aggValuesForMetric.set(resultIndex, getValue(info, idx, values));
        // Not sufficient, check the neighbors. The neighbors only exist when the index is not on the edge, i.e
        // neither the first nor last index.
        } else if (idx != firstIdx() && idx != lastIdx()
            && _counts[prevIdx(idx)] >= _minSamplesPerWindow
            && _counts[nextIdx(idx)] >= _minSamplesPerWindow) {
          extrapolations.putIfAbsent(resultIndex, Extrapolation.AVG_ADJACENT);
          int prevIdx = prevIdx(idx);
          int nextIdx = nextIdx(idx);
          double total = _valuesByMetricId.get(metricId)[prevIdx] + (_counts[idx] == 0 ? 0 : _valuesByMetricId.get(metricId)[idx])
              + _valuesByMetricId.get(metricId)[nextIdx];
          switch (info.strategy()) {
            case AVG:
              double counts = _counts[prevIdx] + _counts[idx] + _counts[nextIdx];
              aggValuesForMetric.set(resultIndex, total / counts);
              break;
            case MAX: // fall through.
            case LATEST:
              // for max and latest, we already only keep the largest or last value.
              aggValuesForMetric.set(resultIndex, _counts[idx] > 0 ?  total / 3 : total / 2);
              break;
            default:
              throw new IllegalStateException("Should never be here.");
          }
        // Neighbor not available, use the insufficient samples.
        } else if (_counts[idx] > 0) {
          aggValuesForMetric.set(resultIndex, getValue(info, idx, values));
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
   * @return the total number of samples added to this RawMetricValues.
   */
  public synchronized int numSamples() {
    int count = 0;
    for (int i : _counts) {
      count += i;
    }
    return count;
  }

  private float getValue(MetricInfo info, int index, float[] values) {
    if (_counts[index] == 0) {
      return 0;
    }
    switch (info.strategy()) {
      case AVG:
        return values[index] / _counts[index];
      case MAX:
      case LATEST:
        return values[index];
      default:
        throw new IllegalStateException("Should never be here.");
    }
  }

  private void updateValue(double newValue, MetricInfo info, int index) {
    switch (info.strategy()) {
      case AVG:
        add(newValue, info.id(), index);
        break;
      case MAX:
        max(newValue, info.id(), index);
        break;
      case LATEST:
        latest(newValue, info.id(), index);
        break;
      default:
        throw new IllegalStateException("Should never be here");
    }
  }

  private void add(double newValue, int metricId, int index) {
    _valuesByMetricId.get(metricId)[index] = (float) (_counts[index] == 0 ? newValue : _valuesByMetricId.get(metricId)[index] + newValue);
  }

  private void max(double newValue, int metricId, int index) {
    _valuesByMetricId.get(metricId)[index] = (float) (_counts[index] == 0 ? newValue : Math.max(
        _valuesByMetricId.get(metricId)[index], newValue));
  }

  private void latest(double newValue, int metricId, int index) {
    _valuesByMetricId.get(metricId)[index] = (float) newValue;
  }

  private void updateValidityAndExtrapolation(int index) {
    if (!updateEnoughSamples(index)) {
      if (!_extrapolations.get(index) && !updateAvailableAvg(index)) {
        if (!updateAvgAdjacent(index)) {
          if (!updateForcedInsufficient(index)) {
            _validity.clear(index);
            _extrapolations.clear(index);
          }
        }
      }
    }
  }

  private boolean updateEnoughSamples(int index) {
    if (_counts[index] == _minSamplesPerWindow) {
      _validity.set(index);
      _extrapolations.clear(index);
      return true;
    }
    return _counts[index] >= _minSamplesPerWindow;
  }

  private boolean updateAvailableAvg(int index) {
    if (_counts[index] == halfMinRequiredSamples()) {
      _validity.set(index);
      _extrapolations.set(index);
      return true;
    }
    return _counts[index] >= halfMinRequiredSamples();
  }

  private boolean updateAvgAdjacent(int index) {
    int prevIdx = prevIdx(index);
    int nextIdx = nextIdx(index);
    if (prevIdx < 0 || nextIdx < 0) {
      return false;
    }
    if (_counts[prevIdx] >= _minSamplesPerWindow
        && _counts[nextIdx] >= _minSamplesPerWindow) {
      _validity.set(index);
      _extrapolations.set(index);
      return true;
    }
    return false;
  }

  private boolean updateForcedInsufficient(int index) {
    if (_counts[index] > 0) {
      _validity.set(index);
      _extrapolations.set(index);
      return true;
    }
    return false;
  }

  private int prevIdx(int idx) {
    return idx == firstIdx() ? -1 : (idx + _counts.length - 1) % _counts.length;
  }

  private int nextIdx(int idx) {
    return idx == lastIdx() ? -1 : (idx + 1) % _counts.length;
  }

  private int firstIdx() {
    return handleWrapping(_oldestWindowIndex);
  }

  private int lastIdx() {
    return handleWrapping(currentWindowIndex() - 1);
  }

  private int halfMinRequiredSamples() {
    return Math.max(1, _minSamplesPerWindow / 2);
  }

  private void validateIndex(long windowIndex) {
    if (!inValidRange(windowIndex)) {
      throw new IllegalArgumentException(String.format("Index %d is out of range [%d, %d]", windowIndex,
                                                       _oldestWindowIndex, currentWindowIndex() - 1));
    }
  }

  private boolean inValidRange(long windowIndex) {
    return windowIndex >= _oldestWindowIndex && windowIndex <= currentWindowIndex() - 1;
  }

  private long currentWindowIndex() {
    return _oldestWindowIndex + _counts.length - 1;
  }

  private int handleWrapping(long index) {
    return (int) (index % _counts.length);
  }
}
