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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A metric array that holds a metric for all the windows.
 */
public class RawMetricValues {
  private static final Logger LOG = LoggerFactory.getLogger(RawMetricValues.class);
  private final int _minSamplesPerSnapshotWindow;
  private final Map<Integer, float[]> _values;
  private final short[] _counts;
  private final BitSet _imputations;
  private final BitSet _validity;
  private long _oldestWindowIndex;

  public RawMetricValues(int numWindows, int minSamplesPerSnapshotWindow) {
    if (numWindows == 1) {
      throw new IllegalArgumentException("The number of windows should be at least 2 because at least one stable"
                                             + " window and one current window are needed.");
    }
    _values = new HashMap<>();
    _counts = new short[numWindows];
    _imputations = new BitSet(numWindows);
    _validity = new BitSet(numWindows);
    _minSamplesPerSnapshotWindow = minSamplesPerSnapshotWindow;
    _oldestWindowIndex = Long.MAX_VALUE;
  }

  public synchronized void addSample(MetricSample<?, ?> sample, long windowIndex, MetricDef metricDef) {
    // This sample is being added during window rolling.
    if (windowIndex < _oldestWindowIndex) {
      return;
    } else if (windowIndex > _oldestWindowIndex + _counts.length) {
      throw new IllegalArgumentException("Cannot add sample to window index " + windowIndex + ", which is larger "
                                             + "than the current window index " + currentWindowIndex());
    }
    int idx = (int) (windowIndex % _counts.length);
    for (Map.Entry<Integer, Double> entry : sample.allMetrics().entrySet()) {
      _values.computeIfAbsent(entry.getKey(), k -> new float[_counts.length]);
      updateValue(entry.getValue(), metricDef.metricInfo(entry.getKey()), idx);
    }
    _counts[idx]++;
    updateValidityAndImputation(idx);
    if (_counts[idx] >= _minSamplesPerSnapshotWindow) {
      _imputations.clear(idx);
      if (idx != firstIdx()) {
        int prevIdx = prevIdx(idx);
        if (_counts[prevIdx] < halfMinRequiredSamples()) {
          updateAvgAdjacent(prevIdx);
        }
      }
      if (idx != lastIdx()) {
        int nextIdx = nextIdx(idx);
        if (_counts[nextIdx] < halfMinRequiredSamples()) {
          updateAvgAdjacent(nextIdx);
        }
      }
    }
    LOG.trace("Added metric sample {} to window index {}, actual index is {}, current count : {}",
              sample, windowIndex, idx, _counts[idx]);
  }

  public synchronized void updateOldestWindowIndex(long newOldestWindowIndex) {
    long prevLastWindowIndex = currentWindowIndex() - 1;
    _oldestWindowIndex = newOldestWindowIndex;
    // Advancing the oldest window index will make the previous current window index become available to its
    // neighbour index (i.e. the previous last index) for AVG_ADJACENT imputation. We don't need to update the
    // previous current window index because it would be up to date during the addSample call.
    if (prevLastWindowIndex >= _oldestWindowIndex) {
      updateValidityAndImputation(handleWrapping(prevLastWindowIndex));
    }
  }

  public synchronized boolean isValid(float maxAllowedImputation) {
    int currentIdx = handleWrapping(currentWindowIndex());
    int numValidIndexesAdjustment = _validity.get(handleWrapping(currentIdx)) ? 1 : 0;
    boolean allIndexesValid = _validity.cardinality() - numValidIndexesAdjustment == _counts.length - 1;

    boolean tooManyImputations = numImputations() > maxAllowedImputation;

    return allIndexesValid && !tooManyImputations;
  }

  public synchronized int numImputations() {
    int currentIdx = handleWrapping(currentWindowIndex());
    int numImputationAdjustment = _imputations.get(currentIdx) ? 1 : 0;
    return _imputations.cardinality() - numImputationAdjustment;
  }

  public synchronized boolean isValidAtWindowIndex(long windowIndex) {
    validateIndex(windowIndex);
    return _validity.get(handleWrapping(windowIndex));
  }

  public synchronized boolean hasImputationAtWindowIndex(long windowIndex) {
    validateIndex(windowIndex);
    return _imputations.get(handleWrapping(windowIndex));
  }

  public synchronized int sampleCountsAtWindowIndex(long windowIndex) {
    validateIndex(windowIndex);
    return _counts[handleWrapping(windowIndex)];
  }

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
      _imputations.clear(index);
    }
  }

  public synchronized ValuesAndImputations aggregate(SortedSet<Long> windowIndexes, MetricDef metricDef) {
    if (_values.isEmpty()) {
      return ValuesAndImputations.empty(windowIndexes.size(), metricDef);
    }
    Map<Integer, MetricValues> aggValues = new HashMap<>();
    SortedMap<Integer, Imputation> imputations = new TreeMap<>();
    for (Map.Entry<Integer, float[]> entry : _values.entrySet()) {
      int metricId = entry.getKey();
      float[] values = entry.getValue();
      MetricInfo info = metricDef.metricInfo(metricId);

      MetricValues aggValuesForMetric = new MetricValues(windowIndexes.size());
      aggValues.put(metricId, aggValuesForMetric);

      int resultIndex = 0;
      for (long windowIndex : windowIndexes) {
        validateIndex(windowIndex);
        int idx = handleWrapping(windowIndex);
        // Sufficient samples
        if (_counts[idx] >= _minSamplesPerSnapshotWindow) {
          aggValuesForMetric.set(resultIndex, getValue(info, idx, values));
        // Not quite sufficient, but have some available.
        } else if (_counts[idx] >= halfMinRequiredSamples()) {
          if (info.id() == 0) {
            imputations.put(resultIndex, Imputation.AVG_AVAILABLE);
          }
          aggValuesForMetric.set(resultIndex, getValue(info, idx, values));
        // Not sufficient, check the neighbors. The neighbors only exists when the index is not on the edge, i.e
        // neither the first nor last index.
        } else if (idx != firstIdx() && idx != lastIdx()
            && _counts[prevIdx(idx)] >= _minSamplesPerSnapshotWindow
            && _counts[nextIdx(idx)] >= _minSamplesPerSnapshotWindow) {
          if (info.id() == 0) {
            imputations.put(resultIndex, Imputation.AVG_ADJACENT);
          }
          int prevIdx = prevIdx(idx);
          int nextIdx = nextIdx(idx);
          double total = _values.get(metricId)[prevIdx] + (_counts[idx] == 0 ? 0 : _values.get(metricId)[idx])
              + _values.get(metricId)[nextIdx];
          switch (info.strategy()) {
            case AVG:
              double counts = _counts[prevIdx] + _counts[idx] + _counts[nextIdx];
              aggValuesForMetric.set(resultIndex, (float) (total / counts));
              break;
            case MAX:
            case LATEST:
              aggValuesForMetric.set(resultIndex, _counts[idx] > 0 ? (float) (total / 3) : (float) (total / 2));
              break;
            default:
              throw new IllegalStateException("Should never be here.");
          }
        // Neighbor not available, use the insufficient samples.
        } else if (_counts[idx] > 0) {
          aggValuesForMetric.set(resultIndex, getValue(info, idx, values));
          if (info.id() == 0) {
            imputations.put(resultIndex, Imputation.FORCED_INSUFFICIENT);
          }
        // Nothing is available, just return all 0 and NO_VALID_IMPUTATION.
        } else {
          aggValuesForMetric.set(resultIndex, 0);
          if (info.id() == 0) {
            imputations.put(resultIndex, Imputation.NO_VALID_IMPUTATION);
          }
        }
        resultIndex++;
      }
    }
    return new ValuesAndImputations(new AggregatedMetricValues(aggValues), imputations);
  }

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
    _values.get(metricId)[index] = (float) (_counts[index] == 0 ? newValue : _values.get(metricId)[index] + newValue);
  }

  private void max(double newValue, int metricId, int index) {
    _values.get(metricId)[index] = (float) (_counts[index] == 0 ? newValue : Math.max(_values.get(metricId)[index], newValue));
  }

  private void latest(double newValue, int metricId, int index) {
    _values.get(metricId)[index] = (float) newValue;
  }

  private void updateValidityAndImputation(int index) {
    if (!updateEnoughSamples(index)) {
      if (!_imputations.get(index) && !updateAvailableAvg(index)) {
        if (!updateAvgAdjacent(index)) {
          if (!updateForcedInsufficient(index)) {
            _validity.clear(index);
            _imputations.clear(index);
          }
        }
      }
    }
  }

  private boolean updateEnoughSamples(int index) {
    if (_counts[index] == _minSamplesPerSnapshotWindow) {
      _validity.set(index);
      _imputations.clear(index);
      return true;
    }
    return _counts[index] >= _minSamplesPerSnapshotWindow;
  }

  private boolean updateAvailableAvg(int index) {
    if (_counts[index] == halfMinRequiredSamples()) {
      _validity.set(index);
      _imputations.set(index);
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
    if (_counts[prevIdx] >= _minSamplesPerSnapshotWindow
        && _counts[nextIdx] >= _minSamplesPerSnapshotWindow) {
      _validity.set(index);
      _imputations.set(index);
      return true;
    }
    return false;
  }

  private boolean updateForcedInsufficient(int index) {
    if (_counts[index] > 0) {
      _validity.set(index);
      _imputations.set(index);
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
    return Math.max(1, _minSamplesPerSnapshotWindow / 2);
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
