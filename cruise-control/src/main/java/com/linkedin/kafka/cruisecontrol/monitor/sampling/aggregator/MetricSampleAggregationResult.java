/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.kafka.cruisecontrol.monitor.sampling.Snapshot;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;


/**
 * This class contains the result of metric samples aggregation from
 * {@link MetricSampleAggregator#recentSnapshots(Cluster, long)}
 */
public class MetricSampleAggregationResult {
  private final Map<TopicPartition, Snapshot[]> _snapshots;
  private final Map<TopicPartition, List<SampleFlaw>> _sampleFlaws;
  private final boolean _includeAllTopics;
  private final long _generation;
  private final Set<TopicPartition> _invalidPartitions;

  MetricSampleAggregationResult(long generation,
                                boolean includeAllTopics) {
    _snapshots = new HashMap<>();
    _sampleFlaws = new HashMap<>();
    _includeAllTopics = includeAllTopics;
    _generation = generation;
    _invalidPartitions = new HashSet<>();
  }

  public Map<TopicPartition, Snapshot[]> snapshots() {
    return _snapshots;
  }

  public Map<TopicPartition, List<SampleFlaw>> sampleFlaws() {
    return _sampleFlaws;
  }

  public List<SampleFlaw> sampleFlaw(TopicPartition tp) {
    return _sampleFlaws.getOrDefault(tp, Collections.emptyList());
  }

  public long generation() {
    return _generation;
  }

  void addPartitionSnapshots(TopicPartition tp, Snapshot[] snapshots) {
    _snapshots.put(tp, snapshots);
  }

  void recordPartitionWithSampleFlaw(TopicPartition tp, long snapshotWindow, Imputation action) {
    List<SampleFlaw> sampleFlaws = _sampleFlaws.computeIfAbsent(tp, k -> new ArrayList<>());
    sampleFlaws.add(new SampleFlaw(snapshotWindow, action));
    if (action == Imputation.FORCED_INSUFFICIENT || action == Imputation.FORCED_UNKNOWN) {
      _invalidPartitions.add(tp);
    }
  }

  void discardAllSnapshots() {
    _snapshots.clear();
    _invalidPartitions.clear();
  }

  boolean includeAllTopics() {
    return _includeAllTopics;
  }

  public Set<TopicPartition> invalidPartitions() {
    return Collections.unmodifiableSet(_invalidPartitions);
  }

  MetricSampleAggregationResult merge(MetricSampleAggregationResult other) {
    this._snapshots.putAll(other.snapshots());
    _invalidPartitions.addAll(other._invalidPartitions);
    for (Map.Entry<TopicPartition, List<SampleFlaw>> entry : other.sampleFlaws().entrySet()) {
      this._sampleFlaws.compute(entry.getKey(), (k, v) -> {
        if (v == null) {
          return entry.getValue();
        } else {
          v.addAll(entry.getValue());
          return v;
        }
      });
    }
    return this;
  }

  /**
   * There are a few imputations we will do when there is not sufficient samples in a snapshot window for a
   * partition. The imputations are used in the following preference order.
   * <ul>
   *     <li>AVG_AVAILABLE: The average of available samples even though there are more than half of the required samples.</li>
   *     <li>AVG_ADJACENT: The average value of the current snapshot and the two adjacent snapshot windows</li>
   *     <li>PREV_PERIOD: The samples from the previous period is used.</li>
   *     <li>FORCED_INSUFFICIENT: The sample is forced to be included with insufficient data.</li>
   *     <li>FORCED_UNKNOWN: The sample is forced to be included and the original value was unknown.</li>
   *     <li>NO_VALID_IMPUTATION: there is no valid imputation</li>
   * </ul>
   */
  public enum Imputation {
    AVG_AVAILABLE, PREV_PERIOD, AVG_ADJACENT, FORCED_INSUFFICIENT, FORCED_UNKNOWN, NO_VALID_IMPUTATION
  }

  /**
   * The sample flaw for a partition that is still treated as valid.
   */
  public static class SampleFlaw {
    private final long _snapshotWindow;
    private final Imputation _imputation;

    SampleFlaw(long snapshotWindow, Imputation imputation) {
      _snapshotWindow = snapshotWindow;
      _imputation = imputation;
    }

    public long snapshotWindow() {
      return _snapshotWindow;
    }

    public Imputation imputation() {
      return _imputation;
    }

    @Override
    public String toString() {
      return String.format("[%d, %s]", _snapshotWindow, _imputation);
    }
  }
}
