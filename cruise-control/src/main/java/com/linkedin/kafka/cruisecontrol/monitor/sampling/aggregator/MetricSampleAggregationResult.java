/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.monitor.sampling.Snapshot;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation.Imputation.FORCED_INSUFFICIENT;
import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation.Imputation.NO_VALID_IMPUTATION;


/**
 * This class contains the result of metric samples aggregation from
 * {@link KafkaMetricSampleAggregator#recentSnapshots(Cluster, long)}
 */
public class MetricSampleAggregationResult {
  private final Map<TopicPartition, Snapshot[]> _snapshots;
  private final Map<TopicPartition, List<SampleFlaw>> _sampleFlaws;
  private final long _generation;
  private final Set<TopicPartition> _invalidPartitions;

  MetricSampleAggregationResult(long generation) {
    _snapshots = new HashMap<>();
    _sampleFlaws = new HashMap<>();
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

  void recordPartitionWithSampleFlaw(TopicPartition tp, long snapshotWindow, SnapshotAndImputation.Imputation action) {
    List<SampleFlaw> sampleFlaws = _sampleFlaws.computeIfAbsent(tp, k -> new ArrayList<>());
    sampleFlaws.add(new SampleFlaw(snapshotWindow, action));
    if (action == FORCED_INSUFFICIENT || action == NO_VALID_IMPUTATION) {
      _invalidPartitions.add(tp);
    }
  }

  public Set<TopicPartition> invalidPartitions() {
    return Collections.unmodifiableSet(_invalidPartitions);
  }

  /**
   * The sample flaw for a partition that is still treated as valid.
   */
  public static class SampleFlaw {
    private final long _snapshotWindow;
    private final SnapshotAndImputation.Imputation _imputation;

    SampleFlaw(long snapshotWindow, SnapshotAndImputation.Imputation imputation) {
      _snapshotWindow = snapshotWindow;
      _imputation = imputation;
    }

    public long snapshotWindow() {
      return _snapshotWindow;
    }

    public SnapshotAndImputation.Imputation imputation() {
      return _imputation;
    }

    @Override
    public String toString() {
      return String.format("[%d, %s]", _snapshotWindow, _imputation);
    }
  }
}
