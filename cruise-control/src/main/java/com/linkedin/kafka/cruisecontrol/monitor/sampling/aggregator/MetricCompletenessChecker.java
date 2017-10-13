/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;


/**
 * A class that helps compute the completeness of the metrics in the {@link MetricSampleAggregator}
 */
public class MetricCompletenessChecker {
  private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(MetricCompletenessChecker.class);
  // The following two data structures help us to quickly identify how many valid partitions are there in each window.
  private final ConcurrentSkipListMap<Long, Map<String, Set<Integer>>> _validPartitionsPerTopicByWindows;
  private final SortedMap<Long, Integer> _validPartitionsByWindows;
  private final int _maxNumSnapshots;
  private volatile ModelGeneration _modelGeneration;
  private volatile long _activeSnapshotWindow;

  public MetricCompletenessChecker(int maxNumSnapshots) {
    _validPartitionsPerTopicByWindows = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
    _validPartitionsByWindows = new TreeMap<>(Comparator.reverseOrder());
    _modelGeneration = null;
    _maxNumSnapshots = maxNumSnapshots;
  }

  /**
   * Get the number of valid windows that meets the the minimum monitored partitions percentage requirement.
   *
   * @param minMonitoredPartitionsPercentage the minimum monitored partitions percentage.
   * @param totalNumPartitions the total number of partitions.
   * @return the number of the most recent valid windows.
   */
  synchronized public int numValidWindows(ModelGeneration modelGeneration,
                                          Cluster cluster,
                                          double minMonitoredPartitionsPercentage,
                                          int totalNumPartitions) {
    updateMetricCompleteness(cluster, modelGeneration);
    int i = 0;
    double minMonitoredNumPartitions = totalNumPartitions * minMonitoredPartitionsPercentage;
    for (Map.Entry<Long, Integer> entry : _validPartitionsByWindows.entrySet()) {
      long window = entry.getKey();
      if ((entry.getValue() < minMonitoredNumPartitions && window != _activeSnapshotWindow) || i == _maxNumSnapshots) {
        break;
      }
      if (window != _activeSnapshotWindow) {
        i++;
      }
    }
    return i;
  }

  synchronized public SortedMap<Long, Double> monitoredPercentages(ModelGeneration modelGeneration,
                                                                   Cluster cluster,
                                                                   int totalNumPartitions) {
    updateMetricCompleteness(cluster, modelGeneration);
    TreeMap<Long, Double> percentages = new TreeMap<>();
    for (Map.Entry<Long, Integer> entry : _validPartitionsByWindows.entrySet()) {
      percentages.put(entry.getKey(), (double) entry.getValue() / totalNumPartitions);
    }
    return percentages;
  }

  /**
   * Get number of snapshot windows in a period.
   */
  synchronized public int numWindows(long from, long to) {
    int i = 0;
    long activeSnapshotWindow = _activeSnapshotWindow;
    for (long window : _validPartitionsByWindows.keySet()) {
      // Exclude the active window.
      if (window >= from && window <= to && window != activeSnapshotWindow) {
        i++;
      }
    }
    return i;
  }

  synchronized void refreshAllPartitionCompleteness(MetricSampleAggregator aggregator,
                                                    Set<Long> windows,
                                                    Set<TopicPartition> partitions) {
    _validPartitionsPerTopicByWindows.clear();
    for (long window : windows) {
      for (TopicPartition tp : partitions) {
        updatePartitionCompleteness(aggregator, window, tp);
      }
    }
    // We need to reset the model generation here. This is because previously we did not populate the partition completeness
    // map and user may have queried and set the model generation to be up to date.
    _modelGeneration = null;
  }

  /**
   * Remove a snapshot window that has been evicted from the metric sample aggregator.
   */
  void removeWindow(long snapshotWindow) {
    _validPartitionsPerTopicByWindows.remove(snapshotWindow);
  }

  /**
   * Update the valid partition number of a topic for a window.
   */
  void updatePartitionCompleteness(MetricSampleAggregator aggregator,
                                   long window,
                                   TopicPartition tp) {
    _activeSnapshotWindow = aggregator.activeSnapshotWindow();
    _validPartitionsPerTopicByWindows.computeIfAbsent(window, w -> new ConcurrentHashMap<>())
                                     .compute(tp.topic(), (t, set) -> {
                                       Set<Integer> s = set == null ? new HashSet<>() : set;
                                       MetricSampleAggregationResult.Imputation imputation = aggregator.validatePartitions(window, tp);
                                       if (imputation != MetricSampleAggregationResult.Imputation.NO_VALID_IMPUTATION) {
                                         LOG.debug("Added partition {} to valid partition set for window {} with imputation {}",
                                                   tp, window, imputation);
                                         synchronized (s) {
                                           s.add(tp.partition());
                                         }
                                       }
                                       return s;
                                     });
  }

  private void updateMetricCompleteness(Cluster cluster,
                                        ModelGeneration modelGeneration) {
    if (_modelGeneration == null || !_modelGeneration.equals(modelGeneration)) {
      _modelGeneration = modelGeneration;
      _validPartitionsByWindows.clear();
      for (Map.Entry<Long, Map<String, Set<Integer>>> entry : _validPartitionsPerTopicByWindows.entrySet()) {
        long window = entry.getKey();
        for (String topic : entry.getValue().keySet()) {
          updateWindowMetricCompleteness(cluster, window, topic);
        }
      }
    }
  }

  private void updateWindowMetricCompleteness(Cluster cluster, long window, String topic) {
    int numValidPartitions = _validPartitionsPerTopicByWindows.get(window).get(topic).size();
    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    // The topic may have been deleted so the cluster does not have it.
    if (partitions != null) {
      int numPartitions = partitions.size();
      _validPartitionsByWindows.compute(window, (w, v) -> {
        int newValue = (v == null ? 0 : v);
        return numValidPartitions == numPartitions ? newValue + numPartitions : newValue;
      });
    }
  }
}
