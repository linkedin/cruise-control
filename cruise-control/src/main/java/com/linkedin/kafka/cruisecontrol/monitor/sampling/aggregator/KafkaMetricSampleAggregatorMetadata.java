/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.common.LongGenerationed;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.WindowAggregationResult;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.kafka.common.TopicPartition;


/**
 * The metadata for {@link KafkaMetricSampleAggregator}. It helps maintain the completeness information of each
 * snapshot window.
 */
class KafkaMetricSampleAggregatorMetadata extends LongGenerationed {
  private final Set<String> _invalidTopics;
  private final Set<String> _topicsWithTooManyImputations;
  private final Map<Long, WindowCompleteness> _completenessByWindows;
  private final double _maxAllowedImputations;

  private volatile int _clusterGeneration = -1;
  private volatile int _numValidPartitions = 0;

  KafkaMetricSampleAggregatorMetadata(long generation, double maxAllowedImputations) {
    super(generation);
    _topicsWithTooManyImputations = new HashSet<>();
    _invalidTopics = new HashSet<>();
    _completenessByWindows = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
    _maxAllowedImputations = maxAllowedImputations;
  }

  boolean generationsMatch(long generation, int clusterGeneration) {
    return compareGeneration(generation) == 0 && _clusterGeneration == clusterGeneration;
  }

  /**
   * Update the window completeness.
   */
  synchronized void update(long generation,
                           int clusterGeneration,
                           List<WindowAggregationResult<TopicPartition>> windowResults) {
    if (!generationsMatch(generation, clusterGeneration)) {
      updateInvalidTopics(windowResults);
      updateImputations(windowResults);
      long maxGeneration = updateWindowCompleteness(windowResults);
      setGeneration(maxGeneration);
      _clusterGeneration = clusterGeneration;
    }
  }

  synchronized int numValidPartitions() {
    return _numValidPartitions;
  }

  /**
   * Get the available windows and valid snapshots that meets the monitored percentage requirements.
   * @param minMonitoredPartitionsPercentage the minimum required monitored percentage.
   * @param totalNumPartitions the total number of partitions.
   * @return An {@link AvailableWindows} containing the information of windows that meets the monitored 
   * partitions percentage requirements.
   */
  synchronized AvailableWindows validWindows(double minMonitoredPartitionsPercentage, 
                                             int totalNumPartitions) {
    SortedSet<Long> windows = new TreeSet<>(Comparator.reverseOrder());
    Map<String, Integer> commonValidTopics = new HashMap<>();
    int minMonitoredPartitions = (int) (totalNumPartitions * minMonitoredPartitionsPercentage);
    int numCommonValidPartitions = 0;
    
    for (Map.Entry<Long, WindowCompleteness> entry : _completenessByWindows.entrySet()) {
      WindowCompleteness completeness = entry.getValue();
      // Only do further check if the window local completeness meets requirement.
      if ((double) completeness.numValidPartitionsLocal() >= minMonitoredPartitions) {
        // First valid window.
        if (commonValidTopics.isEmpty()) {
          windows.add(entry.getKey());
          commonValidTopics.putAll(completeness.validPartitionsByTopic());
          numCommonValidPartitions = completeness.numValidPartitionsLocal();
        } else {
          Set<String> topicsToRemove = 
              maybeIncludeWindow(completeness, commonValidTopics, numCommonValidPartitions, minMonitoredPartitions);
          if (topicsToRemove != null) {
            for (String topic : topicsToRemove) {
              numCommonValidPartitions -= commonValidTopics.remove(topic);
            }
            windows.add(entry.getKey());
          }
        }
      }
    }
    return new AvailableWindows(windows, commonValidTopics.keySet());
  }

  synchronized SortedMap<Long, Double> monitoredPercentagesByWindows(int totalNumPartitions) {
    TreeMap<Long, Double> percentages = new TreeMap<>(Comparator.reverseOrder());
    for (Map.Entry<Long, WindowCompleteness> entry : _completenessByWindows.entrySet()) {
      percentages.put(entry.getKey(), (double) entry.getValue().numValidPartitionsLocal() / totalNumPartitions);
    }
    return percentages;
  }

  synchronized Set<String> invalidTopics() {
    return _invalidTopics;
  }

  synchronized Set<String> topicsWithTooManyImputations() {
    return _topicsWithTooManyImputations;
  }

  private void updateInvalidTopics(List<WindowAggregationResult<TopicPartition>> windowResults) {
    _invalidTopics.clear();
    for (WindowAggregationResult<TopicPartition> windowResult : windowResults) {
      Set<TopicPartition> invalidPartitions = windowResult.invalidEntities().keySet();
      for (TopicPartition tp : invalidPartitions) {
        _invalidTopics.add(tp.topic());
      }
    }
  }

  private void updateImputations(List<WindowAggregationResult<TopicPartition>> windowResults) {
    _topicsWithTooManyImputations.clear();
    Map<TopicPartition, Integer> imputationsByPartition = new HashMap<>();
    for (WindowAggregationResult<TopicPartition> windowResult : windowResults) {
      for (Map.Entry<TopicPartition, SnapshotAndImputation.Imputation> entry : windowResult.entityWithImputations().entrySet()) {
        TopicPartition tp = entry.getKey();
        if (!_invalidTopics.contains(tp.topic())) {
          int numImputations = imputationsByPartition.compute(tp, (p, count) -> count == null ? 1 : count + 1);
          if (numImputations > _maxAllowedImputations) {
            _topicsWithTooManyImputations.add(tp.topic());
          }
        }
      }
    }
  }

  /**
   * Update the window completeness of each snapshot window.
   * @return the maximum generation of all the window results.
   */
  private long updateWindowCompleteness(List<WindowAggregationResult<TopicPartition>> windowResults) {
    _completenessByWindows.clear();
    _numValidPartitions = Integer.MAX_VALUE;
    long generation = -1L;
    for (WindowAggregationResult<TopicPartition> windowResult : windowResults) {
      _numValidPartitions = Math.min(_numValidPartitions,
                                     _completenessByWindows.computeIfAbsent(windowResult.window(),
                                                                            w -> new WindowCompleteness())
                                                           .update(_invalidTopics, _topicsWithTooManyImputations,
                                                                   windowResult));
      generation = Long.max(generation, windowResult.generation());
    }
    return generation;
  }

  /**
   * Check if including the window will still meet the min monitored partitions requirement.
   * 
   * @return A non-null set if the window should be included. Otherwise null will be returned.
   */
  private Set<String> maybeIncludeWindow(WindowCompleteness windowCompleteness, 
                                         Map<String, Integer> currentCommonValidTopics, 
                                         int currentNumCommonValidPartitions, 
                                         int minMonitoredPartitions) {

    Set<String> topicsToRemove = new HashSet<>();
    int numValidPartitionsToReduce = 0;
    for (Map.Entry<String, Integer> entry: currentCommonValidTopics.entrySet()) {
      if (!windowCompleteness.validPartitionsByTopic().containsKey(entry.getKey())) {
        numValidPartitionsToReduce += entry.getValue();
        if (currentNumCommonValidPartitions - numValidPartitionsToReduce < minMonitoredPartitions) {
          return null;
        }
        topicsToRemove.add(entry.getKey());
      }
    }
    return topicsToRemove;
  }
  
  public static class AvailableWindows {
    private final SortedSet<Long> _availableWindows;
    private final Set<String> _validTopics;
    
    private AvailableWindows(SortedSet<Long> availableWindows, Set<String> validTopics) {
      _availableWindows = availableWindows;
      _validTopics = validTopics;
    }

    public SortedSet<Long> availableWindows() {
      return _availableWindows;
    }
    
    public Set<String> validTopics() {
      return _validTopics;
    }
  }

  private static class WindowCompleteness {
    private final Map<String, Integer> _validPartitionsByTopic;
    private int _numValidPartitionsLocal;

    WindowCompleteness() {
      _validPartitionsByTopic = new HashMap<>();
    }
    
    private int update(Set<String> invalidTopics,
                       Set<String> topicsWithTooManyImputations,
                       WindowAggregationResult<TopicPartition> windowResult) {
      _validPartitionsByTopic.clear();
      _numValidPartitionsLocal = 0;
      int numValidPartitionsGlobal = 0;
      Set<String> invalidTopicsInWindow = new HashSet<>();
      for (TopicPartition tp : windowResult.invalidEntities().keySet()) {
        invalidTopicsInWindow.add(tp.topic());
      }
      Set<TopicPartition> partitions = windowResult.snapshots().keySet();
      for (TopicPartition tp : partitions) {
        if (!topicsWithTooManyImputations.contains(tp.topic())) {
          if (!invalidTopicsInWindow.contains(tp.topic())) {
            _numValidPartitionsLocal++;
            _validPartitionsByTopic.compute(tp.topic(), (t, count) -> count == null ? 1 : count + 1);
          }
          if (!invalidTopics.contains(tp.topic())) {
            numValidPartitionsGlobal++; 
          }
        }
      }
      return numValidPartitionsGlobal;
    }
    
    private Map<String, Integer> validPartitionsByTopic() {
      return _validPartitionsByTopic;
    }

    /**
     * The number of valid partitions for this window. The valid partitions are the partitions that
     * 1) not belonging to a invalid topic in this window, and
     * 2) without too many imputations in all the windows.
     */
    private int numValidPartitionsLocal() {
      return _numValidPartitionsLocal;
    }
  }
}
