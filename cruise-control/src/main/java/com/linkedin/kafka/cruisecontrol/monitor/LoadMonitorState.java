/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import com.linkedin.kafka.cruisecontrol.model.LinearRegressionModelParameters;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregationResult;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner.LoadMonitorTaskRunnerState;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import org.apache.kafka.common.TopicPartition;

public class LoadMonitorState {

  private final LoadMonitorTaskRunnerState _loadMonitorTaskRunnerState;
  private final int _numValidSnapshotWindows;
  private final SortedMap<Long, Double> _monitoredSnapshotWindows;
  private final int _numValidMonitoredPartitions;
  private final Map<TopicPartition, List<MetricSampleAggregationResult.SampleFlaw>> _sampleFlaws;
  private final int _totalNumPartitions;
  private final double _bootstrapProgress;
  private final double _loadingProgress;

  private LoadMonitorState(LoadMonitorTaskRunnerState state,
                           int numValidSnapshotWindows,
                           SortedMap<Long, Double> monitoredSnapshotWindows,
                           int numValidMonitoredPartitions,
                           int totalNumPartitions,
                           Map<TopicPartition, List<MetricSampleAggregationResult.SampleFlaw>> sampleFlaws,
                           double bootstrapProgress,
                           double loadingProgress) {
    _loadMonitorTaskRunnerState = state;
    _numValidSnapshotWindows = numValidSnapshotWindows;
    _monitoredSnapshotWindows = monitoredSnapshotWindows;
    _numValidMonitoredPartitions = numValidMonitoredPartitions;
    _sampleFlaws = sampleFlaws;
    _totalNumPartitions = totalNumPartitions;
    _bootstrapProgress = bootstrapProgress;
    _loadingProgress = loadingProgress;
  }

  public static LoadMonitorState notStarted() {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.NOT_STARTED, 0, null, 0, -1, null, -1.0, -1.0);
  }

  public static LoadMonitorState running(int numValidSnapshotWindows,
                                         SortedMap<Long, Double> numMonitoredSnapshotWindows,
                                         int numValidMonitoredTopics,
                                         int totalNumPartitions,
                                         Map<TopicPartition, List<MetricSampleAggregationResult.SampleFlaw>> sampleFlaws) {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.RUNNING,
                                numValidSnapshotWindows,
                                numMonitoredSnapshotWindows,
                                numValidMonitoredTopics,
                                totalNumPartitions,
                                sampleFlaws,
                                -1.0,
                                -1.0);
  }

  public static LoadMonitorState paused(int numValidSnapshotWindows,
                                        SortedMap<Long, Double> numMonitoredSnapshotWindows,
                                        int numValidMonitoredTopics,
                                        int totalNumPartitions,
                                        Map<TopicPartition, List<MetricSampleAggregationResult.SampleFlaw>> sampleFlaws) {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.PAUSED,
                                numValidSnapshotWindows,
                                numMonitoredSnapshotWindows,
                                numValidMonitoredTopics,
                                totalNumPartitions,
                                sampleFlaws,
                                -1.0,
                                -1.0);
  }

  public static LoadMonitorState sampling(int numValidSnapshotWindows,
                                          SortedMap<Long, Double> numMonitoredSnapshotWindows,
                                          int numValidMonitoredTopics,
                                          int totalNumPartitions,
                                          Map<TopicPartition, List<MetricSampleAggregationResult.SampleFlaw>> sampleFlaws) {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.SAMPLING,
                                numValidSnapshotWindows,
                                numMonitoredSnapshotWindows,
                                numValidMonitoredTopics,
                                totalNumPartitions,
                                sampleFlaws,
                                -1.0,
                                -1.0);
  }

  public static LoadMonitorState bootstrapping(int numValidSnapshotWindows,
                                               SortedMap<Long, Double> numMonitoredSnapshotWindows,
                                               int numValidMonitoredTopics,
                                               int totalNumPartitions,
                                               double bootstrapProgress,
                                               Map<TopicPartition, List<MetricSampleAggregationResult.SampleFlaw>> sampleFlaws) {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.BOOTSTRAPPING,
                                numValidSnapshotWindows,
                                numMonitoredSnapshotWindows,
                                numValidMonitoredTopics,
                                totalNumPartitions,
                                sampleFlaws,
                                bootstrapProgress,
                                -1.0);
  }

  public static LoadMonitorState training(int numValidSnapshotWindows,
                                          SortedMap<Long, Double> numMonitoredSnapshotWindows,
                                          int numValidMonitoredTopics,
                                          int totalNumPartitions,
                                          Map<TopicPartition, List<MetricSampleAggregationResult.SampleFlaw>> sampleFlaws) {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.TRAINING,
                                numValidSnapshotWindows,
                                numMonitoredSnapshotWindows,
                                numValidMonitoredTopics,
                                totalNumPartitions,
                                sampleFlaws,
                                -1.0,
                                -1.0);
  }

  public static LoadMonitorState loading(int numValidSnapshotWindows,
                                         SortedMap<Long, Double> numMonitoredSnapshotWindows,
                                         int numValidMonitoredTopics,
                                         int totalNumPartitions,
                                         double loadingProgress) {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.LOADING,
                                numValidSnapshotWindows,
                                numMonitoredSnapshotWindows,
                                numValidMonitoredTopics,
                                totalNumPartitions,
                                Collections.emptyMap(),
                                -1.0,
                                loadingProgress);
  }

  @Override
  public String toString() {
    String trained = ModelParameters.trainingCompleted()
        ? "(TRAINED)" : String.format("(%.3f%% trained)",
                                      ModelParameters.modelCoefficientTrainingCompleteness() * 100);
    float validPartitionPercent = (float) _numValidMonitoredPartitions / _totalNumPartitions;
    float validWindowPercent = (float) _numValidSnapshotWindows / _monitoredSnapshotWindows.size();
    switch (_loadMonitorTaskRunnerState) {
      case NOT_STARTED:
        return String.format("{state: %s}", _loadMonitorTaskRunnerState);

      case RUNNING:
      case SAMPLING:
      case PAUSED:
        return String.format("{state: %s%s, NumValidWindows: (%d/%d) (%.3f%%), NumValidPartitions: %d/%d (%.3f%%), flawedPartitions: %d}",
                             _loadMonitorTaskRunnerState, trained, _numValidSnapshotWindows,
                             _monitoredSnapshotWindows.size(), validWindowPercent * 100,
                             _numValidMonitoredPartitions, _totalNumPartitions, validPartitionPercent * 100,
                             _sampleFlaws.size());
      case BOOTSTRAPPING:
        return String.format("{state: %s%s, BootstrapProgress: %.3f%%, NumValidWindows: (%d/%d) (%.3f%%), "
                                 + "NumValidPartitions: %d/%d (%.3f%%), flawedPartitions: %d}",
                             _loadMonitorTaskRunnerState, trained, _bootstrapProgress * 100,
                             _numValidSnapshotWindows, _monitoredSnapshotWindows.size(), validWindowPercent * 100,
                             _numValidMonitoredPartitions, _totalNumPartitions, validPartitionPercent * 100, _sampleFlaws.size());

      case TRAINING:
        return String.format("{state: %s%s, NumValidWindows: (%d/%d) (%.3f%%) , NumValidPartitions: %d/%d (%.3f%%), FlawedPartitions: %d}",
                             _loadMonitorTaskRunnerState, trained, _monitoredSnapshotWindows.size(),
                             _numValidSnapshotWindows, validWindowPercent * 100, _numValidMonitoredPartitions,
                             _totalNumPartitions, validPartitionPercent * 100, _sampleFlaws.size());

      case LOADING:
        return String.format("{state: %s%s, LoadingProgress: %.3f%%, NumValidWindows: (%d/%d): (%.3f%%), "
                                 + "NumValidPartitions: %d/%d (%.3f%%), FlawedPartitions: %d}",
                             _loadMonitorTaskRunnerState, trained, _loadingProgress * 100, _numValidSnapshotWindows,
                             _monitoredSnapshotWindows.size(), validWindowPercent * 100,
                             _numValidMonitoredPartitions, _totalNumPartitions, validPartitionPercent * 100,
                             _sampleFlaws.size());
      default:
        throw new IllegalStateException("Should never be here");
    }
  }

  public LoadMonitorTaskRunnerState state() {
    return _loadMonitorTaskRunnerState;
  }

  public SortedMap<Long, Double> monitoredSnapshotWindows() {
    return _monitoredSnapshotWindows;
  }

  public int numValidMonitoredTopics() {
    return _numValidMonitoredPartitions;
  }

  public double monitoringCoverage() {
    return (double) _numValidMonitoredPartitions / _totalNumPartitions;
  }

  public double bootstrapProgress() {
    return _bootstrapProgress;
  }

  public Map<TopicPartition, List<MetricSampleAggregationResult.SampleFlaw>> sampleFlaws() {
    return _sampleFlaws;
  }

  public LinearRegressionModelParameters.LinearRegressionModelState detailTrainingProgress() {
    return ModelParameters.linearRegressionModelState();
  }
}
