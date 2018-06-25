/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import com.linkedin.kafka.cruisecontrol.model.LinearRegressionModelParameters;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.SampleExtrapolation;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner.LoadMonitorTaskRunnerState;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import org.apache.kafka.common.TopicPartition;
import java.util.HashMap;

public class LoadMonitorState {

  private final LoadMonitorTaskRunnerState _loadMonitorTaskRunnerState;
  private final int _numValidWindows;
  private final SortedMap<Long, Float> _monitoredWindows;
  private final int _numValidMonitoredPartitions;
  private final Map<TopicPartition, List<SampleExtrapolation>> _sampleExtrapolations;
  private final int _totalNumPartitions;
  private final double _bootstrapProgress;
  private final double _loadingProgress;

  private LoadMonitorState(LoadMonitorTaskRunnerState state,
                           int numValidWindows,
                           SortedMap<Long, Float> monitoredWindows,
                           int numValidMonitoredPartitions,
                           int totalNumPartitions,
                           Map<TopicPartition, List<SampleExtrapolation>> sampleExtrapolations,
                           double bootstrapProgress,
                           double loadingProgress) {
    _loadMonitorTaskRunnerState = state;
    _numValidWindows = numValidWindows;
    _monitoredWindows = monitoredWindows;
    _numValidMonitoredPartitions = numValidMonitoredPartitions;
    _sampleExtrapolations = sampleExtrapolations;
    _totalNumPartitions = totalNumPartitions;
    _bootstrapProgress = bootstrapProgress;
    _loadingProgress = loadingProgress;
  }

  public static LoadMonitorState notStarted() {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.NOT_STARTED, 0, null, 0, -1, null, -1.0, -1.0);
  }

  public static LoadMonitorState running(int numValidSnapshotWindows,
                                         SortedMap<Long, Float> partitionCoverageByWindows,
                                         int numValidMonitoredTopics,
                                         int totalNumPartitions,
                                         Map<TopicPartition, List<SampleExtrapolation>> sampleExtrapolations) {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.RUNNING,
                                numValidSnapshotWindows,
                                partitionCoverageByWindows,
                                numValidMonitoredTopics,
                                totalNumPartitions,
                                sampleExtrapolations,
                                -1.0,
                                -1.0);
  }

  public static LoadMonitorState paused(int numValidSnapshotWindows,
                                        SortedMap<Long, Float> monitoredSnapshotWindows,
                                        int numValidMonitoredTopics,
                                        int totalNumPartitions,
                                        Map<TopicPartition, List<SampleExtrapolation>> sampleExtrapolations) {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.PAUSED,
                                numValidSnapshotWindows,
                                monitoredSnapshotWindows,
                                numValidMonitoredTopics,
                                totalNumPartitions,
                                sampleExtrapolations,
                                -1.0,
                                -1.0);
  }

  public static LoadMonitorState sampling(int numValidSnapshotWindows,
                                          SortedMap<Long, Float> monitoredSnapshotWindows,
                                          int numValidMonitoredTopics,
                                          int totalNumPartitions,
                                          Map<TopicPartition, List<SampleExtrapolation>> sampleExtrapolations) {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.SAMPLING,
                                numValidSnapshotWindows,
                                monitoredSnapshotWindows,
                                numValidMonitoredTopics,
                                totalNumPartitions,
                                sampleExtrapolations,
                                -1.0,
                                -1.0);
  }

  public static LoadMonitorState bootstrapping(int numValidSnapshotWindows,
                                               SortedMap<Long, Float> monitoredSnapshotWindows,
                                               int numValidMonitoredTopics,
                                               int totalNumPartitions,
                                               double bootstrapProgress,
                                               Map<TopicPartition, List<SampleExtrapolation>> sampleExtrapolations) {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.BOOTSTRAPPING,
                                numValidSnapshotWindows,
                                monitoredSnapshotWindows,
                                numValidMonitoredTopics,
                                totalNumPartitions,
                                sampleExtrapolations,
                                bootstrapProgress,
                                -1.0);
  }

  public static LoadMonitorState training(int numValidSnapshotWindows,
                                          SortedMap<Long, Float> monitoredSnapshotWindows,
                                          int numValidMonitoredTopics,
                                          int totalNumPartitions,
                                          Map<TopicPartition, List<SampleExtrapolation>> sampleExtrapolations) {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.TRAINING,
                                numValidSnapshotWindows,
                                monitoredSnapshotWindows,
                                numValidMonitoredTopics,
                                totalNumPartitions,
                                sampleExtrapolations,
                                -1.0,
                                -1.0);
  }

  public static LoadMonitorState loading(int numValidSnapshotWindows,
                                         SortedMap<Long, Float> monitoredSnapshotWindows,
                                         int numValidMonitoredTopics,
                                         int totalNumPartitions,
                                         double loadingProgress) {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.LOADING,
                                numValidSnapshotWindows,
                                monitoredSnapshotWindows,
                                numValidMonitoredTopics,
                                totalNumPartitions,
                                Collections.emptyMap(),
                                -1.0,
                                loadingProgress);
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure(boolean verbose) {
    Map<String, Object> loadMonitorState = new HashMap<>();
    double trainingPct = 0.0;
    if (ModelParameters.trainingCompleted()) {
      trainingPct = 100.0;
    } else {
      trainingPct = ModelParameters.modelCoefficientTrainingCompleteness() * 100;
    }
    // generic attribute collection
    switch (_loadMonitorTaskRunnerState) {
      case RUNNING:
      case SAMPLING:
      case PAUSED:
      case BOOTSTRAPPING:
      case TRAINING:
      case LOADING:
        loadMonitorState.put("state", _loadMonitorTaskRunnerState);
        loadMonitorState.put("trained", ModelParameters.trainingCompleted());
        loadMonitorState.put("trainingPct", trainingPct);
        loadMonitorState.put("numMonitoredWindows", _monitoredWindows.size());
        if (verbose) {
          loadMonitorState.put("monitoredWindows", _monitoredWindows);
        }
        loadMonitorState.put("numValidPartitions", _numValidMonitoredPartitions);
        loadMonitorState.put("numTotalPartitions", _totalNumPartitions);
        loadMonitorState.put("monitoringCoveragePct", nanToZero(validPartitionsRatio() * 100));
        loadMonitorState.put("numFlawedPartitions", _sampleExtrapolations.size());
        break;
      default:
        break;
    }
    // specific attribute collection
    switch (_loadMonitorTaskRunnerState) {
      case RUNNING:
      case SAMPLING:
      case PAUSED:
      case TRAINING:
        break;
      case BOOTSTRAPPING:
        loadMonitorState.put("bootstrapProgressPct", nanToZero(_bootstrapProgress * 100));
        break;
      case LOADING:
        loadMonitorState.put("loadingProgressPct", nanToZero(_loadingProgress * 100));
        break;
      default:
        loadMonitorState.put("state", _loadMonitorTaskRunnerState);
        loadMonitorState.put("error", "ILLEGAL_STATE_EXCEPTION");
        break;
    }
    return loadMonitorState;
  }

  /*
   * JSON does not support literal NaN value
   * round it to zero when Java Math sees a NaN
   */
  public static double nanToZero(double v) {
      if (Double.isNaN(v)) {
          return 0.0;
      } else {
          return v;
      }
  }


  @Override
  public String toString() {
    String trained = ModelParameters.trainingCompleted()
        ? "(TRAINED)" : String.format("(%.3f%% trained)",
                                      ModelParameters.modelCoefficientTrainingCompleteness() * 100);
    float validPartitionPercent = (float) _numValidMonitoredPartitions / _totalNumPartitions;
    float validWindowPercent = (float) _numValidWindows / _monitoredWindows.size();
    switch (_loadMonitorTaskRunnerState) {
      case NOT_STARTED:
        return String.format("{state: %s}", _loadMonitorTaskRunnerState);

      case RUNNING:
      case SAMPLING:
      case PAUSED:
        return String.format("{state: %s%s, NumValidWindows: (%d/%d) (%.3f%%), NumValidPartitions: %d/%d (%.3f%%), flawedPartitions: %d}",
                             _loadMonitorTaskRunnerState, trained, _numValidWindows,
                             _monitoredWindows.size(), validWindowPercent * 100,
                             _numValidMonitoredPartitions, _totalNumPartitions, validPartitionPercent * 100,
                             _sampleExtrapolations.size());
      case BOOTSTRAPPING:
        return String.format("{state: %s%s, BootstrapProgress: %.3f%%, NumValidWindows: (%d/%d) (%.3f%%), "
                                 + "NumValidPartitions: %d/%d (%.3f%%), flawedPartitions: %d}",
                             _loadMonitorTaskRunnerState, trained, _bootstrapProgress * 100, _numValidWindows, _monitoredWindows
                .size(), validWindowPercent * 100,
                             _numValidMonitoredPartitions, _totalNumPartitions, validPartitionPercent * 100, _sampleExtrapolations.size());

      case TRAINING:
        return String.format("{state: %s%s, NumValidWindows: (%d/%d) (%.3f%%) , NumValidPartitions: %d/%d (%.3f%%), FlawedPartitions: %d}",
                             _loadMonitorTaskRunnerState, trained, _monitoredWindows.size(), _numValidWindows,
                             validWindowPercent * 100, _numValidMonitoredPartitions, _totalNumPartitions, validPartitionPercent * 100,
                             _sampleExtrapolations.size());

      case LOADING:
        return String.format("{state: %s%s, LoadingProgress: %.3f%%, NumValidWindows: (%d/%d): (%.3f%%), "
                                 + "NumValidPartitions: %d/%d (%.3f%%), FlawedPartitions: %d}",
                             _loadMonitorTaskRunnerState, trained, _loadingProgress * 100, _numValidWindows,
                             _monitoredWindows.size(), validWindowPercent * 100,
                             _numValidMonitoredPartitions, _totalNumPartitions, validPartitionPercent * 100,
                             _sampleExtrapolations.size());
      default:
        throw new IllegalStateException("Should never be here");
    }
  }

  public LoadMonitorTaskRunnerState state() {
    return _loadMonitorTaskRunnerState;
  }

  public int numValidWindows() {
    return _numValidWindows;
  }

  public SortedMap<Long, Float> monitoredWindows() {
    return _monitoredWindows;
  }

  public int numValidPartitions() {
    return _numValidMonitoredPartitions;
  }

  public double validPartitionsRatio() {
    return (double) _numValidMonitoredPartitions / _totalNumPartitions;
  }

  public double bootstrapProgress() {
    return _bootstrapProgress;
  }

  public Map<TopicPartition, List<SampleExtrapolation>> sampleExtrapolations() {
    return _sampleExtrapolations;
  }

  public LinearRegressionModelParameters.LinearRegressionModelState detailTrainingProgress() {
    return ModelParameters.linearRegressionModelState();
  }
}
