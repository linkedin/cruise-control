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
  private static final String STATE = "state";
  private static final String REASON_OF_LATEST_PAUSE_OR_RESUME = "reasonOfLatestPauseOrResume";
  private static final String TRAINED = "trained";
  private static final String TRAINING_PCT = "trainingPct";
  private static final String NUM_MONITORED_WINDOWS = "numMonitoredWindows";
  private static final String MONITORED_WINDOWS = "monitoredWindows";
  private static final String NUM_VALID_PARTITIONS = "numValidPartitions";
  private static final String NUM_TOTAL_PARTITIONS = "numTotalPartitions";
  private static final String MONITORING_COVERAGE_PCT = "monitoringCoveragePct";
  private static final String NUM_FLAWED_PARTITIONS = "numFlawedPartitions";
  private static final String BOOTSTRAP_PROGRESS_PCT = "bootstrapProgressPct";
  private static final String LOADING_PROGRESS_PCT = "loadingProgressPct";
  private static final String ERROR = "error";
  private final LoadMonitorTaskRunnerState _loadMonitorTaskRunnerState;
  private final int _numValidWindows;
  private final SortedMap<Long, Float> _monitoredWindows;
  private final int _numValidMonitoredPartitions;
  private final Map<TopicPartition, List<SampleExtrapolation>> _sampleExtrapolations;
  private final int _totalNumPartitions;
  private final double _bootstrapProgress;
  private final double _loadingProgress;
  private final String _reasonOfLatestPauseOrResume;

  private LoadMonitorState(LoadMonitorTaskRunnerState state,
                           int numValidWindows,
                           SortedMap<Long, Float> monitoredWindows,
                           int numValidMonitoredPartitions,
                           int totalNumPartitions,
                           Map<TopicPartition, List<SampleExtrapolation>> sampleExtrapolations,
                           double bootstrapProgress,
                           double loadingProgress,
                           String reasonOfLatestPauseOrResume) {
    _loadMonitorTaskRunnerState = state;
    _numValidWindows = numValidWindows;
    _monitoredWindows = monitoredWindows;
    _numValidMonitoredPartitions = numValidMonitoredPartitions;
    _sampleExtrapolations = sampleExtrapolations;
    _totalNumPartitions = totalNumPartitions;
    _bootstrapProgress = bootstrapProgress;
    _loadingProgress = loadingProgress;
    _reasonOfLatestPauseOrResume = reasonOfLatestPauseOrResume;
  }

  private LoadMonitorState(LoadMonitorTaskRunnerState state,
                           int numValidWindows,
                           SortedMap<Long, Float> monitoredWindows,
                           int numValidMonitoredPartitions,
                           int totalNumPartitions,
                           Map<TopicPartition, List<SampleExtrapolation>> sampleExtrapolations,
                           double bootstrapProgress,
                           double loadingProgress) {
    this(state, numValidWindows, monitoredWindows, numValidMonitoredPartitions, totalNumPartitions, sampleExtrapolations,
         bootstrapProgress, loadingProgress, null);
  }

  public static LoadMonitorState notStarted() {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.NOT_STARTED, 0, null, 0, -1, null, -1.0, -1.0);
  }

  public static LoadMonitorState running(int numValidSnapshotWindows,
                                         SortedMap<Long, Float> partitionCoverageByWindows,
                                         int numValidMonitoredTopics,
                                         int totalNumPartitions,
                                         Map<TopicPartition, List<SampleExtrapolation>> sampleExtrapolations,
                                         String reasonOfLatestPauseOrResume) {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.RUNNING,
                                numValidSnapshotWindows,
                                partitionCoverageByWindows,
                                numValidMonitoredTopics,
                                totalNumPartitions,
                                sampleExtrapolations,
                                -1.0,
                                -1.0,
                                reasonOfLatestPauseOrResume);
  }

  public static LoadMonitorState paused(int numValidSnapshotWindows,
                                        SortedMap<Long, Float> monitoredSnapshotWindows,
                                        int numValidMonitoredTopics,
                                        int totalNumPartitions,
                                        Map<TopicPartition, List<SampleExtrapolation>> sampleExtrapolations,
                                        String reasonOfLatestPauseOrResume) {
    return new LoadMonitorState(LoadMonitorTaskRunnerState.PAUSED,
                                numValidSnapshotWindows,
                                monitoredSnapshotWindows,
                                numValidMonitoredTopics,
                                totalNumPartitions,
                                sampleExtrapolations,
                                -1.0,
                                -1.0,
                                reasonOfLatestPauseOrResume);
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

  private void setCommonJsonGenericAttributeCollection(boolean verbose, Map<String, Object> loadMonitorState) {
    loadMonitorState.put(STATE, _loadMonitorTaskRunnerState);
    loadMonitorState.put(TRAINED, ModelParameters.trainingCompleted());
    loadMonitorState.put(TRAINING_PCT, ModelParameters.trainingCompleted()
                                       ? 100.0 : (ModelParameters.modelCoefficientTrainingCompleteness() * 100));
    loadMonitorState.put(NUM_MONITORED_WINDOWS, _monitoredWindows.size());
    if (verbose) {
      loadMonitorState.put(MONITORED_WINDOWS, _monitoredWindows);
    }
    loadMonitorState.put(NUM_VALID_PARTITIONS, _numValidMonitoredPartitions);
    loadMonitorState.put(NUM_TOTAL_PARTITIONS, _totalNumPartitions);
    loadMonitorState.put(MONITORING_COVERAGE_PCT, nanToZero(validPartitionsRatio() * 100));
    loadMonitorState.put(NUM_FLAWED_PARTITIONS, _sampleExtrapolations.size());
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure(boolean verbose) {
    Map<String, Object> loadMonitorState = new HashMap<>();
    // generic attribute collection
    switch (_loadMonitorTaskRunnerState) {
      case RUNNING:
      case SAMPLING:
      case PAUSED:
      case BOOTSTRAPPING:
      case TRAINING:
      case LOADING:
        setCommonJsonGenericAttributeCollection(verbose, loadMonitorState);
        break;
      default:
        break;
    }
    // specific attribute collection
    switch (_loadMonitorTaskRunnerState) {
      case RUNNING:
      case PAUSED:
        loadMonitorState.put(REASON_OF_LATEST_PAUSE_OR_RESUME, _reasonOfLatestPauseOrResume == null ? "N/A" : _reasonOfLatestPauseOrResume);
        break;
      case SAMPLING:
      case TRAINING:
        break;
      case BOOTSTRAPPING:
        loadMonitorState.put(BOOTSTRAP_PROGRESS_PCT, nanToZero(_bootstrapProgress * 100));
        break;
      case LOADING:
        loadMonitorState.put(LOADING_PROGRESS_PCT, nanToZero(_loadingProgress * 100));
        break;
      default:
        loadMonitorState.put(STATE, _loadMonitorTaskRunnerState);
        loadMonitorState.put(ERROR, "ILLEGAL_STATE_EXCEPTION");
        break;
    }
    return loadMonitorState;
  }

  /*
   * JSON does not support literal NaN value
   * round it to zero when Java Math sees a NaN
   */
  public static double nanToZero(double v) {
      return Double.isNaN(v) ? 0.0 : v;
  }


  @Override
  public String toString() {
    String trained = ModelParameters.trainingCompleted()
                     ? "(TRAINED)"
                     : String.format("(%.3f%% trained)", ModelParameters.modelCoefficientTrainingCompleteness() * 100);
    float validPartitionPercent = (float) _numValidMonitoredPartitions / _totalNumPartitions;
    float validWindowPercent = (float) _numValidWindows / _monitoredWindows.size();
    switch (_loadMonitorTaskRunnerState) {
      case NOT_STARTED:
        return String.format("{state: %s}", _loadMonitorTaskRunnerState);

      case RUNNING:
      case PAUSED:
        return String.format("{state: %s%s, NumValidWindows: (%d/%d) (%.3f%%), NumValidPartitions: %d/%d (%.3f%%), flawedPartitions: %d%s}",
                             _loadMonitorTaskRunnerState, trained, _numValidWindows,
                             _monitoredWindows.size(), validWindowPercent * 100,
                             _numValidMonitoredPartitions, _totalNumPartitions, validPartitionPercent * 100,
                             _sampleExtrapolations.size(), _reasonOfLatestPauseOrResume == null
                                                           ? "" : String.format(", reasonOfPauseOrResume: %s", _reasonOfLatestPauseOrResume));
      case SAMPLING:
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
