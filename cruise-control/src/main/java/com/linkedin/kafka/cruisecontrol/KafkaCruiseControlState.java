/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerState;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitorState;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.SampleExtrapolation;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.gson.Gson;
import java.util.StringJoiner;
import org.apache.kafka.common.TopicPartition;


public class KafkaCruiseControlState {
  private static final String PARTITION_MOVEMENTS = "partition movements";
  private static final String LEADERSHIP_MOVEMENTS = "leadership movements";
  private ExecutorState _executorState;
  private LoadMonitorState _monitorState;
  private AnalyzerState _analyzerState;

  public KafkaCruiseControlState(ExecutorState executionState,
                                 LoadMonitorState monitorState,
                                 AnalyzerState analyzerState) {
    _executorState = executionState;
    _monitorState = monitorState;
    _analyzerState = analyzerState;
  }

  public ExecutorState executorState() {
    return _executorState;
  }

  public LoadMonitorState monitorState() {
    return _monitorState;
  }

  public AnalyzerState analyzerState() {
    return _analyzerState;
  }

  /**
   * Return a valid JSON encoded string
   *
   * @param version JSON version
   */
  public String getJSONString(int version, boolean verbose) {
    Gson gson = new Gson();
    Map<String, Object> jsonStructure = getJsonStructure(verbose);
    jsonStructure.put("version", version);
    return gson.toJson(jsonStructure);
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure(boolean verbose) {
    Map<String, Object> cruiseControlState = new HashMap<>();
    if (_monitorState != null) {
      cruiseControlState.put("MonitorState", _monitorState.getJsonStructure(verbose));
    }
    if (_executorState != null) {
      cruiseControlState.put("ExecutorState", _executorState.getJsonStructure(verbose));
    }
    if (_analyzerState != null) {
      cruiseControlState.put("AnalyzerState", _analyzerState.getJsonStructure(verbose));
    }
    return cruiseControlState;
  }

  @Override
  public String toString() {
    return String.format("%s%s%s",
                         _monitorState != null ? String.format("MonitorState: %s%n", _monitorState) : "",
                         _executorState != null ? String.format("ExecutorState: %s%n", _executorState) : "",
                         _analyzerState != null ? String.format("AnalyzerState: %s%n", _analyzerState) : "");
  }

  private void writeVerboseMonitorState(OutputStream out) throws IOException {
    if (_monitorState != null) {
      out.write(String.format("%n%nMonitored Windows [Window End_Time=Data_Completeness]:%n").getBytes(StandardCharsets.UTF_8));

      StringJoiner joiner = new StringJoiner(", ", "{", "}");
      for (Map.Entry<Long, Float> entry : _monitorState.monitoredWindows().entrySet()) {
        joiner.add(String.format("%d=%.3f%%", entry.getKey(), entry.getValue() * 100));
      }
      out.write(joiner.toString().getBytes(StandardCharsets.UTF_8));
    }
  }

  private void writeVerboseAnalyzerState(OutputStream out) throws IOException {
    if (_analyzerState != null) {
      out.write(String.format("%n%nGoal Readiness:%n").getBytes(StandardCharsets.UTF_8));
      for (Map.Entry<Goal, Boolean> entry : _analyzerState.readyGoals().entrySet()) {
        Goal goal = entry.getKey();
        out.write(String.format("%50s, %s, %s%n", goal.getClass().getSimpleName(), goal.clusterModelCompletenessRequirements(),
                                entry.getValue() ? "Ready" : "NotReady").getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  private void writeVerboseExecutorState(OutputStream out) throws IOException {
    if (_executorState != null) {
      if (_executorState.state() == ExecutorState.State.REPLICA_MOVEMENT_TASK_IN_PROGRESS
          || _executorState.state() == ExecutorState.State.STOPPING_EXECUTION) {
        out.write(String.format("%n%nIn progress %s:%n", PARTITION_MOVEMENTS).getBytes(StandardCharsets.UTF_8));
        for (ExecutionTask task : _executorState.inProgressPartitionMovements()) {
          out.write(String.format("%s%n", task).getBytes(StandardCharsets.UTF_8));
        }
        out.write(String.format("%n%nAborting %s:%n", PARTITION_MOVEMENTS).getBytes(StandardCharsets.UTF_8));
        for (ExecutionTask task : _executorState.abortingPartitionMovements()) {
          out.write(String.format("%s%n", task).getBytes(StandardCharsets.UTF_8));
        }
        out.write(String.format("%n%nAborted %s:%n", PARTITION_MOVEMENTS).getBytes(StandardCharsets.UTF_8));
        for (ExecutionTask task : _executorState.abortedPartitionMovements()) {
          out.write(String.format("%s%n", task).getBytes(StandardCharsets.UTF_8));
        }
        out.write(String.format("%n%nDead %s:%n", PARTITION_MOVEMENTS).getBytes(StandardCharsets.UTF_8));
        for (ExecutionTask task : _executorState.deadPartitionMovements()) {
          out.write(String.format("%s%n", task).getBytes(StandardCharsets.UTF_8));
        }
        out.write(String.format("%n%n%s %s:%n", _executorState.state() == ExecutorState.State.STOPPING_EXECUTION
                                                ? "Cancelled" : "Pending", PARTITION_MOVEMENTS).getBytes(StandardCharsets.UTF_8));
        for (ExecutionTask task : _executorState.pendingPartitionMovements()) {
          out.write(String.format("%s%n", task).getBytes(StandardCharsets.UTF_8));
        }
      } else if (_executorState.state() == ExecutorState.State.LEADER_MOVEMENT_TASK_IN_PROGRESS) {
        out.write(String.format("%n%nPending %s:%n", LEADERSHIP_MOVEMENTS).getBytes(StandardCharsets.UTF_8));
        for (ExecutionTask task : _executorState.pendingLeadershipMovements()) {
          out.write(String.format("%s%n", task).getBytes(StandardCharsets.UTF_8));
        }
      }
    }
  }

  private void writeSuperVerbose(OutputStream out) throws IOException {
    if (_monitorState != null) {
      out.write(String.format("%n%nExtrapolated metric samples:%n").getBytes(StandardCharsets.UTF_8));
      Map<TopicPartition, List<SampleExtrapolation>> sampleFlaws = _monitorState.sampleExtrapolations();
      if (sampleFlaws != null && !sampleFlaws.isEmpty()) {
        for (Map.Entry<TopicPartition, List<SampleExtrapolation>> entry : sampleFlaws.entrySet()) {
          out.write(String.format("%n%s: %s", entry.getKey(), entry.getValue()).getBytes(StandardCharsets.UTF_8));
        }
      } else {
        out.write("None".getBytes(StandardCharsets.UTF_8));
      }
      if (_monitorState.detailTrainingProgress() != null) {
        out.write(
            String.format("%n%nLinear Regression Model State:%n%s", _monitorState.detailTrainingProgress()).getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  public void writeOutputStream(OutputStream out, boolean verbose, boolean superVerbose) throws IOException {
    out.write(toString().getBytes(StandardCharsets.UTF_8));

    if (verbose || superVerbose) {
      writeVerboseMonitorState(out);
      writeVerboseAnalyzerState(out);
      writeVerboseExecutorState(out);

      if (superVerbose) {
        writeSuperVerbose(out);
      }
    }
  }

  public enum SubState {
    ANALYZER, MONITOR, EXECUTOR
  }
}
