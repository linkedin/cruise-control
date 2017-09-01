/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerState;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitorState;


public class KafkaCruiseControlState {
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

  @Override
  public String toString() {
    return String.format("{MonitorState: %s, ExecutorState: %s, AnalyzerState: %s}",
                         _monitorState, _executorState, _analyzerState);
  }
}
