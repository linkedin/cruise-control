/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerState;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitorState;
import java.util.HashMap;
import java.util.Map;
import com.google.gson.Gson;

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

  /**
   * Return a valid JSON encoded string
   *
   * @param version JSON version
   */
  public String getJSONString(int version) {
    Gson gson = new Gson();
    Map<String, Object> jsonStructure = getJsonStructure();
    jsonStructure.put("version", version);
    return gson.toJson(jsonStructure);
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> cruiseControlState = new HashMap<>();
    cruiseControlState.put("MonitorState", _monitorState.getJsonStructure());
    cruiseControlState.put("ExecutorState", _executorState.getJsonStructure());
    cruiseControlState.put("AnalyzerState", _analyzerState.getJsonStructure());
    return cruiseControlState;
  }

  @Override
  public String toString() {
    return String.format("{MonitorState: %s, ExecutorState: %s, AnalyzerState: %s}",
                         _monitorState, _executorState, _analyzerState);
  }
}
