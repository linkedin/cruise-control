/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;


/**
 * The state for the analyzer.
 */
public class AnalyzerState {
  private final boolean _isProposalReady;
  private final Map<Goal, Boolean> _readyGoals;

  public AnalyzerState(boolean isProposalReady,
                       Map<Goal, Boolean> readyGoals) {
    _isProposalReady = isProposalReady;
    _readyGoals = readyGoals;
  }

  public boolean proposalReady() {
    return _isProposalReady;
  }

  public Map<Goal, Boolean> readyGoals() {
    return _readyGoals;
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> analyzerState = new HashMap<>();
    Set<Map<String, String>> readyGoalNames = new HashSet<>();
    for (Map.Entry<Goal, Boolean> entry : _readyGoals.entrySet()) {
      if (entry.getValue()) {
        Map<String, String> goalMap = new HashMap<>();
        goalMap.put("goal", entry.getKey().name());
        readyGoalNames.add(goalMap);
      }
    }
    analyzerState.put("isProposalReady", _isProposalReady);
    analyzerState.put("readyGoals", readyGoalNames);
    return analyzerState;
  }

  @Override
  public String toString() {
    Set<String> readyGoalNames = new HashSet<>();
    for (Map.Entry<Goal, Boolean> entry : _readyGoals.entrySet()) {
      if (entry.getValue()) {
        readyGoalNames.add(entry.getKey().getClass().getSimpleName());
      }
    }
    return String.format("{isProposalReady: %s, ReadyGoals: %s}", _isProposalReady, readyGoalNames);
  }
}
