/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


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

  @Override
  public String toString() {
    Set<String> readyGoalNames = new HashSet<>();
    for (Map.Entry<Goal, Boolean> entry : _readyGoals.entrySet()) {
      if (entry.getValue()) {
        readyGoalNames.add(entry.getKey().getClass().getSimpleName());
      }
    }
    return String.format("{isProposalReady: %s, ReadyGaols: %s}", _isProposalReady, readyGoalNames);
  }
}
