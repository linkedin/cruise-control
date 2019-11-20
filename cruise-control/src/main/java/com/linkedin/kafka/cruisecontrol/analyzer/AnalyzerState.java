/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;


/**
 * The state for the analyzer.
 */
public class AnalyzerState {
  private static final String IS_PROPOSAL_READY = "isProposalReady";
  private static final String READY_GOALS = "readyGoals";
  private static final String STATUS = "status";
  private static final String READY = "ready";
  private static final String NOT_READY = "notReady";
  private static final String NAME = "name";
  private static final String MODEL_COMPLETE_REQUIREMENT = "modelCompleteRequirement";
  private static final String GOAL_READINESS = "goalReadiness";
  private final boolean _isProposalReady;
  private final Map<Goal, Boolean> _readyGoals;

  /**
   * @param isProposalReady True if the goal optimizer has valid cached proposals for optimization with the default goals.
   * @param readyGoals Goals that are ready for self-healing.
   */
  public AnalyzerState(boolean isProposalReady, Map<Goal, Boolean> readyGoals) {
    _isProposalReady = isProposalReady;
    _readyGoals = readyGoals;
  }

  /**
   * @return True if the proposal is ready, false otherwise.
   */
  public boolean proposalReady() {
    return _isProposalReady;
  }

  /**
   * @return A map of ready goals.
   */
  public Map<Goal, Boolean> readyGoals() {
    return _readyGoals;
  }

  /**
   * @param verbose True if verbose, false otherwise.
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure(boolean verbose) {
    Map<String, Object> analyzerState = new HashMap<>(verbose ? 3 : 2);
    Set<String> readyGoalNames = new HashSet<>();
    for (Map.Entry<Goal, Boolean> entry : _readyGoals.entrySet()) {
      if (entry.getValue()) {
        readyGoalNames.add(entry.getKey().name());
      }
    }
    analyzerState.put(IS_PROPOSAL_READY, _isProposalReady);
    analyzerState.put(READY_GOALS, readyGoalNames);
    if (verbose) {
      List<Object> goalReadinessList = new ArrayList<>(_readyGoals.size());
      for (Map.Entry<Goal, Boolean> entry : _readyGoals.entrySet()) {
        Goal goal = entry.getKey();
        Map<String, Object> goalReadinessRecord = new HashMap<>(3);
        goalReadinessRecord.put(NAME, goal.getClass().getSimpleName());
        goalReadinessRecord.put(MODEL_COMPLETE_REQUIREMENT, goal.clusterModelCompletenessRequirements().getJsonStructure());
        goalReadinessRecord.put(STATUS, entry.getValue() ? READY : NOT_READY);
        goalReadinessList.add(goalReadinessRecord);
      }
      analyzerState.put(GOAL_READINESS, goalReadinessList);
    }
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
    return String.format("{%s: %s, %s: %s}", IS_PROPOSAL_READY, _isProposalReady, READY_GOALS, readyGoalNames);
  }
}
