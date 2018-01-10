/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;


/**
 * A class that holds all the goal violations.
 */
public class GoalViolations extends Anomaly {
  private final List<Violation> _goalViolations = new ArrayList<>();

  public void addViolation(int priority, String goalName, Set<BalancingProposal> balancingProposals) {
    _goalViolations.add(new Violation(priority, goalName, balancingProposals));
  }

  /**
   * Get all the goal violations.
   */
  public List<Violation> violations() {
    return _goalViolations;
  }

  @Override
  void fix(KafkaCruiseControl kafkaCruiseControl) throws KafkaCruiseControlException {
    // Fix the violations using a rebalance.
    kafkaCruiseControl.rebalance(Collections.emptyList(), false, null, new OperationProgress());
  }

  public static class Violation {
    private final int _priority;
    private final String _goalName;
    private final Set<BalancingProposal> _balancingProposals;

    public Violation(int priority, String goalName, Set<BalancingProposal> balancingProposals) {
      _priority = priority;
      _goalName = goalName;
      _balancingProposals = balancingProposals;
    }

    public int priority() {
      return _priority;
    }

    public String goalName() {
      return _goalName;
    }

    public Set<BalancingProposal> balancingProposals() {
      return _balancingProposals;
    }
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(",");
    _goalViolations.forEach(v -> joiner.add(v.goalName()));
    return "{" + joiner.toString() + "}";
  }
}
