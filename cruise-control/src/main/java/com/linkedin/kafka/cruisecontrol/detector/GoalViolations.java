/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that holds all the goal violations.
 */
public class GoalViolations extends KafkaAnomaly {
  private static final Logger LOG = LoggerFactory.getLogger(GoalViolations.class);
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final List<Violation> _goalViolations = new ArrayList<>();
  private final boolean _allowCapacityEstimation;

  public GoalViolations(KafkaCruiseControl kafkaCruiseControl, boolean allowCapacityEstimation) {
    _kafkaCruiseControl = kafkaCruiseControl;
    _allowCapacityEstimation = allowCapacityEstimation;
  }

  public void addViolation(int priority, String goalName, Set<ExecutionProposal> executionProposals) {
    _goalViolations.add(new Violation(priority, goalName, executionProposals));
  }

  /**
   * Get all the goal violations.
   */
  public List<Violation> violations() {
    return _goalViolations;
  }

  @Override
  public void fix() throws KafkaCruiseControlException {
    // Fix the violations using a rebalance.
    try {
      _kafkaCruiseControl.rebalance(
          Collections.emptyList(), false, null, new OperationProgress(), _allowCapacityEstimation,
          null, null, false, null);
    } catch (IllegalStateException e) {
      LOG.warn("Got exception when trying to fix the cluster. " + e.getMessage());
    }
  }

  public static class Violation {
    private final int _priority;
    private final String _goalName;
    private final Set<ExecutionProposal> _executionProposals;

    public Violation(int priority, String goalName, Set<ExecutionProposal> executionProposals) {
      _priority = priority;
      _goalName = goalName;
      _executionProposals = executionProposals;
    }

    public int priority() {
      return _priority;
    }

    public String goalName() {
      return _goalName;
    }

    public Set<ExecutionProposal> executionProposals() {
      return _executionProposals;
    }
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(",");
    _goalViolations.forEach(v -> joiner.add(v.goalName()));
    return "{" + joiner.toString() + "}";
  }
}
