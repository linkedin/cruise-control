/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
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
  // The priority order of goals is maintained here.
  private final List<String> _fixableViolatedGoals;
  private final List<String> _unfixableViolatedGoals;
  private final boolean _allowCapacityEstimation;

  public GoalViolations(KafkaCruiseControl kafkaCruiseControl, boolean allowCapacityEstimation) {
    _kafkaCruiseControl = kafkaCruiseControl;
    _allowCapacityEstimation = allowCapacityEstimation;
    _fixableViolatedGoals = new ArrayList<>();
    _unfixableViolatedGoals = new ArrayList<>();
  }

  /**
   * Add detected goal violation.
   *
   * @param goalName The name of the goal.
   * @param fixable Whether the violated goal is fixable or not.
   */
  public void addViolation(String goalName, boolean fixable) {
    if (fixable) {
      _fixableViolatedGoals.add(goalName);
    } else {
      _unfixableViolatedGoals.add(goalName);
    }
  }

  /**
   * Get all the goal violations.
   */
  public Set<String> violations() {
    Set<String> allViolatedGoals = new HashSet<>(_unfixableViolatedGoals);
    allViolatedGoals.addAll(_fixableViolatedGoals);
    return allViolatedGoals;
  }

  @Override
  public void fix() throws KafkaCruiseControlException {
    if (_unfixableViolatedGoals.isEmpty()) {
      try {
        // Fix the fixable goal violations with rebalance operation.
        _kafkaCruiseControl.rebalance(Collections.emptyList(), false, null, new OperationProgress(), _allowCapacityEstimation,
                                      null, null, false, null, null);
      } catch (IllegalStateException e) {
        LOG.warn("Got exception when trying to fix the cluster for violated goals {} " + e.getMessage(), _fixableViolatedGoals);
      }
    } else {
      LOG.info("Skip fixing goal violations due to unfixable goal violations {} detected.", _unfixableViolatedGoals);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{unfixable goal violations: {");
    StringJoiner joiner = new StringJoiner(",");
    _unfixableViolatedGoals.forEach(joiner::add);
    sb.append(joiner.toString());
    sb.append("}, fixable goal violations: {");
    joiner = new StringJoiner(",");
    _fixableViolatedGoals.forEach(joiner::add);
    sb.append("}}");
    return sb.toString();
  }
}
