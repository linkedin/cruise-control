/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private final Map<Boolean, List<String>> _violatedGoalsByFixability;
  private final boolean _allowCapacityEstimation;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final boolean _excludeRecentlyRemovedBrokers;

  public GoalViolations(KafkaCruiseControl kafkaCruiseControl,
                        boolean allowCapacityEstimation,
                        boolean excludeRecentlyDemotedBrokers,
                        boolean excludeRecentlyRemovedBrokers) {
    _kafkaCruiseControl = kafkaCruiseControl;
    _allowCapacityEstimation = allowCapacityEstimation;
    _violatedGoalsByFixability = new HashMap<>();
    _excludeRecentlyDemotedBrokers = excludeRecentlyDemotedBrokers;
    _excludeRecentlyRemovedBrokers = excludeRecentlyRemovedBrokers;
  }

  /**
   * Add detected goal violation.
   *
   * @param goalName The name of the goal.
   * @param fixable Whether the violated goal is fixable or not.
   */
  public void addViolation(String goalName, boolean fixable) {
    _violatedGoalsByFixability.computeIfAbsent(fixable, k -> new ArrayList<>()).add(goalName);
  }

  /**
   * Get all the goal violations.
   */
  public Map<Boolean, List<String>> violatedGoalsByFixability() {
    return _violatedGoalsByFixability;
  }

  @Override
  public void fix() throws KafkaCruiseControlException {
    if (_violatedGoalsByFixability.get(false) == null) {
      try {
        // Fix the fixable goal violations with rebalance operation.
        _kafkaCruiseControl.rebalance(Collections.emptyList(), false, null, new OperationProgress(), _allowCapacityEstimation,
                                      null, null, false, null,
                                      null, _excludeRecentlyDemotedBrokers, _excludeRecentlyRemovedBrokers);
      } catch (IllegalStateException e) {
        LOG.warn("Got exception when trying to fix the cluster for violated goals {}: {}", _violatedGoalsByFixability.get(true), e.getMessage());
      }
    } else {
      LOG.info("Skip fixing goal violations due to unfixable goal violations {} detected.", _violatedGoalsByFixability.get(false));
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{Unfixable goal violations: {");
    StringJoiner joiner = new StringJoiner(",");
    _violatedGoalsByFixability.getOrDefault(false, Collections.emptyList()).forEach(joiner::add);
    sb.append(joiner.toString());
    sb.append("}, Fixable goal violations: {");
    joiner = new StringJoiner(",");
    _violatedGoalsByFixability.getOrDefault(true, Collections.emptyList()).forEach(joiner::add);
    sb.append(joiner.toString());
    sb.append(String.format("}, Exclude brokers recently (removed: %s demoted: %s)}",
                            _excludeRecentlyRemovedBrokers, _excludeRecentlyDemotedBrokers));
    return sb.toString();
  }
}
