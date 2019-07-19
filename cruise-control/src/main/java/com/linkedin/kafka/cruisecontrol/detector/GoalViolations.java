/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that holds all the goal violations.
 */
public class GoalViolations extends KafkaAnomaly {
  private static final String ID_PREFIX = AnomalyType.GOAL_VIOLATION.toString();
  private static final Logger LOG = LoggerFactory.getLogger(GoalViolations.class);
  private final KafkaCruiseControl _kafkaCruiseControl;
  // The priority order of goals is maintained here.
  private final Map<Boolean, List<String>> _violatedGoalsByFixability;
  private final boolean _allowCapacityEstimation;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final boolean _excludeRecentlyRemovedBrokers;
  private final String _anomalyId;
  private final List<String> _selfHealingGoals;

  public GoalViolations(KafkaCruiseControl kafkaCruiseControl,
                        boolean allowCapacityEstimation,
                        boolean excludeRecentlyDemotedBrokers,
                        boolean excludeRecentlyRemovedBrokers,
                        List<String> selfHealingGoals) {
    _kafkaCruiseControl = kafkaCruiseControl;
    _allowCapacityEstimation = allowCapacityEstimation;
    _violatedGoalsByFixability = new HashMap<>();
    _excludeRecentlyDemotedBrokers = excludeRecentlyDemotedBrokers;
    _excludeRecentlyRemovedBrokers = excludeRecentlyRemovedBrokers;
    _anomalyId = String.format("%s-%s", ID_PREFIX, UUID.randomUUID().toString().substring(ID_PREFIX.length() + 1));
    _optimizationResult = null;
    _selfHealingGoals = selfHealingGoals;
  }

  /**
   * Add detected goal violation.
   *
   * @param goalName The name of the goal.
   * @param fixable Whether the violated goal is fixable or not.
   */
  void addViolation(String goalName, boolean fixable) {
    _violatedGoalsByFixability.computeIfAbsent(fixable, k -> new ArrayList<>()).add(goalName);
  }

  /**
   * Get all the goal violations.
   */
  public Map<Boolean, List<String>> violatedGoalsByFixability() {
    return _violatedGoalsByFixability;
  }

  @Override
  public String anomalyId() {
    return _anomalyId;
  }

  @Override
  public boolean fix() throws KafkaCruiseControlException {
    if (_violatedGoalsByFixability.get(false) == null) {
      try {
        // Fix the fixable goal violations with rebalance operation.
        _optimizationResult = new OptimizationResult(_kafkaCruiseControl.rebalance(_selfHealingGoals,
                                                                                   false,
                                                                                   null,
                                                                                   new OperationProgress(),
                                                                                   _allowCapacityEstimation,
                                                                                   null,
                                                                                   null,
                                                                                   null,
                                                                                   false,
                                                                                   null,
                                                                                   null,
                                                                                   null,
                                                                                   _anomalyId,
                                                                                   _excludeRecentlyDemotedBrokers,
                                                                                   _excludeRecentlyRemovedBrokers,
                                                                                   false,
                                                                                   true,
                                                                                   Collections.emptySet(),
                                                                                   false),
                                                     null);
        // Ensure that only the relevant response is cached to avoid memory pressure.
        _optimizationResult.discardIrrelevantAndCacheJsonAndPlaintext();
        return true;
      } catch (IllegalStateException e) {
        LOG.warn("Got exception when trying to fix the cluster for violated goals {}: {}", _violatedGoalsByFixability.get(true), e.getMessage());
      }
    } else {
      LOG.info("Skip fixing goal violations due to unfixable goal violations {} detected.", _violatedGoalsByFixability.get(false));
    }
    return false;
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
