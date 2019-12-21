/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RebalanceRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.extractKafkaCruiseControlObjectFromConfig;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.GOAL_VIOLATION;


/**
 * A class that holds all the goal violations.
 */
public class GoalViolations extends KafkaAnomaly {
  private static final Logger LOG = LoggerFactory.getLogger(GoalViolations.class);
  // The priority order of goals is maintained here.
  protected Map<Boolean, List<String>> _violatedGoalsByFixability;
  protected boolean _excludeRecentlyDemotedBrokers;
  protected boolean _excludeRecentlyRemovedBrokers;
  protected RebalanceRunnable _rebalanceRunnable;

  /**
   * An anomaly to indicate goal violation(s).
   */
  public GoalViolations() {
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
   * @return All the goal violations.
   */
  public Map<Boolean, List<String>> violatedGoalsByFixability() {
    return _violatedGoalsByFixability;
  }

  @Override
  public boolean fix() throws KafkaCruiseControlException {
    if (_violatedGoalsByFixability.get(false) == null) {
      try {
        // Fix the fixable goal violations with rebalance operation.
        _optimizationResult = new OptimizationResult(_rebalanceRunnable.rebalance(), null);
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
  public AnomalyType anomalyType() {
    return GOAL_VIOLATION;
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

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _violatedGoalsByFixability = new HashMap<>();
    _optimizationResult = null;
    KafkaCruiseControl kafkaCruiseControl = extractKafkaCruiseControlObjectFromConfig(configs, GOAL_VIOLATION);
    KafkaCruiseControlConfig config = kafkaCruiseControl.config();
    boolean allowCapacityEstimation = config.getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
    _excludeRecentlyDemotedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
    _excludeRecentlyRemovedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
    _rebalanceRunnable = new RebalanceRunnable(kafkaCruiseControl,
                                               getSelfHealingGoalNames(config),
                                               allowCapacityEstimation,
                                               _excludeRecentlyDemotedBrokers,
                                               _excludeRecentlyRemovedBrokers,
                                               _anomalyId.toString(),
                                                String.format("Self healing for %s: %s", GOAL_VIOLATION, this));
  }
}
