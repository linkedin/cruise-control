/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.GOAL_VIOLATION;


/**
 * A class that holds all the goal violations.
 */
public class GoalViolations extends AbstractGoalViolations {
  private static final Logger LOG = LoggerFactory.getLogger(GoalViolations.class);

  /**
   * An anomaly to indicate goal violation(s).
   */
  public GoalViolations() {
  }

  @Override
  public AnomalyType anomalyType() {
    return GOAL_VIOLATION;
  }

  @Override
  public Supplier<String> reasonSupplier() {
    return () -> String.format("Self healing for %s: %s", GOAL_VIOLATION, this);
  }

}
