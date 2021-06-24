/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;


/**
 * A util class for Kafka Assigner Goals.
 */
public final class KafkaAssignerUtils {

  private KafkaAssignerUtils() {

  }

  static void sanityCheckOptimizationOptions(OptimizationOptions optimizationOptions) {
    if (optimizationOptions.isTriggeredByGoalViolation()) {
      throw new IllegalArgumentException("Kafka Assigner goals do not support usage by goal violation detector.");
    } else if (optimizationOptions.onlyMoveImmigrantReplicas()) {
      throw new IllegalArgumentException("Kafka Assigner goals do not support usage of modifying topic replication factor.");
    }
  }
}
