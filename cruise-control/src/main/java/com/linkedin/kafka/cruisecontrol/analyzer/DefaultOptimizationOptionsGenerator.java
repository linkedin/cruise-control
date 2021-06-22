/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.Collections;
import java.util.Map;
import java.util.Set;


public class DefaultOptimizationOptionsGenerator implements OptimizationOptionsGenerator, CruiseControlConfigurable {

  @Override
  public OptimizationOptions optimizationOptionsForGoalViolationDetection(ClusterModel clusterModel,
                                                                          Set<String> excludedTopics,
                                                                          Set<Integer> excludedBrokersForLeadership,
                                                                          Set<Integer> excludedBrokersForReplicaMove) {
    return new OptimizationOptions(excludedTopics,
                                   excludedBrokersForLeadership,
                                   excludedBrokersForReplicaMove,
                                   true);
  }

  @Override
  public OptimizationOptions optimizationOptionsForCachedProposalCalculation(ClusterModel clusterModel,
                                                                             Set<String> excludedTopics) {
    return new OptimizationOptions(excludedTopics,
                                   Collections.emptySet(),
                                   Collections.emptySet(),
                                   false,
                                   Collections.emptySet(),
                                   false,
                                   true);
  }

  @Override
  public void configure(Map<String, ?> configs) {
  }
}
