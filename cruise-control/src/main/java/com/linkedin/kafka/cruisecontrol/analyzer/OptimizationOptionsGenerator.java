/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.Set;
import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * The class used to generate {@link OptimizationOptions} for different purposes.
 */
@InterfaceStability.Evolving
public interface OptimizationOptionsGenerator extends CruiseControlConfigurable {

  /**
   * Generate optimization options used for detect goal violation.
   *
   * @param clusterModel The cluster model used to generate optimization options.
   * @param excludedTopics The topics to be specified to exclude in generated optimization options.
   * @param excludedBrokersForLeadership The brokers to be specified to not considered for leadership movement in
   *                                     generated optimization options.
   * @param excludedBrokersForReplicaMove The brokers to be specified to not considered for replica movement in
   *                                      generated optimization options.
   * @return An object of {@link OptimizationOptions}.
   */
  OptimizationOptions optimizationOptionsForGoalViolationDetection(ClusterModel clusterModel,
                                                                   Set<String> excludedTopics,
                                                                   Set<Integer> excludedBrokersForLeadership,
                                                                   Set<Integer> excludedBrokersForReplicaMove);

  /**
   * Generate optimization options used to calculate cached optimization proposal.
   *
   * @param clusterModel The cluster model used to generate optimization options.
   * @param excludedTopics The topics to be specified to exclude in generated optimization options.
   * @return An object of {@link OptimizationOptions}.
   */
  OptimizationOptions optimizationOptionsForCachedProposalCalculation(ClusterModel clusterModel,
                                                                      Set<String> excludedTopics);
}
