/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Set;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * This is the API available to the GoalOptimizer to generate balancing proposals.
 */
@InterfaceStability.Evolving
public interface Goal extends Configurable {

  /**
   * Optimize the given cluster in accordance with the requirements of this goal. Guarantee that optimized goals will
   * still satisfy their requirements after optimize function completes its execution.
   * <p>
   *   The implementation of a soft goal should return a boolean indicating whether the goal has been met
   *   after the optimization or not.
   * </p>
   * <p>
   *   The implementation of a hard goal should just throw an {@link OptimizationFailureException} when the goal
   *   cannot be met.
   * </p>
   * @param optimizedGoals Goals that have already been optimized.
   * @param clusterModel   The state of the cluster before optimizing it.
   * @param excludedTopics The topics that should be excluded from the optimization proposal.
   * @return true if the goal is successfully met, false otherwise. Note that for hard goals, the implementation
   * should just throw exceptions if the goal is not met.
   * @throws KafkaCruiseControlException
   */
  boolean optimize(Set<Goal> optimizedGoals, ClusterModel clusterModel, Set<String> excludedTopics)
      throws KafkaCruiseControlException;

  /**
   * Check whether given proposal is acceptable by this goal in the given state of the cluster. A proposal is
   * acceptable by a goal if it satisfies requirements of the goal.
   *
   * @param proposal     Proposal to be checked for acceptance.
   * @param clusterModel State of the cluster before application of the proposal.
   * @return True if proposal is acceptable by this goal, false otherwise.
   */
  boolean isProposalAcceptable(BalancingProposal proposal, ClusterModel clusterModel);

  /**
   * Get an instance of {@link ClusterModelStatsComparator} for this goal.
   *
   * The {@link ClusterModelStatsComparator#compare(ClusterModelStats, ClusterModelStats)} method should give a
   * preference between two {@link ClusterModelStats}.
   *
   * Cruise Control will not reuse the returned instance.
   *
   * The returned value must not be null.
   */
  ClusterModelStatsComparator clusterModelStatsComparator();

  /**
   * Specify the load requirements for this goal. For different goals the load requirement maybe different.
   * For example, rack-awareness do not need load information at all, while resource distribution largely relies
   * on the monitored load completeness.
   *
   * Note that the load requirements are only used for the auto-operations. Users can pick a cluster model and run
   * against an arbitrary goal list. When users do that, the load requirements specified in each goal is ignored.
   *
   * The returned evalue must not be null.
   *
   * @return the load requirement for this goal.
   */
  ModelCompletenessRequirements clusterModelCompletenessRequirements();

  /**
   * Get the name of this goal. Name of a goal provides an identification for the goal in human readable format.
   */
  String name();


  /**
   * A comparator that compares two cluster model stats.
   */
  interface ClusterModelStatsComparator extends Comparator<ClusterModelStats>, Serializable {

    /**
     * Compare two cluster model stats and determine which stats is preferred.
     *
     * @param stats1 the first stats
     * @param stats2 the second stats
     * @return Positive value if stats1 is preferred, 0 if the two stats are equally preferred, negative value if stats2
     * is preferred.
     */
    @Override
    int compare(ClusterModelStats stats1, ClusterModelStats stats2);

    /**
     * This is a method to get the reason for the last comparison. The implementation should at least provide a
     * reason when the last comparison returns negative value.
     * @return A string that explains the result of last comparison.
     */
    String explainLastComparison();
  }
}
