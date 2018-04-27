/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Set;
import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * This is the interface of the optimization goals used by Cruise Control. Users can implement this goal and
 * add the implementation class name to Cruise Control goals configuration so that Cruise Control will take the goal
 * when generating the optimization proposals.
 * <p>
 *   See the <a href="https://github.com/linkedin/cruise-control/wiki/Configurations">Cruise Control configurations wiki</a>
 *   for more details.
 * </p>
 * <p>
 *   We have provided a helper {@link AbstractGoal} class with some defined optimization steps to make the implementation
 *   of the goals simpler.
 * </p>
 */
@InterfaceStability.Evolving
public interface Goal extends CruiseControlConfigurable {
  /**
   * Optimize the given cluster model as needed for this goal.
   * <p>
   *   The method will be given a cluster model. The goal can try to optimize the cluster model by performing some
   *   admin operations (e.g. move replicas or leadership of partitions).
   * </p>
   * <p>
   *   During the optimization, the implementation should make sure that all the previously optimized goals
   *   are still satisfied after this method completes its execution. The implementation can use
   *   {@link #actionAcceptance(BalancingAction, ClusterModel)} to check whether an admin operation
   *   is allowed by a previously optimized goal.
   * </p>
   * <p>
   *   The implementation of a soft goal should return a boolean indicating whether the goal has been met
   *   after the optimization or not.
   * </p>
   * <p>
   *   The implementation of a hard goal should throw an {@link OptimizationFailureException} when the goal
   *   cannot be met. This will then fail the entire optimization attempt.
   * </p>
   * @param clusterModel   The cluster model reflecting the current state of the cluster. It is a result of the
   *                       optimization of the previously optimized goals.
   * @param optimizedGoals Goals that have already been optimized. These goals cannot be violated.
   * @param excludedTopics The topics that should be excluded from the optimization action.
   * @return true if the goal is met after the optimization, false otherwise. Note that for hard goals,
   * the implementation should just throw exceptions if the goal is not met.
   * @throws KafkaCruiseControlException
   */
  boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, Set<String> excludedTopics)
      throws KafkaCruiseControlException;

  /**
   * @deprecated
   * Please use {@link #actionAcceptance(BalancingAction, ClusterModel)} instead.
   *
   * Check whether given action is acceptable by this goal in the given state of the cluster. An action is
   * acceptable by a goal if it satisfies requirements of the goal.
   * It is assumed that the given action does not involve replicas regarding excluded topics.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel State of the cluster before application of the action.
   * @return True if action is acceptable by this goal, false otherwise.
   */
  @Deprecated
  boolean isActionAcceptable(BalancingAction action, ClusterModel clusterModel);

  /**
   * Check whether the given action is acceptable by this goal in the given state of the cluster. An action is
   * (1) accepted by a goal if it satisfies requirements of the goal, or (2) rejected by a goal if it violates its
   * requirements. The return value indicates whether the action is accepted or why it is rejected.
   * It is assumed that the given action does not involve replicas regarding excluded topics.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel State of the cluster before application of the action.
   * @return the action acceptance indicating whether an action is accepted, or why it is rejected.
   */
  ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel);

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
   * The returned value must not be null.
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
