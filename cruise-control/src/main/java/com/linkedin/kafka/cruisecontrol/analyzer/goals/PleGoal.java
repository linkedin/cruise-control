/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class for achieving the following hard goal:
 * HARD GOAL: Generate leadership movement proposals to provide PLE similar to leader balance module in kafka-assigner.
 *
 * This goal assigns leadership for each partition by first choosing the first replica that has 0 leaders so far,
 * then by choosing the replica with the lowest leader ratio.
 *
 * @see <a href="https://github.com/linkedin/kafka-tools/blob/master/kafka/tools/assigner/actions/balancemodules/leader.py">
 *   https://github.com/linkedin/kafka-tools/blob/master/kafka/tools/assigner/actions/balancemodules/leader.py</a>
 *   for the original algorithm.
 */
public class PleGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(PleGoal.class);
  private double _maximumLeaderRatioPerBroker;

  /**
   * Constructor for Ple Goal.
   */
  public PleGoal() {
    _maximumLeaderRatioPerBroker = Double.MAX_VALUE;
  }

  /**
   * Package private for unit test.
   */
  PleGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
    _maximumLeaderRatioPerBroker = Double.MAX_VALUE;
  }

  /**
   * Check whether the given proposal is acceptable by this goal.
   *
   * A proposal is acceptable under the following cases:
   * (1) if the source broker has only a single leader, then the leader or leadership cannot be removed from it.
   * (2) if the leader ratios at the source or destination broker goes over the maximum leader ratio of the cluster.
   *
   * @param proposal     Proposal to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return True if proposal is acceptable by this goal, false otherwise.
   */
  @Override
  public boolean isProposalAcceptable(BalancingProposal proposal, ClusterModel clusterModel) {
    int numLocalLeaders = clusterModel.broker(proposal.sourceBrokerId()).leaderReplicas().size();
    boolean isLeader = clusterModel.broker(proposal.sourceBrokerId()).replica(proposal.topicPartition()).isLeader();
    if (numLocalLeaders == 1 && isLeader) {
      return false;
    }

    Broker destinationBroker = clusterModel.broker(proposal.destinationBrokerId());
    int numDestinationLeaders = destinationBroker.leaderReplicas().size();
    int numDestinationReplicas = destinationBroker.replicas().size();

    if (proposal.balancingAction().equals(BalancingAction.LEADERSHIP_MOVEMENT)) {
      double proposedLeaderRatioInDestinationBroker = ((double) (numDestinationLeaders + 1)) / numDestinationReplicas;
      return !(proposedLeaderRatioInDestinationBroker > _maximumLeaderRatioPerBroker);
    } else {
      // Replica movement.
      if (isLeader) {
        double proposedLeaderRatioInRemoteBroker = ((double) (numDestinationLeaders + 1)) / (numDestinationReplicas + 1);
        return !(proposedLeaderRatioInRemoteBroker > _maximumLeaderRatioPerBroker);
      }

      int numProposedLocalReplicas = clusterModel.broker(proposal.sourceBrokerId()).replicas().size() - 1;
      double proposedLeaderRatioInSourceBroker = ((double) numLocalLeaders) / (numProposedLocalReplicas);
      return !(proposedLeaderRatioInSourceBroker > _maximumLeaderRatioPerBroker);
    }
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new PleGoal.PleGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    // We only need the latest snapshot and include all the topics.
    return new ModelCompletenessRequirements(1, 0.0, true);
  }

  /**
   * Get the name of this goal. Name of a goal provides an identification for the goal in human readable format.
   */
  @Override
  public String name() {
    return PleGoal.class.getSimpleName();
  }

  /**
   * Check if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param proposal     Proposal containing information about
   * @return True if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingProposal proposal) {
    return true;
  }

  /**
   * This goal does not perform per-broker balancing; hence, this method is irrelevant.
   */
  @Override
  protected Collection<Broker> brokersToBalance(ClusterModel clusterModel) {
    return null;
  }

  /**
   * This goal does not care about self-healing. Hence, if there are dead brokers, there will be replicas on those
   * brokers at the end of the distribution process.
   * This function performs all the balancing in one step.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposals.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws AnalysisInputException, ModelInputException, OptimizationFailureException {
    LOG.debug("Starting PLE with excluded topics = {}", excludedTopics);

    Map<Integer, Integer> numLeadersByBrokerId = new HashMap<>();
    for (Broker broker : clusterModel.brokers()) {
      numLeadersByBrokerId.put(broker.id(), 0);
    }

    Map<String, List<Partition>> partitionsByTopic = clusterModel.getPartitionsByTopic();
    for (Map.Entry<String, List<Partition>> entry: partitionsByTopic.entrySet()) {
      if (excludedTopics.contains(entry.getKey())) {
        LOG.warn("Skipping topic {} as it is explicitly excluded.", entry.getKey());
        continue;
      }

      for (Partition partition : entry.getValue()) {
        // The selected leader is either: (1) The first replica that has 0 leaders so far (2) The replica with the
        // lowest leader ratio.

        Replica newLeader = null;
        Broker curLeaderBroker = partition.leader().broker();
        double minRatio = Double.MAX_VALUE;

        // Select the first replica that has 0 leaders so far (if any) as the new leader.
        if (numLeadersByBrokerId.get(curLeaderBroker.id()) == 0) {
          newLeader = partition.leader();
        } else {
          for (Replica follower : partition.followers()) {
            if (numLeadersByBrokerId.get(follower.broker().id()) == 0) {
              newLeader = follower;
              break;
            }
          }
        }

        // If the leader has not been selected so far, choose the replica with the lowest leader ratio.
        if (newLeader == null) {
          // Leader
          double leaderRatio = ((double) numLeadersByBrokerId.get(curLeaderBroker.id())) / curLeaderBroker.replicas().size();
          int result = AnalyzerUtils.compare(leaderRatio, minRatio, AnalyzerUtils.EPSILON);
          if (result < 0) {
            minRatio = leaderRatio;
            newLeader = partition.leader();
          }

          // Followers
          for (Replica follower : partition.followers()) {
            Broker followerBroker = follower.broker();
            leaderRatio = ((double) numLeadersByBrokerId.get(followerBroker.id())) / followerBroker.replicas().size();
            result = AnalyzerUtils.compare(leaderRatio, minRatio, AnalyzerUtils.EPSILON);
            if (result < 0) {
              minRatio = leaderRatio;
              newLeader = follower;
            }
          }
          if (newLeader == null) {
            throw new OptimizationFailureException("Failed to update the new leader position.");
          }
        }

        // If the leader changed, swap the new leader with the current leader
        if (!partition.leader().equals(newLeader)) {
          if (maybeApplyBalancingAction(clusterModel,
                                        partition.leader(),
                                        Collections.singletonList(newLeader.broker()),
                                        BalancingAction.LEADERSHIP_MOVEMENT,
                                        Collections.emptySet(),
                                        true) == null) {
            throw new AnalysisInputException(String.format("Violated %s. Unable to receive leadership from source"
                                                           + " %d  to destination: %d",
                                                           name(), partition.leader().broker().id(), newLeader.broker().id()));
          }
        }

        // Update the brokers hash
        numLeadersByBrokerId.put(newLeader.broker().id(), numLeadersByBrokerId.get(newLeader.broker().id()) + 1);
      }
    }

    finish();
  }

  /**
   * This goal does not perform per-broker balancing; hence, this method is irrelevant.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws AnalysisInputException, OptimizationFailureException {
    throw new IllegalAccessError("The update goal state method of PLE goal shall not be accessed.");
  }

  /**
   * This goal does not perform per-broker balancing; hence, this method is irrelevant.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    Set<String> excludedTopics)
      throws AnalysisInputException, ModelInputException {
    throw new IllegalAccessError("The rebalance for broker method of PLE goal shall not be accessed.");
  }

  private static class PleGoalStatsComparator implements ClusterModelStatsComparator {

    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // This goal do not care about stats. The optimization would have already failed if the goal is not met.
      return 0;
    }

    @Override
    public String explainLastComparison() {
      return null;
    }
  }
}