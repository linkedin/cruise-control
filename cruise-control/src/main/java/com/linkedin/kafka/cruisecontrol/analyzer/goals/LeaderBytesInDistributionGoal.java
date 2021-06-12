/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.DENOMINATOR_FOR_MIN_VALID_WINDOWS_FOR_SELF_HEALING;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;


/**
 * Soft goal to distribute leader bytes evenly. This goal will not do any actual bytes movement; hence, it cannot be
 * used to fix offline replicas or decommission dead brokers.
 *
 * Warning: This custom goal does not take resource heterogeneity of inbound network capacity into account.
 */
public class LeaderBytesInDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(LeaderBytesInDistributionGoal.class);

  // The balance limits are based on brokers not excluded for replica moves.
  private double _meanLeaderBytesIn;
  private Set<Integer> _overLimitBrokerIds;
  // This is used to identify brokers not excluded for replica moves.
  private Set<Integer> _brokersAllowedReplicaMove;

  public LeaderBytesInDistributionGoal() {
  }

  /** Testing constructor */
  LeaderBytesInDistributionGoal(BalancingConstraint balancingConstraint) {
    _balancingConstraint = balancingConstraint;
  }

  /**
   * An action is acceptable if it does not move the leader bytes in above the threshold for leader bytes in.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel State of the cluster before application of the action.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    Replica sourceReplica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());

    initMeanLeaderBytesIn(clusterModel);

    if (!sourceReplica.isLeader()) {
      switch (action.balancingAction()) {
        case INTER_BROKER_REPLICA_SWAP:
          if (!destinationBroker.replica(action.destinationTopicPartition()).isLeader()) {
            // No leadership bytes are being swapped between source and destination.
            return ACCEPT;
          }
          break;
        case INTER_BROKER_REPLICA_MOVEMENT:
          // No leadership bytes are being moved to destination.
          return ACCEPT;
        case LEADERSHIP_MOVEMENT:
          throw new IllegalStateException("Attempt to move leadership from the follower.");
        default:
          throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
      }
    }

    double sourceReplicaUtilization = sourceReplica.load().expectedUtilizationFor(Resource.NW_IN);
    double newDestLeaderBytesIn;

    switch (action.balancingAction()) {
      case INTER_BROKER_REPLICA_SWAP:
        double destinationReplicaUtilization = destinationBroker.replica(action.destinationTopicPartition()).load()
            .expectedUtilizationFor(Resource.NW_IN);
        newDestLeaderBytesIn = destinationBroker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN)
                               + sourceReplicaUtilization - destinationReplicaUtilization;

        Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
        double newSourceLeaderBytesIn = sourceBroker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN)
                                        + destinationReplicaUtilization - sourceReplicaUtilization;

        if (newSourceLeaderBytesIn > balanceThreshold(clusterModel, sourceBroker.id())) {
          return REPLICA_REJECT;
        }
        break;
      case INTER_BROKER_REPLICA_MOVEMENT:
      case LEADERSHIP_MOVEMENT:
        newDestLeaderBytesIn = destinationBroker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN)
                               + sourceReplicaUtilization;
        break;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }

    return !(newDestLeaderBytesIn > balanceThreshold(clusterModel, destinationBroker.id())) ? ACCEPT : REPLICA_REJECT;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new LeaderBytesInDistributionGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(Math.max(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING,
                                                      _numWindows / DENOMINATOR_FOR_MIN_VALID_WINDOWS_FOR_SELF_HEALING),
                                             _minMonitoredPartitionPercentage, false);
  }

  @Override
  public String name() {
    return LeaderBytesInDistributionGoal.class.getSimpleName();
  }

  @Override
  public boolean isHardGoal() {
    return false;
  }

  @Override
  protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
    // Brokers having inbound network traffic over the balance threshold for inbound traffic are eligible for balancing.
    SortedSet<Broker> brokersToBalance = new TreeSet<>();
    for (Broker broker : clusterModel.brokers()) {
      if (broker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN) > balanceThreshold(clusterModel, broker.id())) {
        brokersToBalance.add(broker);
      }
    }

    return brokersToBalance;
  }

  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    if (action.balancingAction() != ActionType.LEADERSHIP_MOVEMENT) {
      throw new IllegalStateException("Found balancing action " + action.balancingAction()
                                      + " but expected leadership movement.");
    }
    return actionAcceptance(action, clusterModel) == ACCEPT;
  }

  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    _brokersAllowedReplicaMove = GoalUtils.aliveBrokersNotExcludedForReplicaMove(clusterModel, optimizationOptions);
    if (_brokersAllowedReplicaMove.isEmpty()) {
      // Handle the case when all alive brokers are excluded from replica moves.
      ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
          .numBrokers(clusterModel.maxReplicationFactor()).build();
      throw new OptimizationFailureException(String.format("[%s] All alive brokers are excluded from replica moves.", name()), recommendation);
    }
    _meanLeaderBytesIn = 0.0;
    _overLimitBrokerIds = new HashSet<>();
    // Sort leader replicas for each broker.
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectLeaders())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .setScoreFunc(ReplicaSortFunctionFactory.reverseSortByMetricGroupValue(Resource.NW_IN.toString()))
                              .trackSortedReplicasFor(replicaSortName(this, true, true), clusterModel);
  }

  @Override
  public void finish() {
    super.finish();
    // Clean up the memory
    _overLimitBrokerIds.clear();
  }

  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    // While proposals exclude the excludedTopics, the leader bytes in still considers replicas of the excludedTopics.
    if (!_overLimitBrokerIds.isEmpty()) {
      LOG.warn("There were still {} brokers over the limit.", _overLimitBrokerIds.size());
      _succeeded = false;
    }
    finish();
  }

  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) {

    double balanceThreshold = balanceThreshold(clusterModel, broker.id());
    if (broker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN) < balanceThreshold) {
      return;
    }

    boolean overThreshold = true;
    Iterator<Replica> leaderReplicaIt = broker.trackedSortedReplicas(replicaSortName(this, true, true)).sortedReplicas(true).iterator();
    while (overThreshold && leaderReplicaIt.hasNext()) {
      Replica leaderReplica = leaderReplicaIt.next();
      List<Replica> onlineFollowers = clusterModel.partition(leaderReplica.topicPartition()).onlineFollowers();
      List<Broker> eligibleBrokers = onlineFollowers.stream().map(Replica::broker)
          .sorted(Comparator.comparingDouble(a -> a.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN)))
          .collect(Collectors.toList());
      maybeApplyBalancingAction(clusterModel, leaderReplica, eligibleBrokers, ActionType.LEADERSHIP_MOVEMENT,
                                optimizedGoals, optimizationOptions);
      overThreshold = broker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN) > balanceThreshold;
    }
    if (overThreshold) {
      _overLimitBrokerIds.add(broker.id());
    }
  }

  private void initMeanLeaderBytesIn(ClusterModel clusterModel) {
    if (_meanLeaderBytesIn == 0.0) {
      double bytesIn = clusterModel.aliveBrokers().stream().mapToDouble(b -> b.leadershipLoadForNwResources()
                                                                              .expectedUtilizationFor(Resource.NW_IN)).sum();
      _meanLeaderBytesIn = bytesIn / _brokersAllowedReplicaMove.size();
    }
  }

  /**
   * In this context of this goal the balance threshold can not be measured against an absolute number since leader bytes
   * in is constrained by network capacity but also depends on follower bytes in. We also reuse the NW_IN low utilization
   * threshold to avoid unnecessary rebalance.
   * @param clusterModel non-null
   * @param brokerId the brokerId
   * @return A non-negative value
   */
  private double balanceThreshold(ClusterModel clusterModel, int brokerId) {
    initMeanLeaderBytesIn(clusterModel);
    double lowUtilizationThreshold =
        _balancingConstraint.lowUtilizationThreshold(Resource.NW_IN) * clusterModel.broker(brokerId).capacityFor(Resource.NW_IN);
    // We only balance leader bytes in rate of the brokers whose leader bytes in rate is higher than the minimum
    // balancing threshold.
    return Math.max(_meanLeaderBytesIn * _balancingConstraint.resourceBalancePercentage(Resource.NW_IN), lowUtilizationThreshold);
  }

  private class LeaderBytesInDistributionGoalStatsComparator implements ClusterModelStatsComparator {
    private String _reasonForLastNegativeResult;

    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      double meanPreLeaderBytesIn = stats1.resourceUtilizationStats().get(Statistic.AVG).get(Resource.NW_IN);
      double threshold = meanPreLeaderBytesIn * _balancingConstraint.resourceBalancePercentage(Resource.NW_IN);
      if (stats1.resourceUtilizationStats().get(Statistic.MAX).get(Resource.NW_IN) <= threshold) {
        return 1;
      }

      // If there are brokers with inbound network load over the threshold, the standard deviation of utilization
      // must not increase compared the initial stats. Otherwise, the goal is producing a worse cluster state.
      double variance1 = stats1.resourceUtilizationStats().get(Statistic.ST_DEV).get(Resource.NW_IN);
      double variance2 = stats2.resourceUtilizationStats().get(Statistic.ST_DEV).get(Resource.NW_IN);
      int result = AnalyzerUtils.compare(Math.sqrt(variance2), Math.sqrt(variance1), Resource.NW_IN);
      if (result < 0) {
        _reasonForLastNegativeResult = String.format("Violated leader bytes in balancing. preVariance: %.3f "
                                                         + "postVariance: %.3f.", variance2, variance1);
      }
      return result;
    }

    @Override
    public String explainLastComparison() {
      return _reasonForLastNegativeResult;
    }
  }
}
