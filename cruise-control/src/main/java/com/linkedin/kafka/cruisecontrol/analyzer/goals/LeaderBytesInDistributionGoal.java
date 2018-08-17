/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;


/**
 * Soft goal to distribute leader bytes evenly.
 */
public class LeaderBytesInDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(LeaderBytesInDistributionGoal.class);

  private double _meanLeaderBytesIn;
  private Set<Integer> _overLimitBrokerIds;

  public LeaderBytesInDistributionGoal() {
  }

  /** Testing constructor */
  LeaderBytesInDistributionGoal(BalancingConstraint balancingConstraint) {
    this._balancingConstraint = balancingConstraint;
  }

  /**
   * @deprecated
   * Please use {@link #actionAcceptance(BalancingAction, ClusterModel)} instead.
   */
  @Override
  public boolean isActionAcceptable(BalancingAction action, ClusterModel clusterModel) {
    return actionAcceptance(action, clusterModel) == ACCEPT;
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
        case REPLICA_SWAP:
          if (!destinationBroker.replica(action.destinationTopicPartition()).isLeader()) {
            // No leadership bytes are being swapped between source and destination.
            return ACCEPT;
          }
          break;
        case REPLICA_MOVEMENT:
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
      case REPLICA_SWAP:
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
      case REPLICA_MOVEMENT:
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
    return new ModelCompletenessRequirements(_numWindows, _minMonitoredPartitionPercentage, false);
  }

  @Override
  public String name() {
    return LeaderBytesInDistributionGoal.class.getSimpleName();
  }

  @Override
  protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
    // Brokers having inbound network traffic over the balance threshold for inbound traffic are eligible for balancing.
    SortedSet<Broker> brokersToBalance = clusterModel.brokers();
    for (Iterator<Broker> iterator = brokersToBalance.iterator(); iterator.hasNext(); ) {
      Broker broker = iterator.next();
      double brokerUtilizationForNwIn = broker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN);
      if (brokerUtilizationForNwIn <= balanceThreshold(clusterModel, broker.id())) {
        iterator.remove();
      }
    }

    return brokersToBalance;
  }

  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    if (action.balancingAction() != ActionType.LEADERSHIP_MOVEMENT) {
      throw new IllegalStateException("Found balancing action " + action.balancingAction() +
          " but expected leadership movement.");
    }
    return actionAcceptance(action, clusterModel) == ACCEPT;
  }

  @Override
  protected void initGoalState(ClusterModel clusterModel, Set<String> excludedTopics) {
    // While proposals exclude the excludedTopics, the leader bytes in still considers replicas of the excludedTopics.
    _meanLeaderBytesIn = 0.0;
    _overLimitBrokerIds = new HashSet<>();
  }

  @Override
  protected void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics) {
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
                                    Set<String> excludedTopics) {

    double balanceThreshold = balanceThreshold(clusterModel, broker.id());
    if (broker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN) < balanceThreshold) {
      return;
    }

    List<Replica> leaderReplicasSortedByBytesIn = broker.replicas().stream()
        .filter(Replica::isLeader)
        .filter(r -> !shouldExclude(r, excludedTopics))
        .sorted((a, b) -> Double.compare(b.load().expectedUtilizationFor(Resource.NW_IN), a.load().expectedUtilizationFor(Resource.NW_IN)))
        .collect(Collectors.toList());

    boolean overThreshold = true;
    Iterator<Replica> leaderReplicaIt = leaderReplicasSortedByBytesIn.iterator();
    while (overThreshold && leaderReplicaIt.hasNext()) {
      Replica leaderReplica = leaderReplicaIt.next();
      List<Replica> followers = clusterModel.partition(leaderReplica.topicPartition()).followers();
      List<Broker> eligibleBrokers = followers.stream().map(Replica::broker)
          .sorted(Comparator.comparingDouble(a -> a.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN)))
          .collect(Collectors.toList());
      maybeApplyBalancingAction(clusterModel, leaderReplica, eligibleBrokers, ActionType.LEADERSHIP_MOVEMENT,
                                optimizedGoals);
      overThreshold = broker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN) > balanceThreshold;
    }
    if (overThreshold) {
      _overLimitBrokerIds.add(broker.id());
    }
  }

  private void initMeanLeaderBytesIn(ClusterModel clusterModel) {
    if (_meanLeaderBytesIn == 0.0) {
      _meanLeaderBytesIn = meanLeaderResourceUtilization(clusterModel.brokers(), Resource.NW_IN);
    }
  }

  private static double meanLeaderResourceUtilization(Collection<Broker> brokers, Resource resource) {
    double accumulator = 0.0;
    int brokerCount = 0;
    for (Broker broker : brokers) {
      if (!broker.isAlive()) {
        continue;
      }
      accumulator += broker.leadershipLoadForNwResources().expectedUtilizationFor(resource);
      brokerCount++;
    }
    return accumulator / brokerCount;
  }

  /**
   * In this context of this goal the balance threshold can not be measured against an absolute number since leader bytes
   * in is constrained by network capacity but also depends on follower bytes in. We also reuse the NW_IN low utilization
   * threshold to avoid unnecessary rebalance.
   * @param clusterModel non-null
   * @param brokerId the brokerId
   * @return a non-negative value
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
