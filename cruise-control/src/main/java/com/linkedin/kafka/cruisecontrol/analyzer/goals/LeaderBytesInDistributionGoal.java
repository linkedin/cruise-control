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
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.*;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.DENOMINATOR_FOR_MIN_VALID_WINDOWS_FOR_SELF_HEALING;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;


/**
 * Soft goal to distribute leader bytes evenly. This goal will not do any actual bytes movement; hence, it cannot be
 * used to fix offline replicas or decommission dead brokers.
 */
public class LeaderBytesInDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(LeaderBytesInDistributionGoal.class);

  private double _meanLeaderBytesIn;
  private double _meanLeaderBytesOut;
  private final Map<Resource, Double> _utilizationByResource;

  private Set<Integer> _overLimitBrokerIds;

  public LeaderBytesInDistributionGoal() {
    _utilizationByResource = new HashMap<>();
  }

  /** Testing constructor */
  LeaderBytesInDistributionGoal(BalancingConstraint balancingConstraint) {
    _balancingConstraint = balancingConstraint;
    _utilizationByResource = new HashMap<>();
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

    double sourceReplicaUtilization;
    double newDestLeaderBytesIn;

    switch (action.balancingAction()) {
      case INTER_BROKER_REPLICA_SWAP:

        Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
        Map<Resource, Double> srcBalanceThresholds = balanceThresholds(clusterModel, sourceBroker.id());
        Replica destinationReplica = destinationBroker.replica(action.destinationTopicPartition());

        // Check whether reject the action based on the resulted inbound network utilization on the source broker
        double destinationReplicaUtilization = destinationReplica.load().expectedUtilizationFor(Resource.NW_IN);
        sourceReplicaUtilization = sourceReplica.load().expectedUtilizationFor(Resource.NW_IN);

        double newSourceLeaderBytesIn = sourceBroker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN)
                                        + destinationReplicaUtilization - sourceReplicaUtilization;

        if (newSourceLeaderBytesIn > srcBalanceThresholds.get(Resource.NW_IN)) {
          return REPLICA_REJECT;
        }

        // Check whether reject the action based on the resulted outbound network utilization on the source broker
        destinationReplicaUtilization = destinationReplica.load().expectedUtilizationFor(Resource.NW_OUT);
        sourceReplicaUtilization = sourceReplica.load().expectedUtilizationFor(Resource.NW_OUT);

        double newSourceLeaderBytesOut = sourceBroker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_OUT)
                                        + destinationReplicaUtilization - sourceReplicaUtilization;

        if (newSourceLeaderBytesOut > srcBalanceThresholds.get(Resource.NW_OUT)) {
          return REPLICA_REJECT;
        }

        // Check whether reject the action based on the resulted CPU utilization on the source broker
        destinationReplicaUtilization = destinationReplica.load().expectedUtilizationFor(Resource.CPU);
        sourceReplicaUtilization = sourceReplica.load().expectedUtilizationFor(Resource.CPU);

        double newSourceBrokerCpuUtilization = sourceBroker.load().expectedUtilizationFor(Resource.CPU)
                                              + destinationReplicaUtilization - sourceReplicaUtilization;

        if (newSourceBrokerCpuUtilization > srcBalanceThresholds.get(Resource.CPU)) {
          return BROKER_REJECT;
        }

        // Check whether reject the action based on the resulted disk utilization on the source broker
        destinationReplicaUtilization = destinationReplica.load().expectedUtilizationFor(Resource.DISK);
        sourceReplicaUtilization = sourceReplica.load().expectedUtilizationFor(Resource.DISK);

        double newSourceBrokerDiskUtilization = sourceBroker.load().expectedUtilizationFor(Resource.DISK)
            + destinationReplicaUtilization - sourceReplicaUtilization;

        if (newSourceBrokerDiskUtilization > srcBalanceThresholds.get(Resource.DISK)) {
          return BROKER_REJECT;
        }

        newDestLeaderBytesIn = destinationBroker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN)
            + sourceReplicaUtilization - destinationReplicaUtilization;
        break;
      case INTER_BROKER_REPLICA_MOVEMENT:
      case LEADERSHIP_MOVEMENT:
        sourceReplicaUtilization = sourceReplica.load().expectedUtilizationFor(Resource.NW_IN);
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
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    // While proposals exclude the excludedTopics, the leader bytes in still considers replicas of the excludedTopics.
    _meanLeaderBytesIn = 0.0;
    _overLimitBrokerIds = new HashSet<>();
    // Sort leader replicas for each broker.
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectLeaders())
                              .addSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics))
                              .setScoreFunc(ReplicaSortFunctionFactory.reverseSortByMetricGroupValue(Resource.NW_IN.toString()))
                              .trackSortedReplicasFor(replicaSortName(this, true, true), clusterModel);
  }

  @Override
  public void finish() {
    _finished = true;
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

  private double getMeanLeaderBytesIn(ClusterModel clusterModel) {
    if (_meanLeaderBytesIn == 0.0) {
      _meanLeaderBytesIn = meanLeaderResourceUtilization(clusterModel.brokers(), Resource.NW_IN);
    }
    return _meanLeaderBytesIn;
  }

  private double getMeanLeaderBytesOut(ClusterModel clusterModel) {
    if (_meanLeaderBytesOut == 0.0) {
      _meanLeaderBytesOut = meanLeaderResourceUtilization(clusterModel.brokers(), Resource.NW_OUT);
    }
    return _meanLeaderBytesOut;
  }

  private double meanResourceUtilization(ClusterModel clusterModel, Resource resource) {
    Double meanUtilization = _utilizationByResource.get(resource);
    if (meanUtilization != null) {
      return meanUtilization;
    }

    double accumulator = 0.0;
    int brokerCount = 0;
    for (Broker broker : clusterModel.brokers()) {
      if (!broker.isAlive()) {
        continue;
      }
      accumulator += broker.load().expectedUtilizationFor(resource);
      brokerCount++;
    }
    if (brokerCount == 0) {
      throw new IllegalStateException("Cannot calculate mean utilization since no broker is alive");
    }

    meanUtilization = accumulator / brokerCount;
    _utilizationByResource.put(resource, meanUtilization);
    return meanUtilization;
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
    if (brokerCount == 0) {
      throw new IllegalStateException("Cannot calculate mean leader resource utilization since no broker is alive");
    }

    return accumulator / brokerCount;
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
    double lowUtilizationThreshold =
        _balancingConstraint.lowUtilizationThreshold(Resource.NW_IN) * clusterModel.broker(brokerId).capacityFor(Resource.NW_IN);
    // We only balance leader bytes in rate of the brokers whose leader bytes in rate is higher than the minimum
    // balancing threshold.
    return Math.max(
        lowUtilizationThreshold,
        getMeanLeaderBytesIn(clusterModel) * _balancingConstraint.resourceBalancePercentage(Resource.NW_IN)
    );
  }

  private Map<Resource, Double> balanceThresholds(ClusterModel clusterModel, int brokerId) {
    Map<Resource, Double> balanceThresholdByResource = new HashMap<>(Resource.cachedValues().size());
    for (Resource resource : Resource.values()) {
      balanceThresholdByResource.put(resource, balanceThresholdForResource(clusterModel, brokerId, resource));
    }

    return balanceThresholdByResource;
  }

  private double balanceThresholdForResource(ClusterModel clusterModel, int brokerId, Resource resource) {
    double balanceThreshold;
    double lowUtilizationThreshold = lowUtilizationThresholdForResource(clusterModel, brokerId, resource);

    switch (resource) {
      case CPU:
      case DISK:
        double meanUtilization = meanResourceUtilization(clusterModel, resource);
        balanceThreshold = Math.max(meanUtilization * _balancingConstraint.resourceBalancePercentage(resource), lowUtilizationThreshold);
        break;

      case NW_IN:
        // We only balance leader bytes in rate of the brokers whose leader bytes in rate is higher than the minimum
        // balancing threshold.
        balanceThreshold = Math.max(
            lowUtilizationThreshold,
            getMeanLeaderBytesIn(clusterModel) * _balancingConstraint.resourceBalancePercentage(resource)
        );
        break;

      case NW_OUT:
        // We only balance leader bytes out rate of the brokers whose leader bytes out rate is higher than the minimum
        // balancing threshold.
        balanceThreshold = Math.max(
            lowUtilizationThreshold,
            getMeanLeaderBytesOut(clusterModel) * _balancingConstraint.resourceBalancePercentage(resource)
        );
        break;

      default:
        throw new IllegalStateException();
    }
    return balanceThreshold;
  }

  private double lowUtilizationThresholdForResource(ClusterModel clusterModel, int brokerId, Resource resource) {
    return _balancingConstraint.lowUtilizationThreshold(resource) * clusterModel.broker(brokerId).capacityFor(resource);
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
