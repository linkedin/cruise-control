/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.RawAndDerivedResource;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Soft goal to distribute leader bytes evenly.
 */
public class LeaderBytesInDistributionGoals extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(LeaderBytesInDistributionGoals.class);

  private double _meanLeaderBytesIn;
  private Set<Integer> _overLimitBrokerIds;

  public LeaderBytesInDistributionGoals() {
  }

  /** Testing constructor */
  LeaderBytesInDistributionGoals(BalancingConstraint balancingConstraint) {
    this._balancingConstraint = balancingConstraint;
  }

  /**
   * A proposal is acceptable if it does not move the leader bytes in above the threshold for leader bytes in.
   *
   * @param proposal     Proposal to be checked for acceptance.
   * @param clusterModel State of the cluster before application of the proposal.
   * @return true if the proposal does not unbalance leader bytes in.
   */
  @Override
  public boolean isProposalAcceptable(BalancingProposal proposal, ClusterModel clusterModel) {
    Replica sourceReplica = clusterModel.broker(proposal.sourceBrokerId()).replica(proposal.topicPartition());
    Broker destinationBroker = clusterModel.broker(proposal.destinationBrokerId());

    initMeanLeaderBytesIn(clusterModel);

    if (!sourceReplica.isLeader()) {
      // No leadership bytes are being moved so I don't care
      return true;
    }

    double balanceThreshold = balanceThreshold(clusterModel, destinationBroker.id());
    double newDestLeaderBytesIn = destinationBroker.leadershipLoad().expectedUtilizationFor(Resource.NW_IN) +
        sourceReplica.load().expectedUtilizationFor(Resource.NW_IN);
    if (newDestLeaderBytesIn > balanceThreshold) {
      return false;
    }

    return true;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new LeaderBytesInDistributionGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(_numSnapshots, _minMonitoredPartitionPercentage, false);
  }

  @Override
  public String name() {
    return LeaderBytesInDistributionGoals.class.getSimpleName();
  }

  @Override
  protected Collection<Broker> brokersToBalance(ClusterModel clusterModel) {
    return clusterModel.brokers().stream()
        .filter(b -> b.leadershipLoad().expectedUtilizationFor(Resource.NW_IN) > balanceThreshold(clusterModel, b.id()))
        .collect(Collectors.toList());
  }

  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingProposal proposal) {
    if (proposal.balancingAction() != BalancingAction.LEADERSHIP_MOVEMENT) {
      throw new IllegalStateException("Found balancing action " + proposal.balancingAction() +
          " but expected leadership movement.");
    }
    return isProposalAcceptable(proposal, clusterModel);
  }

  @Override
  protected void initGoalState(ClusterModel clusterModel) throws AnalysisInputException, ModelInputException {
    _meanLeaderBytesIn = 0.0;
    _overLimitBrokerIds = new HashSet<>();
  }

  @Override
  protected void updateGoalState(ClusterModel clusterModel) throws AnalysisInputException {
    if (!_overLimitBrokerIds.isEmpty()) {
      LOG.warn("There were still {} brokers over the limit.", _overLimitBrokerIds.size());
      _succeeded = false;
    }
    finish();
  }

  @Override
  protected void rebalanceForBroker(Broker broker, ClusterModel clusterModel, Set<Goal> optimizedGoals,
      Set<String> excludedTopics) throws AnalysisInputException, ModelInputException {

    double balanceThreshold = balanceThreshold(clusterModel, broker.id());
    if (broker.leadershipLoad().expectedUtilizationFor(Resource.NW_IN) < balanceThreshold) {
      return;
    }

    List<Replica> leaderReplicasSortedByBytesIn = broker.replicas().stream()
        .filter(r -> r.isLeader())
        .filter(r -> !excludedTopics.contains(r.topicPartition().topic()))
        .sorted((a, b) -> Double.compare(b.load().expectedUtilizationFor(Resource.NW_IN), a.load().expectedUtilizationFor(Resource.NW_IN)))
        .collect(Collectors.toList());

    boolean overThreshold = true;
    Iterator<Replica> leaderReplicaIt = leaderReplicasSortedByBytesIn.iterator();
    while (overThreshold && leaderReplicaIt.hasNext()) {
      Replica leaderReplica = leaderReplicaIt.next();
      List<Replica> followers = clusterModel.partition(leaderReplica.topicPartition()).followers();
      List<Broker> eligibleBrokers = followers.stream().map(Replica::broker)
          .sorted(Comparator.comparingDouble(a -> a.leadershipLoad().expectedUtilizationFor(Resource.NW_IN)))
          .collect(Collectors.toList());
      maybeApplyBalancingAction(clusterModel, leaderReplica, eligibleBrokers, BalancingAction.LEADERSHIP_MOVEMENT,
          optimizedGoals);
      overThreshold = broker.leadershipLoad().expectedUtilizationFor(Resource.NW_IN) > balanceThreshold;
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
      accumulator += broker.leadershipLoad().expectedUtilizationFor(resource);
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
    // We only balance leader bytes in rate of the brokers whose whose leader bytes in rate is higher than the minimum
    // balancing threshold.
    return Math.max(_meanLeaderBytesIn * _balancingConstraint.balancePercentage(Resource.NW_IN), lowUtilizationThreshold);
  }

  private class LeaderBytesInDistributionGoalStatsComparator implements ClusterModelStatsComparator {
    private String _reasonForLastNegativeResult;

    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // Did we need to change anything?
      double[] stat1 = stats1.utilizationMatrix()[RawAndDerivedResource.LEADER_NW_IN.ordinal()];
      double meanPreLeaderBytesIn = new Mean().evaluate(stat1, 0, stat1.length);
      double threshold = meanPreLeaderBytesIn * _balancingConstraint.balancePercentage(Resource.NW_IN);
      if (Arrays.stream(stat1).noneMatch(v -> v > threshold)) {
        return 1;
      }

      double[] stat2 = stats2.utilizationMatrix()[RawAndDerivedResource.LEADER_NW_IN.ordinal()];
      double variance1 = new Variance().evaluate(stat1);
      double variance2 = new Variance().evaluate(stat2);
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
