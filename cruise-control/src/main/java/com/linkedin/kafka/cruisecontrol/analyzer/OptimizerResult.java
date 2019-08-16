/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.MAX_BALANCEDNESS_SCORE;


/**
 * A class for representing the results of goal optimizer. The results include stats by goal priority and
 * optimization proposals.
 */
public class OptimizerResult {
  private static final String NUM_INTER_BROKER_REPLICA_MOVEMENTS = "numReplicaMovements";
  private static final String INTER_BROKER_DATA_TO_MOVE_MB = "dataToMoveMB";
  private static final String NUM_INTRA_BROKER_REPLICA_MOVEMENTS = "numIntraBrokerReplicaMovements";
  private static final String INTRA_BROKER_DATA_TO_MOVE_MB = "intraBrokerDataToMoveMB";
  private static final String NUM_LEADER_MOVEMENTS = "numLeaderMovements";
  private static final String RECENT_WINDOWS = "recentWindows";
  private static final String MONITORED_PARTITIONS_PERCENTAGE = "monitoredPartitionsPercentage";
  private static final String EXCLUDED_TOPICS = "excludedTopics";
  private static final String EXCLUDED_BROKERS_FOR_LEADERSHIP = "excludedBrokersForLeadership";
  private static final String EXCLUDED_BROKERS_FOR_REPLICA_MOVE = "excludedBrokersForReplicaMove";
  private static final String ON_DEMAND_BALANCEDNESS_SCORE_AFTER = "onDemandBalancednessScoreAfter";
  private static final String ON_DEMAND_BALANCEDNESS_SCORE_BEFORE = "onDemandBalancednessScoreBefore";
  private final Map<String, Goal.ClusterModelStatsComparator> _clusterModelStatsComparatorByGoalName;
  private final LinkedHashMap<String, ClusterModelStats> _statsByGoalName;
  private final Set<ExecutionProposal> _proposals;
  private final Set<String> _violatedGoalNamesBeforeOptimization;
  private final Set<String> _violatedGoalNamesAfterOptimization;
  private final BrokerStats _brokerStatsBeforeOptimization;
  private final BrokerStats _brokerStatsAfterOptimization;
  private final ModelGeneration _modelGeneration;
  private final ClusterModelStats _clusterModelStats;
  private final Map<Integer, String> _capacityEstimationInfoByBrokerId;
  private final OptimizationOptions _optimizationOptions;
  private final double _onDemandBalancednessScoreBefore;
  private final double _onDemandBalancednessScoreAfter;

  OptimizerResult(LinkedHashMap<Goal, ClusterModelStats> statsByGoalPriority,
                  Set<String> violatedGoalNamesBeforeOptimization,
                  Set<String> violatedGoalNamesAfterOptimization,
                  Set<ExecutionProposal> proposals,
                  BrokerStats brokerStatsBeforeOptimization,
                  BrokerStats brokerStatsAfterOptimization,
                  ModelGeneration modelGeneration,
                  ClusterModelStats clusterModelStats,
                  Map<Integer, String> capacityEstimationInfoByBrokerId,
                  OptimizationOptions optimizationOptions,
                  Map<String, Double> balancednessCostByGoal) {
    _clusterModelStatsComparatorByGoalName = new LinkedHashMap<>(statsByGoalPriority.size());
    _statsByGoalName = new LinkedHashMap<>(statsByGoalPriority.size());
    for (Map.Entry<Goal, ClusterModelStats> entry : statsByGoalPriority.entrySet()) {
      String goalName = entry.getKey().name();
      Goal.ClusterModelStatsComparator comparator = entry.getKey().clusterModelStatsComparator();
      _clusterModelStatsComparatorByGoalName.put(goalName, comparator);
      _statsByGoalName.put(goalName, entry.getValue());
    }

    _violatedGoalNamesBeforeOptimization = violatedGoalNamesBeforeOptimization;
    _violatedGoalNamesAfterOptimization = violatedGoalNamesAfterOptimization;
    _proposals = proposals;
    _brokerStatsBeforeOptimization = brokerStatsBeforeOptimization;
    _brokerStatsAfterOptimization = brokerStatsAfterOptimization;
    _modelGeneration = modelGeneration;
    _clusterModelStats = clusterModelStats;
    _capacityEstimationInfoByBrokerId = capacityEstimationInfoByBrokerId;
    _optimizationOptions = optimizationOptions;
    // Populate on-demand balancedness score before and after.
    _onDemandBalancednessScoreBefore = onDemandBalancednessScore(balancednessCostByGoal, _violatedGoalNamesBeforeOptimization);
    _onDemandBalancednessScoreAfter = onDemandBalancednessScore(balancednessCostByGoal, _violatedGoalNamesAfterOptimization);
  }

  private double onDemandBalancednessScore(Map<String, Double> balancednessCostByGoal, Set<String> violatedGoals) {
    double onDemandBalancednessScore = MAX_BALANCEDNESS_SCORE;
    for (String goalName : _statsByGoalName.keySet()) {
      if (violatedGoals.contains(goalName)) {
        onDemandBalancednessScore -= balancednessCostByGoal.get(goalName);
      }
    }
    return onDemandBalancednessScore;
  }

  public Map<String, Goal.ClusterModelStatsComparator> clusterModelStatsComparatorByGoalName() {
    return _clusterModelStatsComparatorByGoalName;
  }

  /**
   * @return Stats by goal name ordered by priority.
   */
  public LinkedHashMap<String, ClusterModelStats> statsByGoalName() {
    return _statsByGoalName;
  }

  public Set<ExecutionProposal> goalProposals() {
    return _proposals;
  }

  public Set<String> violatedGoalsBeforeOptimization() {
    return _violatedGoalNamesBeforeOptimization;
  }

  public Set<String> violatedGoalsAfterOptimization() {
    return _violatedGoalNamesAfterOptimization;
  }

  public ModelGeneration modelGeneration() {
    return _modelGeneration;
  }

  public ClusterModelStats clusterModelStats() {
    return _clusterModelStats;
  }

  public BrokerStats brokerStatsBeforeOptimization() {
    return _brokerStatsBeforeOptimization;
  }

  public BrokerStats brokerStatsAfterOptimization() {
    return _brokerStatsAfterOptimization;
  }

  public boolean isCapacityEstimated() {
    return !_capacityEstimationInfoByBrokerId.isEmpty();
  }

  public Map<Integer, String> capacityEstimationInfoByBrokerId() {
    return Collections.unmodifiableMap(_capacityEstimationInfoByBrokerId);
  }

  public Set<String> excludedTopics() {
    return _optimizationOptions.excludedTopics();
  }

  public Set<Integer> excludedBrokersForLeadership() {
    return _optimizationOptions.excludedBrokersForLeadership();
  }

  public Set<Integer> excludedBrokersForReplicaMove() {
    return _optimizationOptions.excludedBrokersForReplicaMove();
  }

  /**
   * The topics of partitions which are going to be modified by proposals.
   */
  public Set<String> topicsWithReplicationFactorChange() {
    Set<String> topics = new HashSet<>(_proposals.size());
    _proposals.stream().filter(p -> p.newReplicas().size() != p.oldReplicas().size()).forEach(p -> topics.add(p.topic()));
    return topics;
  }

  private List<Number> getMovementStats() {
    int numInterBrokerReplicaMovements = 0;
    int numIntraBrokerReplicaMovements = 0;
    int numLeadershipMovements = 0;
    long interBrokerDataToMove = 0L;
    long intraBrokerDataToMove = 0L;
    for (ExecutionProposal p : _proposals) {
      if (!p.replicasToAdd().isEmpty() || !p.replicasToRemove().isEmpty()) {
        numInterBrokerReplicaMovements++;
        interBrokerDataToMove += p.interBrokerDataToMoveInMB();
      } else if (!p.replicasToMoveBetweenDisksByBroker().isEmpty()) {
        numIntraBrokerReplicaMovements += p.replicasToMoveBetweenDisksByBroker().size();
        intraBrokerDataToMove += p.intraBrokerDataToMoveInMB() * p.replicasToMoveBetweenDisksByBroker().size();
      } else {
        numLeadershipMovements++;
      }
    }
    return Arrays.asList(numInterBrokerReplicaMovements, interBrokerDataToMove,
                         numIntraBrokerReplicaMovements, intraBrokerDataToMove,
                         numLeadershipMovements);
  }

  public String getProposalSummary() {
    List<Number> moveStats = getMovementStats();
    return String.format("%n%nOptimization has %d inter-broker replica(%d MB) moves, %d intra-broker replica(%d MB) moves"
                         + " and %d leadership moves with a cluster model of %d recent windows and %.3f%% of the partitions"
                         + " covered.%nExcluded Topics: %s.%nExcluded Brokers For Leadership: %s.%nExcluded Brokers For "
                         + "Replica Move: %s.%nCounts: %s%nOn-demand Balancedness Score Before (%.3f) After(%.3f).",
                         moveStats.get(0).intValue(), moveStats.get(1).longValue(), moveStats.get(2).intValue(),
                         moveStats.get(3).longValue(), moveStats.get(4).intValue(), _clusterModelStats.numSnapshotWindows(),
                         _clusterModelStats.monitoredPartitionsPercentage(), excludedTopics(),
                         excludedBrokersForLeadership(), excludedBrokersForReplicaMove(), _clusterModelStats.toStringCounts(),
                         _onDemandBalancednessScoreBefore, _onDemandBalancednessScoreAfter);
  }

  public Map<String, Object> getProposalSummaryForJson() {
    List<Number> moveStats = getMovementStats();
    Map<String, Object> ret = new HashMap<>(12);
    ret.put(NUM_INTER_BROKER_REPLICA_MOVEMENTS, moveStats.get(0).intValue());
    ret.put(INTER_BROKER_DATA_TO_MOVE_MB, moveStats.get(1).longValue());
    ret.put(NUM_INTRA_BROKER_REPLICA_MOVEMENTS, moveStats.get(2).intValue());
    ret.put(INTRA_BROKER_DATA_TO_MOVE_MB, moveStats.get(3).longValue());
    ret.put(NUM_LEADER_MOVEMENTS, moveStats.get(4).intValue());
    ret.put(RECENT_WINDOWS, _clusterModelStats.numSnapshotWindows());
    ret.put(MONITORED_PARTITIONS_PERCENTAGE, _clusterModelStats.monitoredPartitionsPercentage());
    ret.put(EXCLUDED_TOPICS, excludedTopics());
    ret.put(EXCLUDED_BROKERS_FOR_LEADERSHIP, excludedBrokersForLeadership());
    ret.put(EXCLUDED_BROKERS_FOR_REPLICA_MOVE, excludedBrokersForReplicaMove());
    ret.put(ON_DEMAND_BALANCEDNESS_SCORE_BEFORE, _onDemandBalancednessScoreBefore);
    ret.put(ON_DEMAND_BALANCEDNESS_SCORE_AFTER, _onDemandBalancednessScoreAfter);
    return ret;
  }
}
