/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.MAX_BALANCEDNESS_SCORE;


/**
 * A class for representing the results of goal optimizer. The results include stats by goal priority and
 * optimization proposals.
 */
@JsonResponseClass
public class OptimizerResult {
  @JsonResponseField
  private static final String NUM_INTER_BROKER_REPLICA_MOVEMENTS = "numReplicaMovements";
  @JsonResponseField
  private static final String INTER_BROKER_DATA_TO_MOVE_MB = "dataToMoveMB";
  @JsonResponseField
  private static final String NUM_INTRA_BROKER_REPLICA_MOVEMENTS = "numIntraBrokerReplicaMovements";
  @JsonResponseField
  private static final String INTRA_BROKER_DATA_TO_MOVE_MB = "intraBrokerDataToMoveMB";
  @JsonResponseField
  private static final String NUM_LEADER_MOVEMENTS = "numLeaderMovements";
  @JsonResponseField
  private static final String RECENT_WINDOWS = "recentWindows";
  @JsonResponseField
  private static final String MONITORED_PARTITIONS_PERCENTAGE = "monitoredPartitionsPercentage";
  @JsonResponseField
  private static final String EXCLUDED_TOPICS = "excludedTopics";
  @JsonResponseField
  private static final String EXCLUDED_BROKERS_FOR_LEADERSHIP = "excludedBrokersForLeadership";
  @JsonResponseField
  private static final String EXCLUDED_BROKERS_FOR_REPLICA_MOVE = "excludedBrokersForReplicaMove";
  @JsonResponseField
  private static final String ON_DEMAND_BALANCEDNESS_SCORE_AFTER = "onDemandBalancednessScoreAfter";
  @JsonResponseField
  private static final String ON_DEMAND_BALANCEDNESS_SCORE_BEFORE = "onDemandBalancednessScoreBefore";
  @JsonResponseField
  private static final String PROVISION_STATUS = "provisionStatus";
  @JsonResponseField
  private static final String PROVISION_RECOMMENDATION = "provisionRecommendation";
  private static final String VIOLATED = "VIOLATED";
  private static final String FIXED = "FIXED";
  private static final String NO_ACTION = "NO-ACTION";
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
  private final Map<String, Duration> _optimizationDurationByGoal;
  private final ProvisionResponse _provisionResponse;

  OptimizerResult(LinkedHashMap<Goal, ClusterModelStats> statsByGoalPriority,
                  Set<String> violatedGoalNamesBeforeOptimization,
                  Set<String> violatedGoalNamesAfterOptimization,
                  Set<ExecutionProposal> proposals,
                  BrokerStats brokerStatsBeforeOptimization,
                  ClusterModel clusterModel,
                  OptimizationOptions optimizationOptions,
                  Map<String, Double> balancednessCostByGoal,
                  Map<String, Duration> optimizationDurationByGoal,
                  ProvisionResponse provisionResponse) {
    validateNotNull(statsByGoalPriority, "The stats by goal priority cannot be null.");
    validateNotNull(optimizationDurationByGoal, "The optimization duration by goal priority cannot be null.");
    if (statsByGoalPriority.isEmpty()) {
      throw new IllegalArgumentException("At least one stats by goal priority must be provided to get an optimizer result.");
    }
    BalancingConstraint balancingConstraint = statsByGoalPriority.entrySet().stream().findAny().get().getValue().balancingConstraint();
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
    _brokerStatsAfterOptimization = clusterModel.brokerStats(null);
    _modelGeneration = clusterModel.generation();
    _clusterModelStats = clusterModel.getClusterStats(balancingConstraint, optimizationOptions);
    _capacityEstimationInfoByBrokerId = clusterModel.capacityEstimationInfoByBrokerId();
    _optimizationOptions = optimizationOptions;
    // Populate on-demand balancedness score before and after.
    _onDemandBalancednessScoreBefore = onDemandBalancednessScore(balancednessCostByGoal, _violatedGoalNamesBeforeOptimization);
    _onDemandBalancednessScoreAfter = onDemandBalancednessScore(balancednessCostByGoal, _violatedGoalNamesAfterOptimization);
    _optimizationDurationByGoal = optimizationDurationByGoal;
    _provisionResponse = provisionResponse;
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

  /**
   * @return Cluster model stats comparator by goal names.
   */
  public Map<String, Goal.ClusterModelStatsComparator> clusterModelStatsComparatorByGoalName() {
    return Collections.unmodifiableMap(_clusterModelStatsComparatorByGoalName);
  }

  /**
   * @return Stats by goal name ordered by priority.
   */
  public LinkedHashMap<String, ClusterModelStats> statsByGoalName() {
    return _statsByGoalName;
  }

  /**
   * @return Goal proposals.
   */
  public Set<ExecutionProposal> goalProposals() {
    return Collections.unmodifiableSet(_proposals);
  }

  /**
   * @return Violated goals before optimization.
   */
  public Set<String> violatedGoalsBeforeOptimization() {
    return Collections.unmodifiableSet(_violatedGoalNamesBeforeOptimization);
  }

  /**
   * @return Violated goals after optimization.
   */
  public Set<String> violatedGoalsAfterOptimization() {
    return Collections.unmodifiableSet(_violatedGoalNamesAfterOptimization);
  }

  /**
   * @return The model generation.
   */
  public ModelGeneration modelGeneration() {
    return _modelGeneration;
  }

  /**
   * @return The cluster model stats.
   */
  public ClusterModelStats clusterModelStats() {
    return _clusterModelStats;
  }

  /**
   * @return The broker stats before optimization.
   */
  public BrokerStats brokerStatsBeforeOptimization() {
    return _brokerStatsBeforeOptimization;
  }

  /**
   * @return The broker stats after optimization.
   */
  public BrokerStats brokerStatsAfterOptimization() {
    return _brokerStatsAfterOptimization;
  }

  /**
   * @return {@code true} if the broker capacity is estimated in the underlying cluster model, {@code false} otherwise.
   */
  public boolean isCapacityEstimated() {
    return !_capacityEstimationInfoByBrokerId.isEmpty();
  }

  /**
   * @return The capacity estimation info by broker id.
   */
  public Map<Integer, String> capacityEstimationInfoByBrokerId() {
    return Collections.unmodifiableMap(_capacityEstimationInfoByBrokerId);
  }

  /**
   * @param goalName Name of the goal for which the optimization duration will be retrieved.
   * @return Optimization duration for the given goal name -- i.e. the time it took to optimize the given goal, or {@code null} if the goal
   * is not present in the optimized goals in this optimizer result.
   */
  public Duration optimizationDuration(String goalName) {
    return _optimizationDurationByGoal.get(goalName);
  }

  /**
   * @return The excluded topics in the optimization options.
   */
  public Set<String> excludedTopics() {
    return _optimizationOptions.excludedTopics();
  }

  /**
   * @return The excluded brokers for leadership transfer in the optimization options.
   */
  public Set<Integer> excludedBrokersForLeadership() {
    return _optimizationOptions.excludedBrokersForLeadership();
  }

  /**
   * @return The excluded brokers for replica moves in the optimization options.
   */
  public Set<Integer> excludedBrokersForReplicaMove() {
    return _optimizationOptions.excludedBrokersForReplicaMove();
  }

  /**
    @param goalName Goal name.
   * @return The string describing goal result.
   */
  public String goalResultDescription(String goalName) {
    return _violatedGoalNamesBeforeOptimization.contains(goalName) ? _violatedGoalNamesAfterOptimization.contains(goalName) ? VIOLATED
                                                                                                                            : FIXED : NO_ACTION;
  }

  /**
   * @return The topics of partitions which are going to be modified by proposals.
   */
  public Set<String> topicsWithReplicationFactorChange() {
    Set<String> topics = new HashSet<>();
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

  /**
   * @return Proposal summary for the optimizer result in plaintext format.
   */
  public String getProposalSummary() {
    List<Number> moveStats = getMovementStats();
    String recommendation = _provisionResponse.recommendation();
    return String.format("%n%nOptimization has %d inter-broker replica(%d MB) moves, %d intra-broker replica(%d MB) moves"
                         + " and %d leadership moves with a cluster model of %d recent windows and %.3f%% of the partitions"
                         + " covered.%nExcluded Topics: %s.%nExcluded Brokers For Leadership: %s.%nExcluded Brokers For "
                         + "Replica Move: %s.%nCounts: %s%nOn-demand Balancedness Score Before (%.3f) After(%.3f).%nProvision Status: %s.%s",
                         moveStats.get(0).intValue(), moveStats.get(1).longValue(), moveStats.get(2).intValue(),
                         moveStats.get(3).longValue(), moveStats.get(4).intValue(), _clusterModelStats.numWindows(),
                         _clusterModelStats.monitoredPartitionsPercentage(), excludedTopics(),
                         excludedBrokersForLeadership(), excludedBrokersForReplicaMove(), _clusterModelStats.toStringCounts(),
                         _onDemandBalancednessScoreBefore, _onDemandBalancednessScoreAfter, _provisionResponse.status(),
                         recommendation.isEmpty() ? "" : String.format("%nProvision Recommendation: %s", recommendation));
  }

  /**
   * @return Proposal summary for the optimizer result in JSON format.
   */
  public Map<String, Object> getProposalSummaryForJson() {
    List<Number> moveStats = getMovementStats();
    Map<String, Object> ret = new HashMap<>();
    ret.put(NUM_INTER_BROKER_REPLICA_MOVEMENTS, moveStats.get(0).intValue());
    ret.put(INTER_BROKER_DATA_TO_MOVE_MB, moveStats.get(1).longValue());
    ret.put(NUM_INTRA_BROKER_REPLICA_MOVEMENTS, moveStats.get(2).intValue());
    ret.put(INTRA_BROKER_DATA_TO_MOVE_MB, moveStats.get(3).longValue());
    ret.put(NUM_LEADER_MOVEMENTS, moveStats.get(4).intValue());
    ret.put(RECENT_WINDOWS, _clusterModelStats.numWindows());
    ret.put(MONITORED_PARTITIONS_PERCENTAGE, _clusterModelStats.monitoredPartitionsPercentage());
    ret.put(EXCLUDED_TOPICS, excludedTopics());
    ret.put(EXCLUDED_BROKERS_FOR_LEADERSHIP, excludedBrokersForLeadership());
    ret.put(EXCLUDED_BROKERS_FOR_REPLICA_MOVE, excludedBrokersForReplicaMove());
    ret.put(ON_DEMAND_BALANCEDNESS_SCORE_BEFORE, _onDemandBalancednessScoreBefore);
    ret.put(ON_DEMAND_BALANCEDNESS_SCORE_AFTER, _onDemandBalancednessScoreAfter);
    ret.put(PROVISION_STATUS, _provisionResponse.status());
    ret.put(PROVISION_RECOMMENDATION, _provisionResponse.recommendation());
    return ret;
  }
}
