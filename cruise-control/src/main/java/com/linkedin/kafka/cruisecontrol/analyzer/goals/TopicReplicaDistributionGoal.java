/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils.EPSILON;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal.ChangeType.ADD;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal.ChangeType.REMOVE;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;


/**
 * SOFT GOAL: Balance collocations of replicas of the same topic.
 * <ul>
 * <li>Under: (the average number of topic replicas per broker) * (1 + topic replica count balance percentage)</li>
 * <li>Above: (the average number of topic replicas per broker) * Math.max(0, 1 - topic replica count balance percentage)</li>
 * </ul>
 * Also see: {@link com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig#TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG},
 *  * {@link com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig#GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG},
 * and {@link #balancePercentageWithMargin(OptimizationOptions)}.
 */
public class TopicReplicaDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(TopicReplicaDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  // Flag to indicate whether the self healing failed to relocate all replicas away from dead brokers in its initial
  // attempt and currently omitting the topic replica balance limit to relocate remaining replicas.
  private boolean _selfHealingDeadBrokersOnly;

  private final Map<String, Set<Integer>> _brokerIdsAboveBalanceUpperLimitByTopic;
  private final Map<String, Set<Integer>> _brokerIdsUnderBalanceLowerLimitByTopic;
  private final Map<String, Double> _avgTopicReplicasOnAliveBroker;
  private final Map<String, Integer> _balanceUpperLimitByTopic;
  private final Map<String, Integer> _balanceLowerLimitByTopic;

  /**
   * Constructor for Replica Distribution Goal.
   */
  public TopicReplicaDistributionGoal() {
    _brokerIdsAboveBalanceUpperLimitByTopic = new HashMap<>();
    _brokerIdsUnderBalanceLowerLimitByTopic = new HashMap<>();
    _avgTopicReplicasOnAliveBroker = new HashMap<>();
    _balanceUpperLimitByTopic = new HashMap<>();
    _balanceLowerLimitByTopic = new HashMap<>();
  }

  public TopicReplicaDistributionGoal(BalancingConstraint balancingConstraint) {
    this();
    _balancingConstraint = balancingConstraint;
  }

  /**
   * To avoid churns, we add a balance margin to the user specified rebalance threshold. e.g. when user sets the
   * threshold to be {@link BalancingConstraint#topicReplicaBalancePercentage()}, we use
   * ({@link BalancingConstraint#topicReplicaBalancePercentage()}-1)*{@link #BALANCE_MARGIN} instead.
   *
   * @param optimizationOptions Options to adjust balance percentage with margin in case goal optimization is triggered
   * by goal violation detector.
   * @return the rebalance threshold with a margin.
   */
  private double balancePercentageWithMargin(OptimizationOptions optimizationOptions) {
    double balancePercentage = optimizationOptions.isTriggeredByGoalViolation()
                               ? _balancingConstraint.topicReplicaBalancePercentage()
                                 * _balancingConstraint.goalViolationDistributionThresholdMultiplier()
                               : _balancingConstraint.topicReplicaBalancePercentage();

    return (balancePercentage - 1) * BALANCE_MARGIN;
  }

  /**
   * @param topic Topic for which the upper limit is requested.
   * @param optimizationOptions Options to adjust balance upper limit in case goal optimization is triggered by goal
   * violation detector.
   * @return The topic replica balance upper threshold in number of topic replicas.
   */
  private int balanceUpperLimit(String topic, OptimizationOptions optimizationOptions) {
    return (int) Math.ceil(_avgTopicReplicasOnAliveBroker.get(topic)
                           * (1 + balancePercentageWithMargin(optimizationOptions)));
  }

  /**
   * @param topic Topic for which the lower limit is requested.
   * @param optimizationOptions Options to adjust balance lower limit in case goal optimization is triggered by goal
   * violation detector.
   * @return The replica balance lower threshold in number of topic replicas.
   */
  private int balanceLowerLimit(String topic, OptimizationOptions optimizationOptions) {
    return (int) Math.floor(_avgTopicReplicasOnAliveBroker.get(topic)
                            * Math.max(0, (1 - balancePercentageWithMargin(optimizationOptions))));
  }

  /**
   * Check whether the given action is acceptable by this goal. An action is acceptable if the number of topic replicas at
   * (1) the source broker does not go under the allowed limit.
   * (2) the destination broker does not go over the allowed limit.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    String sourceTopic = action.topic();

    switch (action.balancingAction()) {
      case INTER_BROKER_REPLICA_SWAP:
        String destinationTopic = action.destinationTopic();
        if (sourceTopic.equals(destinationTopic)) {
          return ACCEPT;
        }

        boolean acceptSourceToDest = (isReplicaCountUnderBalanceUpperLimitAfterChange(sourceTopic, destinationBroker, ADD)
                                      && isReplicaCountAboveBalanceLowerLimitAfterChange(sourceTopic, sourceBroker, REMOVE));
        return (acceptSourceToDest
                && isReplicaCountUnderBalanceUpperLimitAfterChange(destinationTopic, sourceBroker, ADD)
                && isReplicaCountAboveBalanceLowerLimitAfterChange(destinationTopic, destinationBroker, REMOVE)) ? ACCEPT
                                                                                                                 : REPLICA_REJECT;
      case LEADERSHIP_MOVEMENT:
        return ACCEPT;
      case INTER_BROKER_REPLICA_MOVEMENT:
        return (isReplicaCountUnderBalanceUpperLimitAfterChange(sourceTopic, destinationBroker, ADD)
                && isReplicaCountAboveBalanceLowerLimitAfterChange(sourceTopic, sourceBroker, REMOVE)) ? ACCEPT : REPLICA_REJECT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  private boolean isReplicaCountUnderBalanceUpperLimitAfterChange(String topic,
                                                                  Broker broker,
                                                                  ReplicaDistributionGoal.ChangeType changeType) {
    int numTopicReplicas = broker.numReplicasOfTopicInBroker(topic);
    int brokerBalanceUpperLimit = broker.isAlive() ? _balanceUpperLimitByTopic.get(topic) : 0;

    return changeType == ADD ? numTopicReplicas + 1 <= brokerBalanceUpperLimit : numTopicReplicas - 1 <= brokerBalanceUpperLimit;
  }

  private boolean isReplicaCountAboveBalanceLowerLimitAfterChange(String topic,
                                                                  Broker broker,
                                                                  ReplicaDistributionGoal.ChangeType changeType) {
    int numTopicReplicas = broker.numReplicasOfTopicInBroker(topic);
    int brokerBalanceLowerLimit = broker.isAlive() ? _balanceLowerLimitByTopic.get(topic) : 0;

    return changeType == ADD ? numTopicReplicas + 1 >= brokerBalanceLowerLimit : numTopicReplicas - 1 >= brokerBalanceLowerLimit;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new TopicReplicaDistrGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
  }

  /**
   * Get the name of this goal. Name of a goal provides an identification for the goal in human readable format.
   */
  @Override
  public String name() {
    return TopicReplicaDistributionGoal.class.getSimpleName();
  }

  @Override
  public boolean isHardGoal() {
    return false;
  }

  /**
   * Get brokers that the rebalance process will go over to apply balancing actions to replicas they contain.
   *
   * @param clusterModel The state of the cluster.
   * @return A collection of brokers that the rebalance process will go over to apply balancing actions to replicas
   * they contain.
   */
  @Override
  protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
    return clusterModel.brokers();
  }

  /**
   * Get the set of topics to rebalance. If there are self healing eligible replicas, gets only their topics.
   * Otherwise gets all topics except excludedTopics.
   *
   * @return The set of topics to rebalance.
   */
  private Set<String> topicsToRebalance(ClusterModel clusterModel, Set<String> excludedTopics) {
    Set<String> topicsToRebalance;
    if (!clusterModel.selfHealingEligibleReplicas().isEmpty()) {
      topicsToRebalance = new HashSet<>();
      for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
        topicsToRebalance.add(replica.topicPartition().topic());
      }
    } else {
      topicsToRebalance = new HashSet<>(clusterModel.topics());
      topicsToRebalance.removeAll(excludedTopics);
    }

    if (topicsToRebalance.isEmpty()) {
      LOG.warn("All topics are excluded from {}.", name());
    }

    return topicsToRebalance;
  }

  /**
   * Initiates this goal.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    Set<String> topicsToRebalance = topicsToRebalance(clusterModel, optimizationOptions.excludedTopics());

    // Initialize the average replicas on an alive broker.
    for (String topic : topicsToRebalance) {
      int numTopicReplicas = clusterModel.numTopicReplicas(topic);
      _avgTopicReplicasOnAliveBroker.put(topic, (numTopicReplicas / (double) clusterModel.aliveBrokers().size()));
      _balanceUpperLimitByTopic.put(topic, balanceUpperLimit(topic, optimizationOptions));
      _balanceLowerLimitByTopic.put(topic, balanceLowerLimit(topic, optimizationOptions));
    }

    _selfHealingDeadBrokersOnly = false;
  }

  /**
   * Check if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model. Assumed to be
   * of type {@link ActionType#INTER_BROKER_REPLICA_MOVEMENT}.
   * @return True if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    // If the source broker is dead and currently self healing dead brokers only, then the proposal must be executed.
    if (!sourceBroker.isAlive() && _selfHealingDeadBrokersOnly) {
      return true;
    }

    //Check that destination and source would not become unbalanced.
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    String sourceTopic = action.topic();

    return isReplicaCountUnderBalanceUpperLimitAfterChange(sourceTopic, destinationBroker, ADD) &&
           isReplicaCountAboveBalanceLowerLimitAfterChange(sourceTopic, sourceBroker, REMOVE);
  }

  /**
   * Update goal state after one round of self-healing / rebalance.
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposal.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws OptimizationFailureException {
    if (!_brokerIdsAboveBalanceUpperLimitByTopic.isEmpty()) {
      _brokerIdsAboveBalanceUpperLimitByTopic.clear();
      _succeeded = false;
    }
    if (!_brokerIdsUnderBalanceLowerLimitByTopic.isEmpty()) {
      _brokerIdsUnderBalanceLowerLimitByTopic.clear();
      _succeeded = false;
    }
    // Sanity check: No self-healing eligible replica should remain at a decommissioned broker.
    try {
      GoalUtils.ensureNoReplicaOnDeadBrokers(clusterModel, name());
    } catch (OptimizationFailureException ofe) {
      if (_selfHealingDeadBrokersOnly) {
        throw ofe;
      }
      _selfHealingDeadBrokersOnly = true;
      LOG.warn("Ignoring topic replica balance limit to move replicas from dead brokers to healthy ones.");
      return;
    }

    finish();
  }

  private static boolean skipBrokerRebalance(Broker broker,
                                             ClusterModel clusterModel,
                                             Collection<Replica> replicas,
                                             boolean requireLessReplicas,
                                             boolean requireMoreReplicas) {
    if (broker.isAlive() && !requireMoreReplicas && !requireLessReplicas) {
      LOG.trace("Skip rebalance: Broker {} is already within the limit for replicas {}.", broker, replicas);
      return true;
    } else if (!clusterModel.newBrokers().isEmpty() && requireMoreReplicas && !broker.isNew()) {
      LOG.trace("Skip rebalance: Cluster has new brokers and this broker {} is not new, but requires more load for "
                + "replicas {}.", broker, replicas);
      return true;
    } else if (!clusterModel.deadBrokers().isEmpty() && requireLessReplicas && broker.isAlive()
               && replicas.stream().noneMatch(replica -> broker.immigrantReplicas().contains(replica))) {
      LOG.trace("Skip rebalance: Cluster is in self-healing mode and the broker {} requires less load, but none of its "
                + "immigrant topic replicas are from the topic being balanced {}.", broker, replicas);
      return true;
    }

    return false;
  }

  /**
   * Rebalance the given broker without violating the constraints of the current goal and optimized goals.
   *
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param optimizationOptions Options to take into account during optimization -- e.g. excluded topics.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) {
    LOG.debug("Rebalancing broker {} [limits] lower: {} upper: {}.", broker.id(), _balanceLowerLimitByTopic,
              _balanceUpperLimitByTopic);

    for (String topic : broker.topics()) {
      if (_avgTopicReplicasOnAliveBroker.get(topic) == null) {
        // Topic is excluded from the rebalance.
        continue;
      }

      Collection<Replica> replicas = broker.replicasOfTopicInBroker(topic);
      int numTopicReplicas = replicas.size();
      boolean requireLessReplicas = broker.isAlive() ? numTopicReplicas > _balanceUpperLimitByTopic.get(topic) : numTopicReplicas > 0;
      boolean requireMoreReplicas = broker.isAlive() && numTopicReplicas < _balanceLowerLimitByTopic.get(topic);

      if (skipBrokerRebalance(broker, clusterModel, replicas, requireLessReplicas, requireMoreReplicas)) {
        continue;
      }

      // Update broker ids over the balance limit for logging purposes.
      if (requireLessReplicas && rebalanceByMovingReplicasOut(broker, topic, clusterModel, optimizedGoals, optimizationOptions)) {
        _brokerIdsAboveBalanceUpperLimitByTopic.computeIfAbsent(topic, t -> new HashSet<>()).add(broker.id());
        LOG.debug("Failed to sufficiently decrease replicas of topic {} in broker {} with replica movements. Replicas: {}.",
                  topic, broker.id(), broker.numReplicasOfTopicInBroker(topic));
      } else if (requireMoreReplicas && rebalanceByMovingReplicasIn(broker, topic, clusterModel, optimizedGoals, optimizationOptions)) {
        _brokerIdsUnderBalanceLowerLimitByTopic.computeIfAbsent(topic, t -> new HashSet<>()).add(broker.id());
        LOG.debug("Failed to sufficiently increase replicas of topic {} in broker {} with replica movements. Replicas: {}.",
                  topic, broker.id(), broker.numReplicasOfTopicInBroker(topic));
      } else {
        LOG.debug("Successfully balanced replicas of topic {} in broker {} by moving replicas. Replicas: {}",
                  topic, broker.id(), broker.numReplicasOfTopicInBroker(topic));
      }
    }
  }

  private static SortedSet<Replica> replicasToMoveOut(ClusterModel clusterModel, Broker broker, String topic) {
    SortedSet<Replica> replicasToMoveOut = new TreeSet<>(broker.replicasOfTopicInBroker(topic));
    // Cluster has offline replicas, but this overloaded broker is alive -- we can move out only the immigrant replicas.
    if (!clusterModel.deadBrokers().isEmpty() && broker.isAlive()) {
      // Return only the immigrant replicas.
      replicasToMoveOut.retainAll(broker.immigrantReplicas());
    }

    return replicasToMoveOut;
  }

  private boolean rebalanceByMovingReplicasOut(Broker broker,
                                               String topic,
                                               ClusterModel clusterModel,
                                               Set<Goal> optimizedGoals,
                                               OptimizationOptions optimizationOptions) {
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    // Get the eligible brokers.
    SortedSet<Broker> candidateBrokers = new TreeSet<>(
        Comparator.comparingInt((Broker b) -> b.numReplicasOfTopicInBroker(topic)).thenComparingInt(Broker::id));

    candidateBrokers.addAll(_selfHealingDeadBrokersOnly ? clusterModel.aliveBrokers() : clusterModel
        .aliveBrokers()
        .stream()
        .filter(b -> b.numReplicasOfTopicInBroker(topic) < _balanceUpperLimitByTopic.get(topic))
        .collect(Collectors.toSet()));

    int numReplicasOfTopicInBroker = broker.numReplicasOfTopicInBroker(topic);

    for (Replica replica : replicasToMoveOut(clusterModel, broker, topic)) {
      if (shouldExclude(replica, excludedTopics)) {
        continue;
      }

      Broker b = maybeApplyBalancingAction(clusterModel, replica, candidateBrokers, ActionType.INTER_BROKER_REPLICA_MOVEMENT,
                                           optimizedGoals, optimizationOptions);
      // Only check if we successfully moved something.
      if (b != null) {
        if (--numReplicasOfTopicInBroker <= (broker.isAlive() ? _balanceUpperLimitByTopic.get(topic) : 0)) {
          return false;
        }
        // Remove and reinsert the broker so the order is correct.
        candidateBrokers.remove(b);
        if (b.numReplicasOfTopicInBroker(topic) < _balanceUpperLimitByTopic.get(topic) || _selfHealingDeadBrokersOnly) {
          candidateBrokers.add(b);
        }
      }
    }

    // All the topic replicas has been moved away from the broker.
    return !broker.replicasOfTopicInBroker(topic).isEmpty();
  }

  private boolean rebalanceByMovingReplicasIn(Broker broker,
                                              String topic,
                                              ClusterModel clusterModel,
                                              Set<Goal> optimizedGoals,
                                              OptimizationOptions optimizationOptions) {
    Set<String> excludedTopics = optimizationOptions.excludedTopics();

    PriorityQueue<Broker> eligibleBrokers = new PriorityQueue<>((b1, b2) -> {
      int result = Integer.compare(b2.numReplicasOfTopicInBroker(topic), b1.numReplicasOfTopicInBroker(topic));
      return result == 0 ? Integer.compare(b1.id(), b2.id()) : result;
    });

    for (Broker aliveBroker : clusterModel.aliveBrokers()) {
      if (aliveBroker.numReplicasOfTopicInBroker(topic) > _balanceLowerLimitByTopic.get(topic)) {
        eligibleBrokers.add(aliveBroker);
      }
    }

    Set<Broker> candidateBrokers = Collections.singleton(broker);
    int numReplicasOfTopicInBroker = broker.numReplicasOfTopicInBroker(topic);

    // Stop when no topic replicas can be moved in anymore.
    while (!eligibleBrokers.isEmpty()) {
      Broker sourceBroker = eligibleBrokers.poll();
      SortedSet<Replica> replicasToMove = replicasToMoveOut(clusterModel, sourceBroker, topic);

      for (Replica replica : replicasToMove) {
        if (shouldExclude(replica, excludedTopics)) {
          continue;
        }
        Broker b = maybeApplyBalancingAction(clusterModel, replica, candidateBrokers, ActionType.INTER_BROKER_REPLICA_MOVEMENT,
                                             optimizedGoals, optimizationOptions);
        // Only need to check status if the action is taken. This will also handle the case that the source broker
        // has nothing to move in. In that case we will never reenqueue that source broker.
        if (b != null) {
          if (++numReplicasOfTopicInBroker >= (broker.isAlive() ? _balanceLowerLimitByTopic.get(topic) : 0)) {
            return false;
          }
          // If the source broker has a lower number of replicas than the next broker in the eligible broker in the
          // queue, we reenqueue the source broker and switch to the next broker.
          if (!eligibleBrokers.isEmpty()
              && sourceBroker.numReplicasOfTopicInBroker(topic) < eligibleBrokers.peek().numReplicasOfTopicInBroker(topic)) {
            eligibleBrokers.add(sourceBroker);
            break;
          }
        }
      }
    }
    return true;
  }

  private class TopicReplicaDistrGoalStatsComparator implements ClusterModelStatsComparator {
    private String _reasonForLastNegativeResult;

    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // Standard deviation of number of topic replicas over brokers in the current must be less than the
      // pre-optimized stats.
      double stdDev1 = stats1.topicReplicaStats().get(Statistic.ST_DEV).doubleValue();
      double stdDev2 = stats2.topicReplicaStats().get(Statistic.ST_DEV).doubleValue();
      int result = AnalyzerUtils.compare(stdDev2, stdDev1, EPSILON);
      if (result < 0) {
        _reasonForLastNegativeResult = String.format("Violated %s. [Std Deviation of Topic Replica Distribution] post-"
                                                     + "optimization:%.3f pre-optimization:%.3f", name(), stdDev1, stdDev2);
      }
      return result;
    }

    @Override
    public String explainLastComparison() {
      return _reasonForLastNegativeResult;
    }
  }
}
