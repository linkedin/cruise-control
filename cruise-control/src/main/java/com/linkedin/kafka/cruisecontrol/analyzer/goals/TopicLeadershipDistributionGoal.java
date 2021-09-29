/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */
package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Rack;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;

/**
 * A hard goal that generates leadership movement and leader replica movement proposals to ensure that the number of
 * leader replicas on each non-excluded broker in the cluster is at most +1 for every topic (not every topic partition
 * count is perfectly divisible by every eligible broker count).
 *
 * This goal runs in two phases ({@link RebalancePhase#PER_RACK} and {@link RebalancePhase#PER_BROKER}). The
 * {@link RebalancePhase#PER_RACK} helps to guarantee that a valid solution can be found when operating in conjunction
 * with the {@link RackAwareGoal} or the {@link RackAwareDistributionGoal} in situations where brokers are evenly
 * distributed amongst racks in the cluster. If brokers are not more or less evenly distributed amongst racks in the
 * cluster, consider setting {@code topic.leadership.distribution.goal.skip.per.rack.phase=true} in
 * {@code cruisecontrol.properties}.
 *
 * This goal will throw an {@link OptimizationFailureException} if it gets stuck trying to find a solution. However, due
 * to the random nature of how it selects which balancing actions to attempt, it is possible that the goal isn't
 * actually impossible to satisfy but that it simply got itself stuck in a state in which it cannot find a valid next
 * move to enact.
 */
public class TopicLeadershipDistributionGoal extends AbstractGoal {
    private static final Logger LOG = LoggerFactory.getLogger(TopicLeadershipDistributionGoal.class);

    private static final Random RANDOM = new Random();

    private enum RebalancePhase { PER_RACK, PER_BROKER }
    private RebalancePhase _rebalancePhase;

    private static final String SKIP_PER_RACK_PHASE_CONFIG = "topic.leadership.distribution.goal.skip.per.rack.phase";
    private boolean _shouldSkipPerRackPhase = false;

    private Set<String> _allowedTopics;
    private Set<Broker> _allowedBrokers;

    private final Map<String, Integer> _targetNumLeadReplicasPerRackByTopic;
    private final Map<String, Map<String, Integer>> _numLeadReplicasByTopicByRackId;

    private final Map<String, Integer> _targetNumLeadReplicasPerBrokerByTopic;
    private final Map<String, Map<Integer, Integer>> _numLeadReplicasByTopicByBrokerId;

    private int _previousTotalDelta;

    public TopicLeadershipDistributionGoal() {
        super();

        _targetNumLeadReplicasPerRackByTopic = new HashMap<>();
        _numLeadReplicasByTopicByRackId = new HashMap<>();

        _targetNumLeadReplicasPerBrokerByTopic = new HashMap<>();
        _numLeadReplicasByTopicByBrokerId = new HashMap<>();
    }

    /**
     * Package private for unit test.
     */
    @SuppressWarnings("unused")
    TopicLeadershipDistributionGoal(BalancingConstraint balancingConstraint) {
        this();
        _balancingConstraint = balancingConstraint;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);

        String skipPerRackPhaseValue = (String) configs.get(SKIP_PER_RACK_PHASE_CONFIG);
        _shouldSkipPerRackPhase = "true".equals(skipPerRackPhaseValue);
    }

    @Override
    public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
        String topic = action.topic();

        if (!_allowedTopics.contains(topic)) {
            // If the topic in question is not an allowed topic, we're not attempting to balance it using this goal;
            // we'll give a blanket approval here.
            return ActionAcceptance.ACCEPT;
        }

        Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
        Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());

        Replica replica = sourceBroker.replica(action.topicPartition());

        if (replica == null) {
            // Source replica does not exist so this is definitely not a valid move.
            return ActionAcceptance.REPLICA_REJECT;
        }

        boolean isTopicBalanced = true;
        boolean isOtherTopicBalanced = true;

        ActionType actionType = action.balancingAction();

        if (actionType.equals(ActionType.LEADERSHIP_MOVEMENT)
                || actionType.equals(ActionType.INTER_BROKER_REPLICA_MOVEMENT)
                || actionType.equals(ActionType.INTER_BROKER_REPLICA_SWAP)) {
            LeadershipCounts counts = new LeadershipCounts(clusterModel, _allowedBrokers, topic);

            if (replica.isLeader()) {
                counts.decrementCount(sourceBroker);
                counts.incrementCount(destinationBroker);

                isTopicBalanced = counts.isBalancedByRack(_targetNumLeadReplicasPerRackByTopic.get(topic))
                        && counts.isBalancedByBroker(_targetNumLeadReplicasPerBrokerByTopic.get(topic));
            }

            if (actionType.equals(ActionType.INTER_BROKER_REPLICA_SWAP)) {
                String otherTopic = action.destinationTopic();
                Replica otherReplica = destinationBroker.replica(action.destinationTopicPartition());

                if (otherReplica == null) {
                    // Destination replica does not exist so this is definitely not a valid move.
                    return ActionAcceptance.REPLICA_REJECT;
                } else if (otherReplica.isLeader()) {
                    LeadershipCounts otherTopicCounts = new LeadershipCounts(clusterModel, _allowedBrokers, otherTopic);

                    otherTopicCounts.decrementCount(destinationBroker);
                    otherTopicCounts.incrementCount(sourceBroker);

                    isOtherTopicBalanced = otherTopicCounts.isBalancedByRack(_targetNumLeadReplicasPerRackByTopic.get(otherTopic))
                            && otherTopicCounts.isBalancedByBroker(_targetNumLeadReplicasPerBrokerByTopic.get(otherTopic));
                }
            }
        }

        return isTopicBalanced && isOtherTopicBalanced ? ActionAcceptance.ACCEPT : ActionAcceptance.REPLICA_REJECT;
    }

    /**
     * Summarizes leadership counts on a per-rack and per-broker basis.
     */
    private static final class LeadershipCounts {
        private final Set<Broker> _allowedBrokers;
        private final Set<Rack> _allowedRacks;

        private final Map<Rack, Integer> _countsByRack;
        private final Map<Broker, Integer> _countsByBroker;

        private LeadershipCounts(ClusterModel clusterModel, Set<Broker> allowedBrokers, String topic) {
            _allowedBrokers = allowedBrokers;
            _allowedRacks = _allowedBrokers.stream().map(Broker::rack).collect(Collectors.toSet());

            _countsByRack = clusterModel.getNumLeadReplicasByRack(topic);
            _countsByBroker = clusterModel.getNumLeadReplicasByBroker(topic);
        }

        private void incrementCount(Broker broker) {
            _countsByRack.compute(broker.rack(), (k, v) -> v == null ? 1 : v + 1);
            _countsByBroker.compute(broker, (k, v) -> v == null ? 1 : v + 1);
        }

        private void decrementCount(Broker broker) {
            _countsByRack.compute(broker.rack(), (k, v) -> {
                assert v != null;
                return v - 1;
            });
            _countsByBroker.compute(broker, (k, v) -> {
                assert v != null;
                return v - 1;
            });
        }

        private boolean isBalancedByRack(int target) {
            return isBalanced(_countsByRack, _allowedRacks, target);
        }

        private boolean isBalancedByBroker(int target) {
            return isBalanced(_countsByBroker, _allowedBrokers, target);
        }

        private static <T> boolean isBalanced(Map<T, Integer> counts, Set<T> overallKeys, int target) {
            for (T key : overallKeys) {
                int count = counts.getOrDefault(key, 0);

                if (count < target || count > target + 1) {
                    return false;
                }
            }

            return true;
        }
    }

    @Override
    public ClusterModelStatsComparator clusterModelStatsComparator() {
        return new GoalUtils.HardGoalStatsComparator();
    }

    @Override
    public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
        return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
    }

    @Override
    public String name() {
        return TopicLeadershipDistributionGoal.class.getSimpleName();
    }

    @Override
    public boolean isHardGoal() {
        return true;
    }

    /**
     * This goal will only submit valid candidates for its balancing proposals for whatever phase it's in, so we only
     * need to check that any previously-completed phases remain satisfied.
     *
     * @param clusterModel The state of the cluster.
     * @param action       Action containing information about potential modification to the given cluster model.
     * @return True if the balancing action is considered valid; false otherwise
     */
    protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
        switch (_rebalancePhase) {
            case PER_RACK:
                // There are no previous phases, so we can automatically return true.
                return true;
            case PER_BROKER:
                if (_shouldSkipPerRackPhase) {
                    // We skipped over the PER_RACK phase; we won't expect the topic to be balanced per rack.
                    return true;
                } else {
                    // The previous phase is the PER_RACK phase, so we check that the topic is still balanced per rack.
                    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
                    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());

                    return isTopicBalancedPerRack(
                            action.topic(),
                            sourceBroker.rack().id(),
                            destinationBroker.rack().id());
                }
            default:
                throw new IllegalStateException("Unknown rebalance phase: " + _rebalancePhase);
        }
    }

    @Override
    protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
            throws OptimizationFailureException {
        if (_shouldSkipPerRackPhase) {
            _rebalancePhase = RebalancePhase.PER_BROKER;
            LOG.debug("Skipping initial PER_RACK phase and entering PER_BROKER phase");
        } else {
            _rebalancePhase = RebalancePhase.PER_RACK;
            LOG.debug("Entering initial PER_RACK phase");
        }

        _allowedTopics = GoalUtils.topicsToRebalance(clusterModel, optimizationOptions.excludedTopics());

        if (_allowedTopics.isEmpty()) {
            LOG.warn("All topics are excluded from {}.", name());
        }

        Set<Integer> excludedBrokers = optimizationOptions.excludedBrokersForLeadership();
        _allowedBrokers = clusterModel.aliveBrokers().stream()
                .filter(b -> !excludedBrokers.contains(b.id()))
                .collect(Collectors.toCollection(HashSet::new));

        if (_allowedBrokers.isEmpty()) {
            logAndThrowOptimizationFailureException("Cannot take any action as all alive brokers are excluded from leadership.");
        }

        _targetNumLeadReplicasPerRackByTopic.clear();
        _numLeadReplicasByTopicByRackId.clear();
        _targetNumLeadReplicasPerBrokerByTopic.clear();
        _numLeadReplicasByTopicByBrokerId.clear();

        Set<Rack> racks = new HashSet<>();

        for (Broker broker : _allowedBrokers) {
            racks.add(broker.rack());
        }

        SortedMap<String, List<Partition>> partitionsByTopic = clusterModel.getPartitionsByTopic();

        for (String topic : _allowedTopics) {
            // Each partition has one leader so the # of leaders is the same as the # of partitions.
            int numLeadReplicas = partitionsByTopic.get(topic).size();

            int targetNumLeadReplicasPerRack = Math.floorDiv(numLeadReplicas, racks.size());
            _targetNumLeadReplicasPerRackByTopic.put(topic, targetNumLeadReplicasPerRack);

            int targetNumLeadReplicasPerBroker = Math.floorDiv(numLeadReplicas, _allowedBrokers.size());
            _targetNumLeadReplicasPerBrokerByTopic.put(topic, targetNumLeadReplicasPerBroker);

            Map<String, Integer> numLeadReplicasPerRack = new HashMap<>();
            Map<Integer, Integer> numLeadReplicasPerBroker = new HashMap<>();

            for (Partition partition : partitionsByTopic.get(topic)) {
                Broker partitionLeader = partition.leader().broker();

                numLeadReplicasPerRack.compute(partitionLeader.rack().id(), (k, v) -> v == null ? 1 : v + 1);
                numLeadReplicasPerBroker.compute(partitionLeader.id(), (k, v) -> v == null ? 1 : v + 1);
            }

            _numLeadReplicasByTopicByRackId.put(topic, numLeadReplicasPerRack);
            _numLeadReplicasByTopicByBrokerId.put(topic, numLeadReplicasPerBroker);

            LOG.trace("Targeting {}(+1) lead replicas per rack for topic {}", targetNumLeadReplicasPerRack, topic);
            LOG.trace("Targeting {}(+1) lead replicas per broker for topic {}", targetNumLeadReplicasPerBroker, topic);
        }

        _previousTotalDelta = Integer.MAX_VALUE;
    }

    /**
     * This method is responsible for transitioning this goal from {@link RebalancePhase#PER_RACK} to
     * {@link RebalancePhase#PER_BROKER} as well as determining when the goal is satisfied and calling
     * {@link #finish()} at the end.
     *
     * @param clusterModel        The state of the cluster.
     * @param optimizationOptions Options to take into account during optimization.
     */
    @Override
    protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
            throws OptimizationFailureException {
        ensureProgressBeingMade(clusterModel);

        switch (_rebalancePhase) {
            case PER_RACK:
                boolean isBalancedPerRack = true;

                for (String topic : _allowedTopics) {
                    if (!isTopicBalancedPerRack(topic)) {
                        isBalancedPerRack = false;
                    }
                }

                if (isBalancedPerRack) {
                    LOG.debug("Transitioning from PER_RACK phase to PER_BROKER phase");
                    _rebalancePhase = RebalancePhase.PER_BROKER;
                    _previousTotalDelta = Integer.MAX_VALUE;
                }
                break;
            case PER_BROKER:
                boolean isBalancedPerBroker = true;

                for (String topic : _allowedTopics) {
                    if (!isTopicBalancedPerBroker(topic)) {
                        isBalancedPerBroker = false;
                    }
                }

                if (isBalancedPerBroker) {
                    finish();
                    LOG.debug("Finished.");
                }
                break;
            default:
                throw new IllegalStateException("Unknown rebalance phase: " + _rebalancePhase);
        }
    }

    private void ensureProgressBeingMade(ClusterModel clusterModel) throws OptimizationFailureException {
        int totalDelta = 0;

        for (String topic : _allowedTopics) {
            switch (_rebalancePhase) {
                case PER_RACK:
                    int targetPerRack = _targetNumLeadReplicasPerRackByTopic.get(topic);

                    Set<String> rackIds = _allowedBrokers.stream()
                            .map(b -> b.rack().id())
                            .collect(Collectors.toSet());

                    for (String rackId : rackIds) {
                        int count = _numLeadReplicasByTopicByRackId.get(topic).getOrDefault(rackId, 0);
                        totalDelta += Math.abs(targetPerRack - count);
                    }
                    break;
                case PER_BROKER:
                    int targetPerBroker = _targetNumLeadReplicasPerBrokerByTopic.get(topic);

                    for (Broker broker : _allowedBrokers) {
                        int count = _numLeadReplicasByTopicByBrokerId.get(topic).getOrDefault(broker.id(), 0);
                        totalDelta += Math.abs(targetPerBroker - count);
                    }
                    break;
                default:
                    throw new IllegalStateException("Unrecognized rebalance phase: " + _rebalancePhase);
            }
        }

        if (totalDelta >= _previousTotalDelta) {
            StringBuilder s = new StringBuilder();

            for (String topic : _allowedTopics) {
                switch (_rebalancePhase) {
                    case PER_RACK:
                        if (!isTopicBalancedPerRack(topic)) {
                            s.append(prettyPrintedLeadershipDistributionByRack(clusterModel, topic));
                        }
                        break;
                    case PER_BROKER:
                        if (!isTopicBalancedPerBroker(topic)) {
                            s.append(prettyPrintedLeadershipDistributionByBroker(clusterModel, topic));
                        }
                        break;
                    default:
                        throw new IllegalStateException("Unrecognized rebalance phase: " + _rebalancePhase);
                }
            }

            logAndThrowOptimizationFailureException("Unable to solve for this goal; remaining imbalanced topics:\n\n" + s);
        }

        _previousTotalDelta = totalDelta;
    }

    @Override
    protected void rebalanceForBroker(
            Broker broker,
            ClusterModel clusterModel,
            Set<Goal> optimizedGoals,
            OptimizationOptions optimizationOptions) throws OptimizationFailureException {
        if (!_allowedBrokers.contains(broker)) {
            return;
        }

        for (String topic : _allowedTopics) {
            Collection<Replica> leaderReplicas = broker.replicasOfTopicInBroker(topic).stream()
                    .filter(Replica::isLeader)
                    .collect(Collectors.toSet());

            switch (_rebalancePhase) {
                case PER_RACK:
                    LOG.debug("Re-balancing for broker {} in rack {} and topic {}", broker.id(), broker.rack().id(), topic);

                    if (!isTopicBalancedPerRack(topic)) {
                        int numLeadReplicasInRack = _numLeadReplicasByTopicByRackId
                                .get(topic)
                                .getOrDefault(broker.rack().id(), 0);

                        int targetNumLeadReplicas = _targetNumLeadReplicasPerRackByTopic.get(topic);
                        int numLeadReplicasToFlip = numLeadReplicasInRack - targetNumLeadReplicas;

                        if (numLeadReplicasToFlip > 0) {
                            loseLeadReplicasForRack(
                                    numLeadReplicasToFlip,
                                    leaderReplicas,
                                    clusterModel,
                                    optimizedGoals,
                                    optimizationOptions);
                        }
                    }
                    break;
                case PER_BROKER:
                    LOG.debug("Re-balancing for broker {} and topic {}", broker.id(), topic);

                    if (!isTopicBalancedPerBroker(topic)) {
                        int numLeadReplicasToMove = leaderReplicas.size() - _targetNumLeadReplicasPerBrokerByTopic.get(topic);

                        if (numLeadReplicasToMove > 0) {
                            loseLeadReplicasForBroker(
                                    numLeadReplicasToMove,
                                    leaderReplicas,
                                    clusterModel,
                                    optimizedGoals,
                                    optimizationOptions);
                        }
                    }
                    break;
                default:
                    throw new IllegalStateException("Unexpected rebalance phase: " + _rebalancePhase);
            }
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean isTopicBalancedPerRack(String topic) {
        return isTopicBalancedPerRack(topic, null, null);
    }

    /**
     * This method works similarly to {@link #isTopicBalancedPerBroker(String topic)} but on a per-rack basis instead
     * of a per-broker basis.
     *
     * @param topic the topic to check
     * @param sourceRackId if considering a proposed action, this is the rack the lead replica is moving from (null if
     *                     not considering a proposed action)
     * @param destinationRackId if considering a proposed action, this is the rack the lead replica is moving to (null
     *                          if not considering a proposed action)
     *
     * @return true if topic is balanced per rack; false otherwise
     * @see #isTopicBalancedPerBroker(String)
     */
    private boolean isTopicBalancedPerRack(
            String topic,
            String sourceRackId,
            String destinationRackId) {
        Map<String, Integer> numLeadReplicasByRackId = _numLeadReplicasByTopicByRackId.get(topic);
        Integer target = _targetNumLeadReplicasPerRackByTopic.get(topic);

        Set<String> rackIds = _allowedBrokers.stream()
                .map(b -> b.rack().id())
                .collect(Collectors.toSet());

        for (String rackId : rackIds) {
            int numLeadReplicas = numLeadReplicasByRackId.getOrDefault(rackId, 0);

            if (rackId.equals(sourceRackId)) {
                numLeadReplicas--;
            }
            if (rackId.equals(destinationRackId)) {
                numLeadReplicas++;
            }

            if (numLeadReplicas < target || numLeadReplicas > target + 1) {
                return false;
            }
        }

        return true;
    }

    /**
     * Because we try to evacuate lead replicas from a broker even if its lead replica count is greater than the target
     * number at all, there could be cases where the leadership for the lead replica distribution is already as evenly
     * distributed as possible.
     *
     * This method detects those cases and allows us to exit {@link #rebalanceForBroker(Broker, ClusterModel, Set, OptimizationOptions)}
     * early for those topics if possible.
     *
     * @param topic topic whose leadership distribution balance should be checked
     * @return true if topic leadership distribution still needs to be balanced; false otherwise
     */
    private boolean isTopicBalancedPerBroker(String topic) {
        Map<Integer, Integer> numLeadReplicasByBrokerId = _numLeadReplicasByTopicByBrokerId.get(topic);
        int target = _targetNumLeadReplicasPerBrokerByTopic.get(topic);

        for (Broker broker : _allowedBrokers) {
            int numLeadReplicas = numLeadReplicasByBrokerId.getOrDefault(broker.id(), 0);

            if (numLeadReplicas < target || numLeadReplicas > target + 1) {
                return false;
            }
        }

        return true;
    }

    private <T> T selectRandomFrom(Collection<T> collection) {
        return collection.isEmpty()
                ? null
                : collection.stream().skip(RANDOM.nextInt(collection.size())).findFirst().orElse(null);
    }

    /**
     * This method seeks to lose {@code numLeadReplicasToMove} replicas from the specified {@code leaderReplicas}
     * candidates.
     *
     * @param numLeadReplicasToMove the number of lead replicas to move
     * @param leaderReplicas candidate replicas to move
     * @param clusterModel {@link ClusterModel}
     * @param optimizedGoals already-optimized {@link Goal}s
     * @param optimizationOptions {@link OptimizationOptions}
     */
    private void loseLeadReplicasForBroker(
            int numLeadReplicasToMove,
            Collection<Replica> leaderReplicas,
            ClusterModel clusterModel,
            Set<Goal> optimizedGoals,
            OptimizationOptions optimizationOptions) {
        int numMoves = 0;

        while (numMoves < numLeadReplicasToMove && !leaderReplicas.isEmpty()) {
            Replica leaderReplica = selectRandomFrom(leaderReplicas);

            if (leaderReplica != null) {
                String topic = leaderReplica.topicPartition().topic();

                // We separate candidate brokers into two separate tiers because we want to try to fill brokers that are
                // under the target # of lead replicas for a given topic before we begin assigning "extra" lead replicas
                // to brokers that are at the target #.
                //
                // In this context, "extra" lead replicas are the result of partition counts that do not divide evenly
                // into the number of available brokers.
                Set<Broker> primaryCandidateBrokers = getBrokersUnderCapacityForTopic(topic, clusterModel, false);
                Set<Broker> secondaryCandidateBrokers = getBrokersUnderCapacityForTopic(topic, clusterModel, true);

                List<Set<Broker>> tieredCandidateBrokers = new ArrayList<>();
                tieredCandidateBrokers.add(primaryCandidateBrokers);
                tieredCandidateBrokers.add(secondaryCandidateBrokers);

                for (Set<Broker> candidates : tieredCandidateBrokers) {
                    if (attemptRelinquishReplicaAction(
                            clusterModel,
                            optimizedGoals,
                            optimizationOptions,
                            leaderReplica,
                            candidates)) {
                        numMoves++;
                        break;
                    }
                }

                // Whether or not we're successful moving leadership of this replica, we don't want to attempt it
                // again.
                leaderReplicas.remove(leaderReplica);

            }
        }
    }

    /**
     * This method enacts changes on a per-broker level but seeks to fulfill a per-rack goal. If applicable, we seek to
     * lose replicas from a rack by evacuating it from a broker that resides in a rack that is above the topic's lead
     * replica count target to a broker that resides in a rack that is below the topic's lead replica count target.
     *
     * @param numLeadReplicasToFlip The number of lead replicas we need to shift for the topic to bring this rack to its
     *                              target count
     * @param leaderReplicas The lead replicas that actually reside on the broker we are currently examining
     * @param clusterModel {@link ClusterModel}
     * @param optimizedGoals The set of already-optimized {@link Goal}s
     * @param optimizationOptions {@link OptimizationOptions}
     */
    private void loseLeadReplicasForRack(
            int numLeadReplicasToFlip,
            Collection<Replica> leaderReplicas,
            ClusterModel clusterModel,
            Set<Goal> optimizedGoals,
            OptimizationOptions optimizationOptions) {
        int numMoves = 0;

        while (numMoves < numLeadReplicasToFlip && !leaderReplicas.isEmpty()) {
            Replica leaderReplica = selectRandomFrom(leaderReplicas);

            if (leaderReplica != null) {
                String topic = leaderReplica.topicPartition().topic();

                Map<String, Integer> numLeadReplicasByRackId = _numLeadReplicasByTopicByRackId.get(topic);
                int targetNumLeadReplicasPerRack = _targetNumLeadReplicasPerRackByTopic.get(topic);

                Set<Broker> primaryCandidates = new HashSet<>();
                Set<Broker> secondaryCandidates = new HashSet<>();

                for (Broker broker : _allowedBrokers) {
                    int numLeadReplicasInRack = numLeadReplicasByRackId.getOrDefault(broker.rack().id(), 0);

                    if (numLeadReplicasInRack < targetNumLeadReplicasPerRack) {
                        primaryCandidates.add(broker);
                    } else if (numLeadReplicasInRack == targetNumLeadReplicasPerRack) {
                        secondaryCandidates.add(broker);
                    }
                }

                List<Set<Broker>> tieredCandidateBrokers = new ArrayList<>();
                tieredCandidateBrokers.add(primaryCandidates);
                tieredCandidateBrokers.add(secondaryCandidates);

                for (Set<Broker> candidates : tieredCandidateBrokers) {
                    if (
                            attemptRelinquishLeadershipAction(
                                    clusterModel, optimizedGoals, optimizationOptions, leaderReplica, candidates)
                            || attemptRelinquishReplicaAction(
                                    clusterModel, optimizedGoals, optimizationOptions, leaderReplica, candidates)
                    ) {
                        numMoves++;
                        break;
                    }
                }

                // Whether we're successful moving leadership of this replica, we don't want to attempt it again.
                leaderReplicas.remove(leaderReplica);
            }
        }
    }

    /**
     * Return a set of brokers that are not leading enough partitions of a certain topic (brokers that are leading less
     * than {@link #_targetNumLeadReplicasPerBrokerByTopic} (optionally +1) lead replicas).
     *
     * Although we want to first fill brokers that are <strong>under</strong> the target number of lead replicas for
     * each topic, we also will allow an additional +1 for cases where the number of partitions of the topic do not
     * divide evenly into the number of allowable brokers. We control which case we're returning with the
     * {@code onTarget} parameter of this method.
     *
     * @param topic The topic to check replica counts for
     * @param clusterModel {@link ClusterModel}
     * @param onTarget True if we want to return brokers that are under the target for the topic or false if we want to
     *                 return brokers that are exactly at the target for the topic (these brokers can hold an additional
     *                 lead replica if necessary)
     * @return Set of brokers that could stand to gain more lead replicas of the specified topic
     */
    private Set<Broker> getBrokersUnderCapacityForTopic(String topic, ClusterModel clusterModel, boolean onTarget) {
        int target = _targetNumLeadReplicasPerBrokerByTopic.get(topic);
        Map<Integer, Integer> numLeadReplicasByBrokerId = _numLeadReplicasByTopicByBrokerId.get(topic);

        return clusterModel.aliveBrokers().stream()
                .filter(b -> _allowedBrokers.contains(b))
                .filter(b -> {
                    int numLeadReplicas = numLeadReplicasByBrokerId.getOrDefault(b.id(), 0);
                    return onTarget ? numLeadReplicas == target : numLeadReplicas < target;
                })
                .collect(Collectors.toSet());
    }

    private void updateLeadReplicaCounts(
            Replica replica,
            Broker sourceBroker,
            Broker destinationBroker) {
        String topic = replica.topicPartition().topic();

        Map<String, Integer> rackCountsToUpdate = _numLeadReplicasByTopicByRackId.get(topic);

        rackCountsToUpdate.compute(destinationBroker.rack().id(), (rackId, numLeadReplicas)
                -> numLeadReplicas == null ? 1 : numLeadReplicas + 1);
        rackCountsToUpdate.compute(sourceBroker.rack().id(), (rackId, numLeadReplicas) -> {
            assert numLeadReplicas != null;
            return numLeadReplicas - 1;
        });

        Map<Integer, Integer> brokerCountsToUpdate = _numLeadReplicasByTopicByBrokerId.get(topic);

        brokerCountsToUpdate.compute(destinationBroker.id(), (brokerId, numLeadReplicas)
                -> numLeadReplicas == null ? 1 : numLeadReplicas + 1);
        brokerCountsToUpdate.compute(sourceBroker.id(), (brokerId, numLeadReplicas) -> {
            assert numLeadReplicas != null;
            return numLeadReplicas - 1;
        });
    }

    /**
     * Attempt to lose a lead replica by swapping places with one of its follower replicas.
     *
     * @param clusterModel {@link ClusterModel}
     * @param optimizedGoals The set of already-optimized {@link Goal}s
     * @param optimizationOptions {@link OptimizationOptions}
     * @param leaderReplica The lead replica to swap out
     * @param candidateBrokers Set of candidate brokers to receive the lead replica
     * @return True if a successful balancing action was found; false otherwise
     */
    private boolean attemptRelinquishLeadershipAction(
            ClusterModel clusterModel,
            Set<Goal> optimizedGoals,
            OptimizationOptions optimizationOptions,
            Replica leaderReplica,
            Set<Broker> candidateBrokers) {
        Broker sourceBroker = leaderReplica.broker();
        Broker destinationBroker = maybeApplyBalancingAction(
                clusterModel,
                leaderReplica,
                candidateBrokers,
                ActionType.LEADERSHIP_MOVEMENT,
                optimizedGoals,
                optimizationOptions);

        if (destinationBroker != null) {
            updateLeadReplicaCounts(leaderReplica, sourceBroker, destinationBroker);
            LOG.trace("Lead replica of {} ({}) moved from {} to {} via {}",
                    leaderReplica.topicPartition().toString(),
                    getReplicaSetString(clusterModel, leaderReplica),
                    sourceBroker.id(),
                    destinationBroker.id(),
                    ActionType.LEADERSHIP_MOVEMENT.balancingAction());
            return true;
        }

        return false;
    }

    /**
     * Attempt to lose a lead replica by moving it onto another broker.
     *
     * @param clusterModel {@link ClusterModel}
     * @param optimizedGoals The set of already-optimized {@link Goal}s
     * @param optimizationOptions {@link OptimizationOptions}
     * @param replica The lead replica to be moved
     * @param candidateBrokers Set of candidate brokers to receive the lead replica
     * @return True if a successful balancing action was found; false otherwise
     */
    private boolean attemptRelinquishReplicaAction(
            ClusterModel clusterModel,
            Set<Goal> optimizedGoals,
            OptimizationOptions optimizationOptions,
            Replica replica,
            Set<Broker> candidateBrokers) {
        Broker sourceBroker = replica.broker();
        Broker destinationBroker = maybeApplyBalancingAction(
                clusterModel,
                replica,
                candidateBrokers,
                ActionType.INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions);

        if (destinationBroker != null) {
            updateLeadReplicaCounts(replica, sourceBroker, destinationBroker);
            LOG.trace("Lead replica of {} ({}) moved from {} to {} via {}",
                    replica.topicPartition().toString(),
                    getReplicaSetString(clusterModel, replica),
                    sourceBroker.id(),
                    destinationBroker.id(),
                    ActionType.INTER_BROKER_REPLICA_MOVEMENT.balancingAction());
            return true;
        }

        return false;
    }

    private String prettyPrintedLeadershipDistributionByBroker(ClusterModel clusterModel, String topic) {
        Map<Integer, Integer> leaderCountsByBrokerId = new HashMap<>();

        for (Partition p : clusterModel.getPartitionsByTopic().get(topic)) {
            leaderCountsByBrokerId.compute(
                    p.leader().broker().id(),
                    (brokerId, numLeadReplicas) -> numLeadReplicas == null ? 1 : numLeadReplicas + 1);
        }

        List<Integer> brokerIds = clusterModel.aliveBrokers().stream()
                .map(Broker::id)
                .sorted(Integer::compareTo)
                .collect(Collectors.toList());

        StringBuilder s = new StringBuilder();

        s.append(String.format("Leadership distribution by broker for %s:%n", topic));

        for (Integer brokerId : brokerIds) {
            s.append(String.format("\t%s: %s%n", brokerId, leaderCountsByBrokerId.get(brokerId)));
        }

        return s.toString();
    }

    private String prettyPrintedLeadershipDistributionByRack(ClusterModel clusterModel, String topic) {
        Map<String, Integer> numLeadReplicasByRack = new HashMap<>();

        for (Partition partition : clusterModel.getPartitionsByTopic().get(topic)) {
            Rack rack = partition.leader().broker().rack();

            if (!numLeadReplicasByRack.containsKey(rack.id())) {
                numLeadReplicasByRack.put(rack.id(), 0);
            }
            numLeadReplicasByRack.put(rack.id(), numLeadReplicasByRack.get(rack.id()) + 1);
        }

        List<String> orderedRackIds = numLeadReplicasByRack.keySet().stream()
                .sorted()
                .collect(Collectors.toList());

        StringBuilder s = new StringBuilder();

        s.append(String.format("Leadership distribution by rack for %s:%n", topic));

        for (String rackId : orderedRackIds) {
            s.append(String.format("\t%s: %s%n", rackId, numLeadReplicasByRack.get(rackId)));
        }

        return s.toString();
    }

    private String getReplicaSetString(ClusterModel clusterModel, Replica replica) {
        Partition partition = clusterModel.partition(replica.topicPartition());

        Broker leader = partition.leader().broker();
        List<Broker> followers = partition.followerBrokers();

        return leader.id() + " [" + leader.rack().id() + "],"
                + followers.stream().map(f -> f.id() + " [" + f.rack().id() + "]")
                .collect(Collectors.joining(","));
    }

    private void logAndThrowOptimizationFailureException(final String message) throws OptimizationFailureException {
        LOG.error(message);
        throw new OptimizationFailureException(message);
    }
}
