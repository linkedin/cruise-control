/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicConfigurationParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicReplicationFactorChangeParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_SKIP_RACK_AWARENESS_CHECK;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_CONCURRENT_MOVEMENTS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.populateRackInfoForReplicationFactorChange;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.partitionWithOfflineReplicas;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.topicsForReplicationFactorChange;


/**
 * The async runnable for updating configuration of topics, which match topic patterns.
 * Currently only supports changing topic's replication factor.
 *
 * If partition's current replication factor is less than target replication factor, new replicas are added to the partition
 * in two steps.
 * <ol>
 *   <li>
 *    Tentatively add new replicas in a rack-aware, round-robin way.
 *    There are two scenarios that rack awareness property is not guaranteed.
 *    <ul>
 *      <li> If metadata does not have rack information about brokers, then it is only guaranteed that new replicas are
 *      added to brokers, which currently do not host any replicas of partition.</li>
 *      <li> If replication factor to set for the topic is larger than number of racks in the cluster and
 *      skipTopicRackAwarenessCheck is set to true, then rack awareness property is ignored.</li>
 *    </ul>
 *   </li>
 *   <li>
 *     Further optimize location of new replica with provided {@link Goal} list.
 *   </li>
 * </ol>
 *
 * If the current replication factor of partition is larger than target replication factor, remove one or more follower
 * replicas from the partition. Replicas are removed following the reverse order of position in replica list of partition.
 */
public class UpdateTopicConfigurationRunnable extends GoalBasedOperationRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateTopicConfigurationRunnable.class);
  protected final String _uuid;
  protected final Map<Short, Pattern> _topicPatternByReplicationFactor;
  protected final boolean _skipRackAwarenessCheck;
  protected final Integer _concurrentInterBrokerPartitionMovements;
  protected final Integer _concurrentLeaderMovements;
  protected final Long _executionProgressCheckIntervalMs;
  protected final ReplicaMovementStrategy _replicaMovementStrategy;
  protected final Long _replicationThrottle;
  protected final String _reason;
  protected final boolean _isTriggeredByUserRequest;
  protected Cluster _cluster;
  protected Map<Short, Set<String>> _topicsToChangeByReplicationFactor;

  public UpdateTopicConfigurationRunnable(KafkaCruiseControl kafkaCruiseControl,
                                          OperationFuture future,
                                          String uuid,
                                          TopicConfigurationParameters parameters) {
    super(kafkaCruiseControl, future, parameters, parameters.dryRun(), parameters.stopOngoingExecution(),
          parameters.topicReplicationFactorChangeParameters() != null
          && parameters.topicReplicationFactorChangeParameters().skipHardGoalCheck());
    // Initialize common parameters
    _reason = parameters.reason();
    _uuid = uuid;
    _isTriggeredByUserRequest = true;
    _cluster = null;
    _topicsToChangeByReplicationFactor = null;

    // Initialize parameters specific to replication factor changes.
    TopicReplicationFactorChangeParameters topicReplicationFactorChangeParameters = parameters.topicReplicationFactorChangeParameters();
    if (topicReplicationFactorChangeParameters != null) {
      _topicPatternByReplicationFactor = topicReplicationFactorChangeParameters.topicPatternByReplicationFactor();
      _skipRackAwarenessCheck = topicReplicationFactorChangeParameters.skipRackAwarenessCheck();
      _concurrentInterBrokerPartitionMovements = topicReplicationFactorChangeParameters.concurrentInterBrokerPartitionMovements();
      _concurrentLeaderMovements = topicReplicationFactorChangeParameters.concurrentLeaderMovements();
      _executionProgressCheckIntervalMs = topicReplicationFactorChangeParameters.executionProgressCheckIntervalMs();
      _replicaMovementStrategy = topicReplicationFactorChangeParameters.replicaMovementStrategy();
      _replicationThrottle = topicReplicationFactorChangeParameters.replicationThrottle();
      } else {
      _topicPatternByReplicationFactor = null;
      _skipRackAwarenessCheck = false;
      _concurrentInterBrokerPartitionMovements = null;
      _concurrentLeaderMovements = null;
      _executionProgressCheckIntervalMs = null;
      _replicaMovementStrategy = null;
      _replicationThrottle = null;
    }
  }

  /**
   * Constructor to be used for creating a runnable for self-healing topic replication factor anomalies
   * (See {@link com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomaly}).
   */
  public UpdateTopicConfigurationRunnable(KafkaCruiseControl kafkaCruiseControl,
                                          Map<Short, Pattern> topicPatternByReplicationFactor,
                                          List<String> selfHealingGoals,
                                          boolean allowCapacityEstimation,
                                          boolean excludeRecentlyDemotedBrokers,
                                          boolean excludeRecentlyRemovedBrokers,
                                          String anomalyId,
                                          Supplier<String> reasonSupplier) {
    super(kafkaCruiseControl, new OperationFuture("Topic replication factor anomaly self-healing."), selfHealingGoals,
          allowCapacityEstimation, excludeRecentlyDemotedBrokers, excludeRecentlyRemovedBrokers);
    // Initialize common parameters
    _reason = reasonSupplier.get();
    _uuid = anomalyId;
    _isTriggeredByUserRequest = false;
    _cluster = null;

    // Initialize parameters specific to replication factor changes.
    _topicPatternByReplicationFactor = topicPatternByReplicationFactor;
    _skipRackAwarenessCheck = SELF_HEALING_SKIP_RACK_AWARENESS_CHECK;
    _concurrentInterBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _concurrentLeaderMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _executionProgressCheckIntervalMs = SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
    _replicaMovementStrategy = SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
    _replicationThrottle = kafkaCruiseControl.config().getLong(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG);
  }

  @Override
  public OptimizationResult getResult() throws Exception {
    if (_topicPatternByReplicationFactor != null) {
      return new OptimizationResult(computeResult(), _kafkaCruiseControl.config());
    }
    // Never reaches here.
    throw new IllegalArgumentException("Nothing executable found in request.");
  }

  @Override
  protected void init() {
    super.init();
    _cluster = _kafkaCruiseControl.kafkaCluster();
    // Ensure there is no offline replica in the cluster.
    PartitionInfo partitionInfo = partitionWithOfflineReplicas(_cluster);
    if (partitionInfo != null) {
      throw new IllegalStateException(String.format("Topic partition %s-%d has offline replicas on brokers %s.",
                                                    partitionInfo.topic(), partitionInfo.partition(),
                                                    Arrays.stream(partitionInfo.offlineReplicas()).mapToInt(Node::id)
                                                          .boxed().collect(Collectors.toSet())));
    }
    _topicsToChangeByReplicationFactor = topicsForReplicationFactorChange(_topicPatternByReplicationFactor, _cluster);
  }

  @Override
  protected OptimizerResult workWithClusterModel() throws KafkaCruiseControlException, TimeoutException, NotEnoughValidWindowsException {
    Map<String, List<Integer>> brokersByRack = new HashMap<>();
    Map<Integer, String> rackByBroker = new HashMap<>();
    ExecutorState executorState = _kafkaCruiseControl.executorState();
    Set<Integer> excludedBrokersForLeadership = _excludeRecentlyDemotedBrokers ? executorState.recentlyDemotedBrokers()
                                                                               : Collections.emptySet();
    Set<Integer> excludedBrokersForReplicaMove = _excludeRecentlyRemovedBrokers ? executorState.recentlyRemovedBrokers()
                                                                                : Collections.emptySet();
    populateRackInfoForReplicationFactorChange(_topicsToChangeByReplicationFactor, _cluster, excludedBrokersForReplicaMove,
                                               _skipRackAwarenessCheck, brokersByRack, rackByBroker);
    ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(_combinedCompletenessRequirements, _allowCapacityEstimation, _operationProgress);
    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = clusterModel.getReplicaDistribution();

    // First try to add and remove replicas to achieve the replication factor for topics of interest.
    clusterModel.createOrDeleteReplicas(_topicsToChangeByReplicationFactor, brokersByRack, rackByBroker, _cluster);

    if (!clusterModel.isClusterAlive()) {
      throw new IllegalArgumentException("All brokers are dead in the cluster.");
    }

    Set<String> excludedTopics = _kafkaCruiseControl.excludedTopics(clusterModel, _excludedTopics);
    LOG.debug("Topics excluded from partition movement: {}", excludedTopics);
    OptimizationOptions optimizationOptions = new OptimizationOptions(excludedTopics,
                                                                      excludedBrokersForLeadership,
                                                                      excludedBrokersForReplicaMove,
                                                                      false,
                                                                      Collections.emptySet(),
                                                                      true);
    // Then further optimize the location of newly added replicas based on goals. Here we restrict the replica movement to
    // only considering newly added replicas, in order to minimize the total bytes to move.
    OptimizerResult result = _kafkaCruiseControl.optimizations(clusterModel, _goalsByPriority, _operationProgress,
                                                               initReplicaDistribution, optimizationOptions);
    if (!_dryRun) {
      _kafkaCruiseControl.executeProposals(result.goalProposals(), Collections.emptySet(), false, _concurrentInterBrokerPartitionMovements,
                                           0, _concurrentLeaderMovements, _executionProgressCheckIntervalMs,
                                           _replicaMovementStrategy, _replicationThrottle, _isTriggeredByUserRequest, _uuid, () -> _reason);
    }
    return result;
  }

  @Override
  protected boolean shouldWorkWithClusterModel() {
    return true;
  }

  @Override
  protected OptimizerResult workWithoutClusterModel() {
    return null;
  }

  @Override
  protected void finish() {
    super.finish();
    _cluster = null;
    _topicsToChangeByReplicationFactor.clear();
  }
}
