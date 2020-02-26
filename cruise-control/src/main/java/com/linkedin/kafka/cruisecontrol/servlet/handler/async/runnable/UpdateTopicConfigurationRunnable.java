/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicConfigurationParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicReplicationFactorChangeParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.goalsByPriority;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckGoals;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckLoadMonitorReadiness;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_DRYRUN;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_SKIP_RACK_AWARENESS_CHECK;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_CONCURRENT_MOVEMENTS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_SKIP_HARD_GOAL_CHECK;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_MODEL_COMPLETENESS_REQUIREMENTS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_STOP_ONGOING_EXECUTION;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.maybeStopOngoingExecutionToModifyAndWait;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.populateRackInfoForReplicationFactorChange;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.partitionWithOfflineReplicas;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.topicsForReplicationFactorChange;


/**
 * The async runnable for updating topic configuration.
 */
public class UpdateTopicConfigurationRunnable extends OperationRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateTopicConfigurationRunnable.class);
  protected final String _uuid;
  protected Map<Short, Pattern> _topicPatternByReplicationFactor;
  protected List<String> _goals;
  protected boolean _skipRackAwarenessCheck;
  protected ModelCompletenessRequirements _requirements;
  protected boolean _allowCapacityEstimation;
  protected Integer _concurrentInterBrokerPartitionMovements;
  protected Integer _concurrentLeaderMovements;
  protected Long _executionProgressCheckIntervalMs;
  protected boolean _skipHardGoalCheck;
  protected ReplicaMovementStrategy _replicaMovementStrategy;
  protected Long _replicationThrottle;
  protected boolean _excludeRecentlyDemotedBrokers;
  protected boolean _excludeRecentlyRemovedBrokers;
  protected boolean _dryRun;
  protected String _reason;
  protected boolean _stopOngoingExecution;
  protected boolean _isTriggeredByUserRequest;

  public UpdateTopicConfigurationRunnable(KafkaCruiseControl kafkaCruiseControl,
                                          OperationFuture future,
                                          String uuid,
                                          TopicConfigurationParameters parameters) {
    super(kafkaCruiseControl, future);
    TopicReplicationFactorChangeParameters topicReplicationFactorChangeParameters = parameters.topicReplicationFactorChangeParameters();
    if (topicReplicationFactorChangeParameters != null) {
      _topicPatternByReplicationFactor = topicReplicationFactorChangeParameters.topicPatternByReplicationFactor();
      _goals = topicReplicationFactorChangeParameters.goals();
      _skipRackAwarenessCheck = topicReplicationFactorChangeParameters.skipRackAwarenessCheck();
      _requirements = topicReplicationFactorChangeParameters.modelCompletenessRequirements();
      _allowCapacityEstimation = topicReplicationFactorChangeParameters.allowCapacityEstimation();
      _concurrentInterBrokerPartitionMovements = topicReplicationFactorChangeParameters.concurrentInterBrokerPartitionMovements();
      _concurrentLeaderMovements = topicReplicationFactorChangeParameters.concurrentLeaderMovements();
      _executionProgressCheckIntervalMs = topicReplicationFactorChangeParameters.executionProgressCheckIntervalMs();
      _skipHardGoalCheck = topicReplicationFactorChangeParameters.skipHardGoalCheck();
      _replicaMovementStrategy = topicReplicationFactorChangeParameters.replicaMovementStrategy();
      _replicationThrottle = topicReplicationFactorChangeParameters.replicationThrottle();
      _excludeRecentlyDemotedBrokers = topicReplicationFactorChangeParameters.excludeRecentlyDemotedBrokers();
      _excludeRecentlyRemovedBrokers = topicReplicationFactorChangeParameters.excludeRecentlyRemovedBrokers();
      _dryRun = topicReplicationFactorChangeParameters.dryRun();
      _reason = topicReplicationFactorChangeParameters.reason();
      _stopOngoingExecution = topicReplicationFactorChangeParameters.stopOngoingExecution();
      }
    _uuid = uuid;
    _isTriggeredByUserRequest = true;
  }

  /**
   * Constructor to be used for creating a runnable for self-healing.
   */
  public UpdateTopicConfigurationRunnable(KafkaCruiseControl kafkaCruiseControl,
                                          Map<Short, Pattern> topicPatternByReplicationFactor,
                                          List<String> selfHealingGoals,
                                          boolean allowCapacityEstimation,
                                          boolean excludeRecentlyDemotedBrokers,
                                          boolean excludeRecentlyRemovedBrokers,
                                          String anomalyId,
                                          Supplier<String> reasonSupplier) {
    super(kafkaCruiseControl, new OperationFuture("Topic replication factor anomaly self-healing."));
    _topicPatternByReplicationFactor = topicPatternByReplicationFactor;
    _goals = selfHealingGoals;
    _skipRackAwarenessCheck = SELF_HEALING_SKIP_RACK_AWARENESS_CHECK;
    _requirements = SELF_HEALING_MODEL_COMPLETENESS_REQUIREMENTS;
    _allowCapacityEstimation = allowCapacityEstimation;
    _concurrentInterBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _concurrentLeaderMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _executionProgressCheckIntervalMs = SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
    _skipHardGoalCheck = SELF_HEALING_SKIP_HARD_GOAL_CHECK;
    _replicaMovementStrategy = SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
    _replicationThrottle = kafkaCruiseControl.config().getLong(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG);
    _excludeRecentlyDemotedBrokers = excludeRecentlyDemotedBrokers;
    _excludeRecentlyRemovedBrokers = excludeRecentlyRemovedBrokers;
    _dryRun = SELF_HEALING_DRYRUN;
    _reason = reasonSupplier.get();
    _stopOngoingExecution = SELF_HEALING_STOP_ONGOING_EXECUTION;
    _uuid = anomalyId;
    _isTriggeredByUserRequest = false;
  }


  @Override
  public OptimizationResult getResult() throws Exception {
    if (_topicPatternByReplicationFactor != null) {
      return new OptimizationResult(updateTopicReplicationFactor(), _kafkaCruiseControl.config());
    }
    // Never reaches here.
    throw new IllegalArgumentException("Nothing executable found in request.");
  }

  /**
   * Update configuration of topics which match topic patterns. Currently only support changing topic's replication factor.
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
   *     Further optimize new replica's location with provided {@link Goal} list.
   *   </li>
   * </ol>
   *
   * If partition's current replication factor is larger than target replication factor, remove one or more follower replicas
   * from the partition. Replicas are removed following the reverse order of position in partition's replica list.
   *
   * @return The optimization result.
   * @throws KafkaCruiseControlException When any exception occurred during the topic configuration updating.
   */
  public OptimizerResult updateTopicReplicationFactor()
      throws KafkaCruiseControlException {
    _kafkaCruiseControl.sanityCheckDryRun(_dryRun, _stopOngoingExecution);
    sanityCheckGoals(_goals, _skipHardGoalCheck, _kafkaCruiseControl.config());
    List<Goal> goalsByPriority = goalsByPriority(_goals, _kafkaCruiseControl.config());
    OperationProgress operationProgress = _future.operationProgress();
    if (goalsByPriority.isEmpty()) {
      throw new IllegalArgumentException("At least one goal must be provided to get an optimization result.");
    } else if (_stopOngoingExecution) {
      maybeStopOngoingExecutionToModifyAndWait(_kafkaCruiseControl, operationProgress);
    }

    Cluster cluster = _kafkaCruiseControl.kafkaCluster();
    // Ensure there is no offline replica in the cluster.
    PartitionInfo partitionInfo = partitionWithOfflineReplicas(cluster);
    if (partitionInfo != null) {
      throw new IllegalStateException(String.format("Topic partition %s-%d has offline replicas on brokers %s.",
                                                    partitionInfo.topic(), partitionInfo.partition(),
                                                    Arrays.stream(partitionInfo.offlineReplicas()).mapToInt(Node::id)
                                                          .boxed().collect(Collectors.toSet())));
    }
    Map<Short, Set<String>> topicsToChangeByReplicationFactor = topicsForReplicationFactorChange(_topicPatternByReplicationFactor, cluster);

    // Generate cluster model and get proposal
    OptimizerResult result;
    Map<String, List<Integer>> brokersByRack = new HashMap<>();
    Map<Integer, String> rackByBroker = new HashMap<>();
    ModelCompletenessRequirements completenessRequirements = _kafkaCruiseControl.modelCompletenessRequirements(goalsByPriority).weaker(_requirements);
    sanityCheckLoadMonitorReadiness(completenessRequirements, _kafkaCruiseControl.getLoadMonitorTaskRunnerState());
    try (AutoCloseable ignored = _kafkaCruiseControl.acquireForModelGeneration(operationProgress)) {
      ExecutorState executorState = _kafkaCruiseControl.executorState();
      Set<Integer> excludedBrokersForLeadership = _excludeRecentlyDemotedBrokers ? executorState.recentlyDemotedBrokers()
                                                                                 : Collections.emptySet();
      Set<Integer> excludedBrokersForReplicaMove = _excludeRecentlyRemovedBrokers ? executorState.recentlyRemovedBrokers()
                                                                                  : Collections.emptySet();
      populateRackInfoForReplicationFactorChange(topicsToChangeByReplicationFactor, cluster, excludedBrokersForReplicaMove,
                                                 _skipRackAwarenessCheck, brokersByRack, rackByBroker);
      ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(completenessRequirements, _allowCapacityEstimation, operationProgress);
      Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = clusterModel.getReplicaDistribution();

      // First try to add and remove replicas to achieve the replication factor for topics of interest.
      clusterModel.createOrDeleteReplicas(topicsToChangeByReplicationFactor, brokersByRack, rackByBroker, cluster);

      if (!clusterModel.isClusterAlive()) {
        throw new IllegalArgumentException("All brokers are dead in the cluster.");
      }

      Set<String> excludedTopics = _kafkaCruiseControl.excludedTopics(clusterModel, null);
      LOG.debug("Topics excluded from partition movement: {}", excludedTopics);
      OptimizationOptions optimizationOptions = new OptimizationOptions(excludedTopics,
                                                                        excludedBrokersForLeadership,
                                                                        excludedBrokersForReplicaMove,
                                                                        false,
                                                                        Collections.emptySet(),
                                                                        true);
      // Then further optimize the location of newly added replicas based on goals. Here we restrict the replica movement to
      // only considering newly added replicas, in order to minimize the total bytes to move.
      result = _kafkaCruiseControl.optimizations(clusterModel, goalsByPriority, operationProgress, initReplicaDistribution, optimizationOptions);
      if (!_dryRun) {
        _kafkaCruiseControl.executeProposals(result.goalProposals(), Collections.emptySet(), false, _concurrentInterBrokerPartitionMovements,
                                             0, _concurrentLeaderMovements, _executionProgressCheckIntervalMs,
                                             _replicaMovementStrategy, _replicationThrottle, _isTriggeredByUserRequest, _uuid, () -> _reason);
      }
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
    return result;
  }
}
