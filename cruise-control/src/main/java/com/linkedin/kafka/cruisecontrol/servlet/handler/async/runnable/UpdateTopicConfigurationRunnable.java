/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.goalsByPriority;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckCapacityEstimation;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckGoals;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckLoadMonitorReadiness;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.maybeStopOngoingExecutionToModifyAndWait;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.populateRackInfoForReplicationFactorChange;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.partitionWithOfflineReplicas;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.topicsForReplicationFactorChange;


/**
 * The async runnable for updating topic configuration.
 */
public class UpdateTopicConfigurationRunnable extends OperationRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateTopicConfigurationRunnable.class);
  protected final TopicReplicationFactorChangeParameters _topicReplicationFactorChangeParameters;
  protected final String _uuid;

  public UpdateTopicConfigurationRunnable(KafkaCruiseControl kafkaCruiseControl,
                                          OperationFuture future,
                                          String uuid,
                                          TopicConfigurationParameters parameters) {
    super(kafkaCruiseControl, future);
    _topicReplicationFactorChangeParameters = parameters.topicReplicationFactorChangeParameters();
    _uuid = uuid;
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    if (_topicReplicationFactorChangeParameters != null) {
      return new OptimizationResult(
          updateTopicReplicationFactor(_topicReplicationFactorChangeParameters.topicPatternByReplicationFactor(),
                                       _topicReplicationFactorChangeParameters.goals(),
                                       _topicReplicationFactorChangeParameters.skipRackAwarenessCheck(),
                                       _topicReplicationFactorChangeParameters.modelCompletenessRequirements(),
                                       _topicReplicationFactorChangeParameters.allowCapacityEstimation(),
                                       _topicReplicationFactorChangeParameters.concurrentInterBrokerPartitionMovements(),
                                       _topicReplicationFactorChangeParameters.concurrentLeaderMovements(),
                                       _topicReplicationFactorChangeParameters.executionProgressCheckIntervalMs(),
                                       _topicReplicationFactorChangeParameters.skipHardGoalCheck(),
                                       _topicReplicationFactorChangeParameters.replicaMovementStrategy(),
                                       _topicReplicationFactorChangeParameters.replicationThrottle(),
                                       _topicReplicationFactorChangeParameters.excludeRecentlyDemotedBrokers(),
                                       _topicReplicationFactorChangeParameters.excludeRecentlyRemovedBrokers(),
                                       _topicReplicationFactorChangeParameters.dryRun(),
                                       _topicReplicationFactorChangeParameters.reason(),
                                       _topicReplicationFactorChangeParameters.stopOngoingExecution()),
          _kafkaCruiseControl.config());
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
   * @param topicPatternByReplicationFactor The name patterns of topic to apply the change with the target replication factor.
   *                                        If no topic in the cluster matches the patterns, an exception will be thrown.
   * @param goals The goals to be met during the new replica assignment. When empty all goals will be used.
   * @param skipTopicRackAwarenessCheck Whether ignore rack awareness property if number of rack in cluster is less
   *                                    than target replication factor.
   * @param requirements The cluster model completeness requirements.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @param concurrentInterBrokerPartitionMovements The maximum number of concurrent inter-broker partition movements per broker
   *                                                (if null, use num.concurrent.partition.movements.per.broker).
   * @param concurrentLeaderMovements The maximum number of concurrent leader movements
   *                                  (if null, use num.concurrent.leader.movements).
   * @param executionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *                                         execution (if null, use execution.progress.check.interval.ms).
   * @param skipHardGoalCheck True if the provided {@code goals} do not have to contain all hard goals, false otherwise.
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when executing demote operations (if null, no throttling is applied).
   * @param excludeRecentlyDemotedBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
   * @param excludeRecentlyRemovedBrokers Exclude recently removed brokers from proposal generation for replica transfer.
   * @param dryRun Whether it is a dry run or not.
   * @param reason Reason of execution.
   * @param stopOngoingExecution True to stop the ongoing execution (if any) and start executing the given proposals,
   *                             false otherwise.
   *
   * @return The optimization result.
   * @throws KafkaCruiseControlException When any exception occurred during the topic configuration updating.
   */
  public OptimizerResult updateTopicReplicationFactor(Map<Short, Pattern> topicPatternByReplicationFactor,
                                                      List<String> goals,
                                                      boolean skipTopicRackAwarenessCheck,
                                                      ModelCompletenessRequirements requirements,
                                                      boolean allowCapacityEstimation,
                                                      Integer concurrentInterBrokerPartitionMovements,
                                                      Integer concurrentLeaderMovements,
                                                      Long executionProgressCheckIntervalMs,
                                                      boolean skipHardGoalCheck,
                                                      ReplicaMovementStrategy replicaMovementStrategy,
                                                      Long replicationThrottle,
                                                      boolean excludeRecentlyDemotedBrokers,
                                                      boolean excludeRecentlyRemovedBrokers,
                                                      boolean dryRun,
                                                      String reason,
                                                      boolean stopOngoingExecution)
      throws KafkaCruiseControlException {
    _kafkaCruiseControl.sanityCheckDryRun(dryRun, stopOngoingExecution);
    sanityCheckGoals(goals, skipHardGoalCheck, _kafkaCruiseControl.config());
    List<Goal> goalsByPriority = goalsByPriority(goals, _kafkaCruiseControl.config());
    OperationProgress operationProgress = _future.operationProgress();
    if (goalsByPriority.isEmpty()) {
      throw new IllegalArgumentException("At least one goal must be provided to get an optimization result.");
    } else if (stopOngoingExecution) {
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
    Map<Short, Set<String>> topicsToChangeByReplicationFactor = topicsForReplicationFactorChange(topicPatternByReplicationFactor, cluster);

    // Generate cluster model and get proposal
    OptimizerResult result;
    Map<String, List<Integer>> brokersByRack = new HashMap<>();
    Map<Integer, String> rackByBroker = new HashMap<>();
    ModelCompletenessRequirements completenessRequirements = _kafkaCruiseControl.modelCompletenessRequirements(goalsByPriority).weaker(requirements);
    sanityCheckLoadMonitorReadiness(completenessRequirements, _kafkaCruiseControl.getLoadMonitorTaskRunnerState());
    try (AutoCloseable ignored = _kafkaCruiseControl.acquireForModelGeneration(operationProgress)) {
      ExecutorState executorState = _kafkaCruiseControl.executorState();
      Set<Integer> excludedBrokersForLeadership = excludeRecentlyDemotedBrokers ? executorState.recentlyDemotedBrokers()
                                                                                : Collections.emptySet();
      Set<Integer> excludedBrokersForReplicaMove = excludeRecentlyRemovedBrokers ? executorState.recentlyRemovedBrokers()
                                                                                 : Collections.emptySet();
      populateRackInfoForReplicationFactorChange(topicsToChangeByReplicationFactor, cluster, excludedBrokersForReplicaMove,
                                                 skipTopicRackAwarenessCheck, brokersByRack, rackByBroker);

      ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(completenessRequirements, operationProgress);
      sanityCheckCapacityEstimation(allowCapacityEstimation, clusterModel.capacityEstimationInfoByBrokerId());
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
      if (!dryRun) {
        _kafkaCruiseControl.executeProposals(result.goalProposals(), Collections.emptySet(), false, concurrentInterBrokerPartitionMovements,
                                             0, concurrentLeaderMovements, executionProgressCheckIntervalMs,
                                             replicaMovementStrategy, replicationThrottle, true, _uuid, reason);
      }
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
    return result;
  }
}
