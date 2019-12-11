/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.async.progress.WaitingForOngoingExecutionToStop;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State.NO_TASK_IN_PROGRESS;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.STOP_PROPOSAL_EXECUTION;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.FORCE_STOP_PARAM;


public class RunnableUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlUtils.class);
  // Common self healing constants used in runnables.
  public static final boolean SELF_HEALING_DRYRUN = false;
  public static final Set<Integer> SELF_HEALING_DESTINATION_BROKER_IDS = Collections.emptySet();
  public static final ReplicaMovementStrategy SELF_HEALING_REPLICA_MOVEMENT_STRATEGY = null;
  public static final Pattern SELF_HEALING_EXCLUDED_TOPICS = null;
  public static final Integer SELF_HEALING_CONCURRENT_MOVEMENTS = null;
  public static final Long SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS = null;
  public static final boolean SELF_HEALING_SKIP_HARD_GOAL_CHECK = false;
  public static final boolean SELF_HEALING_STOP_ONGOING_EXECUTION = false;
  public static final ModelCompletenessRequirements SELF_HEALING_MODEL_COMPLETENESS_REQUIREMENTS = null;
  public static final boolean SELF_HEALING_SKIP_URP_DEMOTION = true;
  public static final boolean SELF_HEALING_EXCLUDE_FOLLOWER_DEMOTION = true;
  private static final Set<String> KAFKA_ASSIGNER_GOALS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(KafkaAssignerEvenRackAwareGoal.class.getSimpleName(),
                                                              KafkaAssignerDiskUsageDistributionGoal.class.getSimpleName())));

  private RunnableUtils() {
  }

  /**
   * Populate cluster rack information for topics to change replication factor. In the process this method also conducts a sanity
   * check to ensure that there are enough racks in the cluster to allocate new replicas to racks which do not host replica
   * of the same partition.
   *
   * @param topicsByReplicationFactor Topics to change replication factor by target replication factor.
   * @param cluster Current cluster state.
   * @param excludedBrokersForReplicaMove Set of brokers which do not host new replicas.
   * @param skipTopicRackAwarenessCheck Whether to skip the rack awareness sanity check or not.
   * @param brokersByRack Mapping from rack to broker.
   * @param rackByBroker Mapping from broker to rack.
   */
  public static void populateRackInfoForReplicationFactorChange(Map<Short, Set<String>> topicsByReplicationFactor,
                                                                Cluster cluster,
                                                                Set<Integer> excludedBrokersForReplicaMove,
                                                                boolean skipTopicRackAwarenessCheck,
                                                                Map<String, List<Integer>> brokersByRack,
                                                                Map<Integer, String> rackByBroker) {
    for (Node node : cluster.nodes()) {
      // New follower replica is not assigned to brokers excluded for replica movement.
      if (excludedBrokersForReplicaMove.contains(node.id())) {
        continue;
      }
      // If the rack is not specified, we use the broker id info as rack info.
      String rack = node.rack() == null || node.rack().isEmpty() ? String.valueOf(node.id()) : node.rack();
      brokersByRack.putIfAbsent(rack, new ArrayList<>());
      brokersByRack.get(rack).add(node.id());
      rackByBroker.put(node.id(), rack);
    }

    topicsByReplicationFactor.forEach((replicationFactor, topics) -> {
      if (replicationFactor > brokersByRack.size()) {
        if (skipTopicRackAwarenessCheck) {
          LOG.info("Target replication factor for topics {} is {}, which is larger than number of racks in cluster. Rack-awareness "
                   + "property will be violated to add new replicas.", topics, replicationFactor);
        } else {
          throw new RuntimeException(String.format("Unable to change replication factor of topics %s to %d since there are only %d "
                                                   + "racks in the cluster, to skip the rack-awareness check, set %s to true in the request.",
                                                   topics, replicationFactor, brokersByRack.size(), ParameterUtils.SKIP_RACK_AWARENESS_CHECK_PARAM));
        }
      }
    });
  }

  /**
   * Check that only one target replication factor is set for each topic.
   *
   * @param topicsToChangeByReplicationFactor Topics to change replication factor by target replication factor.
   */
  private static void sanityCheckTargetReplicationFactorForTopic(Map<Short, Set<String>> topicsToChangeByReplicationFactor) {
    Set<String> topicsToChange = new HashSet<>();
    Set<String> topicsHavingMultipleTargetReplicationFactors = new HashSet<>();
    for (Set<String> topics : topicsToChangeByReplicationFactor.values()) {
      for (String topic : topics) {
        if (!topicsToChange.add(topic)) {
          topicsHavingMultipleTargetReplicationFactors.add(topic);
        }
      }
    }
    if (!topicsHavingMultipleTargetReplicationFactors.isEmpty()) {
      throw new IllegalStateException(String.format("Topics %s are requested with more than one target replication factor.",
                                                    topicsHavingMultipleTargetReplicationFactors));
    }
  }

  /**
   * Populate topics to change replication factor based on the request and current cluster state.
   * @param topicPatternByReplicationFactor Requested topic patterns to change replication factor by target replication factor.
   * @param cluster Current cluster state.
   * @return Topics to change replication factor by target replication factor.
   */
  public static Map<Short, Set<String>> topicsForReplicationFactorChange(Map<Short, Pattern> topicPatternByReplicationFactor,
                                                                         Cluster cluster) {
    Map<Short, Set<String>> topicsToChangeByReplicationFactor = new HashMap<>(topicPatternByReplicationFactor.size());
    for (Map.Entry<Short, Pattern> entry : topicPatternByReplicationFactor.entrySet()) {
      short replicationFactor = entry.getKey();
      Pattern topicPattern = entry.getValue();
      Set<String> topics = cluster.topics().stream().filter(t -> topicPattern.matcher(t).matches()).collect(Collectors.toSet());
      // Ensure there are topics matching the requested topic pattern.
      if (topics.isEmpty()) {
        throw new IllegalStateException(String.format("There is no topic in cluster matching pattern '%s'.", topicPattern));
      }
      Set<String> topicsToChange = topics.stream()
                                         .filter(t -> cluster.partitionsForTopic(t).stream().anyMatch(p -> p.replicas().length != replicationFactor))
                                         .collect(Collectors.toSet());
      if (!topicsToChange.isEmpty()) {
        topicsToChangeByReplicationFactor.put(replicationFactor, topicsToChange);
      }
    }

    if (topicsToChangeByReplicationFactor.isEmpty()) {
      throw new IllegalStateException(String.format("All topics matching given pattern already have target replication factor. Requested "
                                                    + "topic pattern by replication factor: %s.", topicPatternByReplicationFactor));
    }
    // Sanity check that no topic is set with more than one target replication factor.
    sanityCheckTargetReplicationFactorForTopic(topicsToChangeByReplicationFactor);
    return topicsToChangeByReplicationFactor;
  }

  /**
   * Check if the ClusterAndGeneration needs to be refreshed to retrieve the requested substates.
   *
   * @param substates Substates for which the need for refreshing the ClusterAndGeneration will be evaluated.
   * @return True if substates contain {@link CruiseControlState.SubState#ANALYZER} or
   * {@link CruiseControlState.SubState#MONITOR}, false otherwise.
   */
  public static boolean shouldRefreshClusterAndGeneration(Set<CruiseControlState.SubState> substates) {
    return substates.stream()
                    .anyMatch(substate -> substate == CruiseControlState.SubState.ANALYZER
                                          || substate == CruiseControlState.SubState.MONITOR);
  }

  /**
   * Check partitions in the given cluster for existence of offline replicas, and get the first such partition.
   * If there no such partitions are identified, return {@code null}.
   *
   * @param cluster The current cluster state.
   * @return The first identified partition with offline replicas, {@code null} if no such partition exists.
   */
  public static PartitionInfo partitionWithOfflineReplicas(Cluster cluster) {
    for (String topic : cluster.topics()) {
      for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
        if (partitionInfo.offlineReplicas().length > 0) {
          return partitionInfo;
        }
      }
    }
    return null;
  }

  /**
   * Check whether any of the given goals contain a Kafka Assigner goal.
   *
   * @param goals The goals to check
   * @return True if the given goals contain a Kafka Assigner goal, false otherwise.
   */
  public static boolean isKafkaAssignerMode(Collection<String> goals) {
    return goals.stream().anyMatch(KAFKA_ASSIGNER_GOALS::contains);
  }

  /**
   * Sanity check to ensure that the given cluster model has no offline replicas on bad disks in Kafka Assigner mode.
   * @param goals Goals to check whether it is Kafka Assigner mode or not.
   * @param clusterModel Cluster model for which the existence of an offline replicas on bad disks will be verified.
   */
  public static void sanityCheckBrokersHavingOfflineReplicasOnBadDisks(List<String> goals, ClusterModel clusterModel) {
    if (isKafkaAssignerMode(goals) && !clusterModel.brokersHavingOfflineReplicasOnBadDisks().isEmpty()) {
      throw new IllegalStateException("Kafka Assigner mode is not supported when there are offline replicas on bad disks."
                                      + " Please run fix_offline_replicas before using Kafka Assigner mode.");
    }
  }

  /**
   * Gracefully stop the ongoing execution (if any) and wait until the execution stops. Exceptional cases:
   * <ul>
   *   <li>If there is no ongoing Cruise Control execution, this call is a noop.</li>
   *   <li>If another request has asked for modifying the ongoing execution, then throw an exception.</li>
   * </ul>
   *
   * The caller of this method should be aware that the following scenario is possible and acceptable:
   * <ol>
   *   <li>A user request calls this function to stop the ongoing execution.</li>
   *   <li>This function stops the ongoing execution -- i.e. the execution state now is
   *   {@link com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State#NO_TASK_IN_PROGRESS}.</li>
   *   <li>Before the caller of this function would start a new execution, another user request (or self-healing)
   *   can start a new execution.</li>
   * </ol>
   *
   * @param kafkaCruiseControl The Kafka Cruise Control instance.
   * @param operationProgress The progress for the job.
   */
  public static void maybeStopOngoingExecutionToModifyAndWait(KafkaCruiseControl kafkaCruiseControl,
                                                              OperationProgress operationProgress) {
    if (!kafkaCruiseControl.hasOngoingExecution()) {
      LOG.info("There is already no ongoing Cruise Control execution. Skip stopping execution.");
      return;
    } else if (!kafkaCruiseControl.modifyOngoingExecution(true)) {
      throw new IllegalStateException("Another request has asked for modifying the ongoing execution.");
    }

    LOG.info("Gracefully stopping the ongoing execution... Use {} endpoint with {}=true to force-stop it now.",
             STOP_PROPOSAL_EXECUTION, FORCE_STOP_PARAM);

    WaitingForOngoingExecutionToStop step = new WaitingForOngoingExecutionToStop();
    operationProgress.addStep(step);

    while (kafkaCruiseControl.executionState() != NO_TASK_IN_PROGRESS) {
      // Stop ongoing execution, and wait for the execution to stop.
      try {
        kafkaCruiseControl.userTriggeredStopExecution(false);
        Thread.sleep(kafkaCruiseControl.executionProgressCheckIntervalMs());
      } catch (InterruptedException e) {
        kafkaCruiseControl.modifyOngoingExecution(false);
        throw new IllegalStateException("Interrupted while waiting for gracefully stopping the ongoing execution.");
      }
    }
    step.done();
  }

  /**
   * Sanity check to ensure that the given cluster model contains brokers with offline replicas.
   * @param clusterModel Cluster model for which the existence of an offline replica will be verified.
   */
  public static void sanityCheckOfflineReplicaPresence(ClusterModel clusterModel) {
    if (clusterModel.brokersHavingOfflineReplicasOnBadDisks().isEmpty()) {
      for (Broker deadBroker : clusterModel.deadBrokers()) {
        if (!deadBroker.replicas().isEmpty()) {
          // Has offline replica(s) on a dead broker.
          return;
        }
      }
      throw new IllegalStateException("Cluster has no offline replica on brokers " + clusterModel.brokers() + " to fix.");
    }
    // Has offline replica(s) on a broken disk.
  }
}
