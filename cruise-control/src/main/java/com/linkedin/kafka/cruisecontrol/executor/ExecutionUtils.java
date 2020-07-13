/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import kafka.zk.KafkaZkClient;
import kafka.zk.ZkVersion;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class ExecutionUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionUtils.class);
  public static final long METADATA_REFRESH_BACKOFF = 100L;
  public static final long METADATA_EXPIRY_MS = Long.MAX_VALUE;
  public static final String EXECUTION_STARTED = "execution-started";
  public static final String KAFKA_ASSIGNER_MODE = "kafka_assigner";
  public static final String EXECUTION_STOPPED = "execution-stopped";
  public static final String GAUGE_EXECUTION_STOPPED = EXECUTION_STOPPED;
  public static final String GAUGE_EXECUTION_STOPPED_BY_USER = EXECUTION_STOPPED + "-by-user";
  public static final String GAUGE_EXECUTION_STARTED_IN_KAFKA_ASSIGNER_MODE = EXECUTION_STARTED + "-" + KAFKA_ASSIGNER_MODE;
  public static final String GAUGE_EXECUTION_STARTED_IN_NON_KAFKA_ASSIGNER_MODE = EXECUTION_STARTED + "-non-" + KAFKA_ASSIGNER_MODE;


  private ExecutionUtils() { }

  /**
   * Checks whether the topicPartitions of the execution tasks in the given subset is indeed a subset of the given set.
   *
   * @param set The original set.
   * @param subset The subset to validate whether it is indeed a subset of the given set.
   * @return True if the topicPartitions of the given subset constitute a subset of the given set, false otherwise.
   */
  public static boolean isSubset(Set<TopicPartition> set, Collection<ExecutionTask> subset) {
    boolean isSubset = true;
    for (ExecutionTask executionTask : subset) {
      TopicPartition tp = executionTask.proposal().topicPartition();
      if (!set.contains(tp)) {
        isSubset = false;
        break;
      }
    }

    return isSubset;
  }

  /**
   * Deletes zNodes for ongoing replica and leadership movement tasks. Then deletes controller zNode to trigger a
   * controller re-election for the cancellation to take effect.
   *
   * This operation has side-effects that may leave partitions with extra replication factor and changes the controller.
   * Hence, it will be deprecated in Kafka 2.4+, which introduced PartitionReassignment Kafka API to enable graceful and
   * instant mechanism to cancel ongoing replica reassignments.
   *
   * @param kafkaZkClient KafkaZkClient to use for deleting the relevant zNodes to force stop the execution (if any).
   */
  public static void deleteZNodesToStopExecution(KafkaZkClient kafkaZkClient) {
    // Delete zNode of ongoing replica movement tasks.
    LOG.info("Deleting zNode for ongoing replica movements {}.", kafkaZkClient.getPartitionReassignment());
    kafkaZkClient.deletePartitionReassignment(ZkVersion.MatchAnyVersion());
    // delete zNode of ongoing leadership movement tasks.
    LOG.info("Deleting zNode for ongoing leadership changes {}.", kafkaZkClient.getPreferredReplicaElection());
    kafkaZkClient.deletePreferredReplicaElection(ZkVersion.MatchAnyVersion());
    // Delete controller zNode to trigger a controller re-election.
    LOG.info("Deleting controller zNode to re-elect a new controller. Old controller is {}.", kafkaZkClient.getControllerId());
    kafkaZkClient.deleteController(ZkVersion.MatchAnyVersion());
  }

  /**
   * Check if the given task is done.
   * For what it means to be done, see {@link #isInterBrokerReplicaActionDone}, {@link #isIntraBrokerReplicaActionDone},
   * and {@link #isLeadershipMovementDone}.
   *
   * @param cluster Kafka cluster.
   * @param logdirInfoByTask Disk information for the intra-broker replica movement task, ignored otherwise.
   * @param task Task to check for being done.
   * @return {@code true} if task is done, {@code false} otherwise.
   */
  public static boolean isTaskDone(Cluster cluster,
                                   Map<ExecutionTask, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> logdirInfoByTask,
                                   ExecutionTask task) {
    switch (task.type()) {
      case INTER_BROKER_REPLICA_ACTION:
        return isInterBrokerReplicaActionDone(cluster, task);
      case INTRA_BROKER_REPLICA_ACTION:
        return isIntraBrokerReplicaActionDone(logdirInfoByTask, task);
      case LEADER_ACTION:
        return isLeadershipMovementDone(cluster, task);
      default:
        throw new IllegalStateException("Unknown task type " + task.type());
    }
  }

  /**
   * For an inter-broker replica movement action, the completion depends on the task state:
   * <ul>
   *   <li>{@link ExecutionTask.State#IN_PROGRESS}: when the current replica list is the same as the new replica list
   *   and all replicas are in-sync.</li>
   *   <li>{@link ExecutionTask.State#ABORTING}: done when the current replica list is the same as the old replica list.
   *   Due to race condition, we also consider it done if the current replica list is the same as the new replica list
   *   and all replicas are in-sync.</li>
   *   <li>{@link ExecutionTask.State#DEAD}: always considered as done because we neither move forward or rollback.</li>
   * </ul>
   *
   * There should be no other task state seen here.
   *
   * @param cluster Kafka cluster.
   * @param task Task to check for being done.
   * @return {@code true} if task is done, {@code false} otherwise.
   */
  private static boolean isInterBrokerReplicaActionDone(Cluster cluster, ExecutionTask task) {
    PartitionInfo partitionInfo = cluster.partition(task.proposal().topicPartition());
    switch (task.state()) {
      case IN_PROGRESS:
        return task.proposal().isInterBrokerMovementCompleted(partitionInfo);
      case ABORTING:
        return task.proposal().isInterBrokerMovementAborted(partitionInfo);
      case DEAD:
        return true;
      default:
        throw new IllegalStateException("Should never be here. State " + task.state());
    }
  }

  /**
   * Check whether intra-broker replica movement is done by comparing replica's current logdir with the logdir proposed
   * by task's proposal.
   *
   * @param logdirInfoByTask Disk information by task.
   * @param task Task to check for being done.
   * @return {@code true} if task is done, {@code false} otherwise.
   */
  private static boolean isIntraBrokerReplicaActionDone(
      Map<ExecutionTask, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> logdirInfoByTask,
      ExecutionTask task) {
    if (logdirInfoByTask.containsKey(task)) {
      return logdirInfoByTask.get(task).getCurrentReplicaLogDir()
                             .equals(task.proposal().replicasToMoveBetweenDisksByBroker().get(task.brokerId()).logdir());
    }
    return false;
  }

  /**
   * The completeness of leadership movement depends on the task state:
   * <ul>
   *   <li>{@link ExecutionTask.State#IN_PROGRESS}: Done when either (1) the proposed leader becomes the leader, (2) the
   *   partition has no leader in the cluster (e.g. deleted or offline), or (3) the partition has another leader and the
   *   proposed leader is out of ISR.</li>
   *   <li>{@link ExecutionTask.State#ABORTING} or {@link ExecutionTask.State#DEAD}: Always considered as done. The
   *   destination cannot become leader anymore.</li>
   * </ul>
   *
   * There should be no other task state seen here.
   *
   * @param cluster Kafka cluster.
   * @param task Task to check for being done.
   * @return {@code true} if task is done, {@code false} otherwise.
   */
  private static boolean isLeadershipMovementDone(Cluster cluster, ExecutionTask task) {
    switch (task.state()) {
      case IN_PROGRESS:
        TopicPartition tp = task.proposal().topicPartition();
        Node leader = cluster.leaderFor(tp);
        return (leader != null && leader.id() == task.proposal().newLeader().brokerId())
               || leader == null
               || !isInIsr(task.proposal().newLeader().brokerId(), cluster, tp);
      case ABORTING:
      case DEAD:
        return true;
      default:
        throw new IllegalStateException("Should never be here.");
    }
  }

  private static boolean isInIsr(Integer leader, Cluster cluster, TopicPartition tp) {
    return Arrays.stream(cluster.partition(tp).inSyncReplicas()).anyMatch(node -> node.id() == leader);
  }
}
