/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.PartitionReassignment;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import kafka.zk.KafkaZkClient;
import kafka.zk.ZkVersion;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_LOG_FLUSH_TIME_MS_999TH;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_999TH;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_REQUEST_QUEUE_SIZE;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH;


public final class ExecutionUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionUtils.class);
  public static final int DEFAULT_RETRY_BACKOFF_BASE = 2;
  public static final long METADATA_REFRESH_BACKOFF = 100L;
  public static final long METADATA_EXPIRY_MS = Long.MAX_VALUE;
  public static final String EXECUTION_STARTED = "execution-started";
  public static final String KAFKA_ASSIGNER_MODE = "kafka_assigner";
  public static final String EXECUTION_STOPPED = "execution-stopped";
  public static final String GAUGE_EXECUTION_STOPPED = EXECUTION_STOPPED;
  public static final String GAUGE_EXECUTION_STOPPED_BY_USER = EXECUTION_STOPPED + "-by-user";
  public static final String GAUGE_EXECUTION_STARTED_IN_KAFKA_ASSIGNER_MODE = EXECUTION_STARTED + "-" + KAFKA_ASSIGNER_MODE;
  public static final String GAUGE_EXECUTION_STARTED_IN_NON_KAFKA_ASSIGNER_MODE = EXECUTION_STARTED + "-non-" + KAFKA_ASSIGNER_MODE;
  public static final String GAUGE_EXECUTION_INTER_BROKER_PARTITION_MOVEMENTS_PER_BROKER_CAP = "inter-broker-partition-movements-per-broker-cap";
  public static final String GAUGE_EXECUTION_INTRA_BROKER_PARTITION_MOVEMENTS_PER_BROKER_CAP = "intra-broker-partition-movements-per-broker-cap";
  public static final String GAUGE_EXECUTION_LEADERSHIP_MOVEMENTS_GLOBAL_CAP = "leadership-movements-global-cap";
  public static final long EXECUTION_HISTORY_SCANNER_PERIOD_SECONDS = 5;
  public static final long EXECUTION_HISTORY_SCANNER_INITIAL_DELAY_SECONDS = 0;
  public static final int ADDITIVE_INCREASE_PARAM = 1;
  public static final int MULTIPLICATIVE_DECREASE_PARAM = 2;
  static final Map<String, Double> CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME = new HashMap<>(5);
  public static final long EXECUTION_TASK_FUTURE_ERROR_VERIFICATION_TIMEOUT_MS = 10000L;
  static long LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS;
  static int LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS;


  private ExecutionUtils() { }

  /**
   * Initialize the concurrency adjuster limits and timeout-related configs for list partition reassignment requests.
   *
   * @param config The configurations for Cruise Control.
   */
  static void init(KafkaCruiseControlConfig config) {
    CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.put(BROKER_LOG_FLUSH_TIME_MS_999TH.name(),
                                                  config.getDouble(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_LOG_FLUSH_TIME_MS_CONFIG));
    CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH.name(),
                                                  config.getDouble(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_FOLLOWER_FETCH_LOCAL_TIME_MS_CONFIG));
    CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.put(BROKER_PRODUCE_LOCAL_TIME_MS_999TH.name(),
                                                  config.getDouble(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_PRODUCE_LOCAL_TIME_MS_CONFIG));
    CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH.name(),
                                                  config.getDouble(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_CONSUMER_FETCH_LOCAL_TIME_MS_CONFIG));
    CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.put(BROKER_REQUEST_QUEUE_SIZE.name(),
                                                  config.getDouble(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_REQUEST_QUEUE_SIZE_CONFIG));
    LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS = config.getLong(ExecutorConfig.LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS_CONFIG);
    LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS = config.getInt(ExecutorConfig.LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS_CONFIG);
  }

  private static String toMetricName(Short metricId) {
    return KafkaMetricDef.brokerMetricDef().metricInfo(metricId).name();
  }

  /**
   * Check whether the current metrics are within the limit specified by {@link #CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME}.
   * Package private for unit tests.
   *
   * @param currentMetricsByBroker Current metrics by broker.
   * @return {@code true} if all brokers are within the limit specified by {@link #CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME},
   * {@code false} otherwise.
   */
  static boolean withinConcurrencyAdjusterLimit(Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker) {
    boolean withinLimit = true;
    Set<BrokerEntity> brokersWithNoMetrics = new HashSet<>();
    Map<String, StringBuilder> overLimitDetailsByMetricName = new HashMap<>(
        CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.size());
    for (String metricName : CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.keySet()) {
      overLimitDetailsByMetricName.put(metricName, new StringBuilder());
    }

    for (Map.Entry<BrokerEntity, ValuesAndExtrapolations> entry : currentMetricsByBroker.entrySet()) {
      BrokerEntity broker = entry.getKey();
      ValuesAndExtrapolations current = entry.getValue();
      if (current == null) {
        brokersWithNoMetrics.add(broker);
        continue;
      }

      // Check whether the broker is within the acceptable limit for the relevant metrics. If not, collect details.
      for (Short metricId : current.metricValues().metricIds()) {
        String metricName = toMetricName(metricId);
        Double limit = CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.get(metricName);
        if (limit != null) {
          double metricValue = current.metricValues().valuesFor(metricId).latest();
          if (metricValue > limit) {
            overLimitDetailsByMetricName.get(metricName).append(String.format("%d(%.2f) ", broker.brokerId(), metricValue));
          }
        }
      }
    }

    for (Map.Entry<String, StringBuilder> entry : overLimitDetailsByMetricName.entrySet()) {
      StringBuilder brokersWithValues = entry.getValue();
      if (brokersWithValues.length() > 0) {
        LOG.info("{} is over the acceptable limit for brokers with values: {}.", entry.getKey(), brokersWithValues);
        withinLimit = false;
      }
    }
    if (!brokersWithNoMetrics.isEmpty()) {
      LOG.warn("Assuming {} are over the acceptable limit as no broker metrics exist to verify.", brokersWithNoMetrics);
      withinLimit = false;
    }

    return withinLimit;
  }

  /**
   * Provide a recommended concurrency for the ongoing inter-broker partition movements using selected current broker
   * metrics and based on additive-increase/multiplicative-decrease (AIMD) feedback control algorithm.
   *
   * @param currentMetricsByBroker Current metrics by broker.
   * @param currentInterBrokerPartitionMovementConcurrency The effective allowed inter-broker partition movement concurrency per broker.
   * @param maxPartitionMovementsPerBroker The maximum allowed inter-broker partition movement concurrency per broker.
   * @return {@code null} to indicate recommendation for no change in allowed inter-broker partition movement concurrency.
   * Otherwise an integer to indicate the recommended inter-broker partition movement concurrency.
   */
  static Integer recommendedConcurrency(Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker,
                                        int currentInterBrokerPartitionMovementConcurrency,
                                        int maxPartitionMovementsPerBroker) {
    boolean withinAdjusterLimit = ExecutionUtils.withinConcurrencyAdjusterLimit(currentMetricsByBroker);
    Integer recommendedConcurrency = null;
    if (withinAdjusterLimit) {
      // Additive-increase inter-broker replica reassignment concurrency (MAX: maxPartitionMovementsPerBroker).
      if (currentInterBrokerPartitionMovementConcurrency < maxPartitionMovementsPerBroker) {
        recommendedConcurrency = currentInterBrokerPartitionMovementConcurrency + ADDITIVE_INCREASE_PARAM;
        LOG.info("Concurrency adjuster recommended an increase in inter-broker partition movement concurrency to {}", recommendedConcurrency);
      }
    } else {
      // Multiplicative-decrease inter-broker replica reassignment concurrency (MIN: 1).
      if (currentInterBrokerPartitionMovementConcurrency > 1) {
        recommendedConcurrency = Math.max(1, currentInterBrokerPartitionMovementConcurrency / MULTIPLICATIVE_DECREASE_PARAM);
        LOG.info("Concurrency adjuster recommended a decrease in inter-broker partition movement concurrency to {}", recommendedConcurrency);
      }
    }
    return recommendedConcurrency;
  }

  /**
   * Retrieve the set of {@link TopicPartition partitions} that are currently being reassigned.
   *
   * @param adminClient The adminClient to ask for ongoing partition reassignments.
   * @return The set of {@link TopicPartition partitions} that are being reassigned.
   */
  public static Set<TopicPartition> partitionsBeingReassigned(AdminClient adminClient)
      throws InterruptedException, ExecutionException, TimeoutException {
    return ongoingPartitionReassignments(adminClient).keySet();
  }

  /**
   * Retrieve the map of {@link PartitionReassignment reassignment} by {@link TopicPartition partitions}.
   *
   * If the response times out, this method retries up to {@link #LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS} times.
   * The maximum time to wait for the admin client response is computed as:
   * {@link #LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS} * ({@link #DEFAULT_RETRY_BACKOFF_BASE} ^ {@code attempt}).
   *
   * @param adminClient The adminClient to ask for ongoing partition reassignments.
   * @return The map of {@link PartitionReassignment reassignment} by {@link TopicPartition partitions}.
   */
  public static Map<TopicPartition, PartitionReassignment> ongoingPartitionReassignments(AdminClient adminClient)
      throws InterruptedException, ExecutionException, TimeoutException {
    Map<TopicPartition, PartitionReassignment> partitionReassignments = null;
    int attempts = 0;
    long timeoutMs = LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS;
    do {
      ListPartitionReassignmentsResult responseResult = adminClient.listPartitionReassignments();
      try {
        // A successful response is expected to be non-null.
        partitionReassignments = responseResult.reassignments().get(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        LOG.info("Failed to list partition reassignments in {}ms (attempt={}). Consider increasing the value of {} config.",
                 timeoutMs, attempts + 1, ExecutorConfig.LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS_CONFIG);
        if (++attempts == LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS) {
          throw e;
        }
        timeoutMs *= DEFAULT_RETRY_BACKOFF_BASE;
      }
    } while (partitionReassignments == null);

    return partitionReassignments;
  }

  /**
   * Package private for unit tests.
   * @return A value to indicate a cancelled reassignment for any partition.
   */
  static Optional<NewPartitionReassignment> cancelReassignmentValue() {
    return java.util.Optional.empty();
  }

  private static Optional<NewPartitionReassignment> reassignmentValue(List<Integer> targetReplicas) {
    return Optional.of(new NewPartitionReassignment(targetReplicas));
  }

  /**
   * Submits the given inter-broker replica reassignment tasks for execution using the given admin client.
   *
   * @param adminClient The adminClient to submit new inter-broker replica reassignments.
   * @param tasks Inter-broker replica reassignment tasks to execute.
   * @return The {@link AlterPartitionReassignmentsResult result} of reassignment request -- cannot be {@code null}.
   */
  public static AlterPartitionReassignmentsResult submitReplicaReassignmentTasks(AdminClient adminClient, List<ExecutionTask> tasks) {
    if (tasks == null || tasks.isEmpty()) {
      throw new IllegalArgumentException(String.format("Tasks to execute (%s) cannot be null or empty.", tasks));
    }

    // Update the ongoing replica reassignments in case the task status has changed.
    Map<TopicPartition, Optional<NewPartitionReassignment>> newReassignments = new HashMap<>(tasks.size());
    for (ExecutionTask task : tasks) {
      TopicPartition tp = task.proposal().topicPartition();
      List<Integer> newReplicas = new ArrayList<>(task.proposal().newReplicas().size());
      for (ReplicaPlacementInfo replicaPlacementInfo : task.proposal().newReplicas()) {
        newReplicas.add(replicaPlacementInfo.brokerId());
      }
      switch (task.state()) {
        case ABORTING:
        case ABORTED:
        case DEAD:
          // A task in one of these states should not have a corresponding partition movement -- cancel it.
          newReassignments.put(tp, cancelReassignmentValue());
          LOG.debug("The ongoing reassignment will be cancelled for task {}.", task);
          break;
        case COMPLETED:
          // No action needed.
          LOG.debug("Task {} has already been completed.", task);
          break;
        case IN_PROGRESS:
          // No need to check whether the topic exists or being deleted as the server response will indicate those.
          // Likewise, no need to check if there is already an ongoing execution for the partition, because if one
          // exists, it will be modified to execute the desired task.
          newReassignments.put(tp, reassignmentValue(newReplicas));
          LOG.debug("Task {} will be executed.", task);
          break;
        default:
          throw new IllegalStateException(String.format("Unrecognized task state %s.", task.state()));
      }
    }

    if (newReassignments.isEmpty()) {
      throw new IllegalArgumentException("All tasks submitted for replica reassignment are already completed.");
    }
    return adminClient.alterPartitionReassignments(newReassignments);
  }

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
   * Process the given {@link AlterPartitionReassignmentsResult result} of alterPartitionReassignments request to:
   * <ul>
   *   <li>ensure that the corresponding request has been accepted,</li>
   *   <li>identify the set of partitions that were deleted upon submission of the corresponding inter-broker replica
   *   reassignment tasks and populate the given set</li>
   *   <li>identify the set of partitions that were dead upon submission of the corresponding inter-broker replica
   *   reassignment tasks and populate the given set, and</li>
   *   <li>identify the set of partitions that were not in progress upon submission of the corresponding cancellation/
   *   rollback for the inter-broker replica reassignment tasks and populate the given set</li>
   * </ul>
   *
   * @param result the result of a request to alter partition reassignments, or {@code null} if no new reassignment submitted.
   * @param deleted a set to populate with partitions that were deleted upon submission of the corresponding inter-broker
   *                replica reassignment tasks.
   * @param dead a set to populate with partitions that were dead upon submission of the corresponding inter-broker
   *             replica reassignment tasks.
   * @param noReassignmentToCancel a set to populate with partitions that were not in progress upon submission of the
   *                               corresponding cancellation/rollback for the inter-broker replica reassignment tasks.
   */
  public static void processAlterPartitionReassignmentsResult(AlterPartitionReassignmentsResult result,
                                                              Set<TopicPartition> deleted,
                                                              Set<TopicPartition> dead,
                                                              Set<TopicPartition> noReassignmentToCancel) {
    if (result != null) {
      for (Map.Entry<TopicPartition, KafkaFuture<Void>> entry: result.values().entrySet()) {
        TopicPartition tp = entry.getKey();
        try {
          entry.getValue().get(EXECUTION_TASK_FUTURE_ERROR_VERIFICATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          LOG.debug("Replica reassignment for {} has been accepted.", tp);
        } catch (ExecutionException ee) {
          if (Errors.INVALID_REPLICA_ASSIGNMENT.exception().getClass() == ee.getCause().getClass()) {
            dead.add(tp);
            LOG.debug("Replica reassignment failed for {} due to dead destination broker(s).", tp);
          } else if (Errors.UNKNOWN_TOPIC_OR_PARTITION.exception().getClass() == ee.getCause().getClass()) {
            deleted.add(tp);
            LOG.debug("Replica reassignment failed for {} due to its topic deletion.", tp);
          } else if (Errors.NO_REASSIGNMENT_IN_PROGRESS.exception().getClass() == ee.getCause().getClass()) {
            // Attempt to cancel/rollback a reassignment that does not exist.
            noReassignmentToCancel.add(tp);
            LOG.debug("Rollback failed for {} due to lack of corresponding ongoing replica reassignment.", tp);
          } else {
            // Not expected to happen.
            throw new IllegalStateException(String.format("%s encountered an unknown execution exception.", tp), ee);
          }
        } catch (TimeoutException | InterruptedException e) {
          // May indicate transient (e.g. network) issues and might require a task re-execution -- i.e. handled in Executor.
          // If this is observed frequently, we may need to bump up EXECUTION_TASK_FUTURE_ERROR_VERIFICATION_TIMEOUT_MS.
          LOG.warn("Failed to process AlterPartitionReassignmentsResult of {}.", tp, e);
        }
      }
    }
  }

  /**
   * Deletes zNode for ongoing leadership movement tasks. Then deletes controller zNode to trigger a controller
   * re-election for the cancellation to take effect.
   *
   * This operation has side-effects -- i.e. changes the controller. Note that, the executor adopted PartitionReassignment
   * Kafka API for graceful and instant mechanism to cancel ongoing replica reassignments. Hence, no such side-effects
   * would be incurred to stop ongoing replica reassignments.
   *
   * @param kafkaZkClient KafkaZkClient to use for deleting the relevant zNodes to force stop the execution (if any).
   */
  public static void deleteZNodesToForceStopLeadershipMoves(KafkaZkClient kafkaZkClient) {
    // delete zNode of ongoing leadership movement tasks.
    LOG.info("Deleting zNode for ongoing leadership changes {}.", kafkaZkClient.getPreferredReplicaElection());
    kafkaZkClient.deletePreferredReplicaElection(ZkVersion.MatchAnyVersion());
    // Delete controller zNode to trigger a controller re-election.
    LOG.info("Deleting controller zNode to re-elect a new controller. Old controller is {}.", kafkaZkClient.getControllerId());
    kafkaZkClient.deleteController(ZkVersion.MatchAnyVersion());
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
  static boolean isInterBrokerReplicaActionDone(Cluster cluster, ExecutionTask task) {
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
  static boolean isIntraBrokerReplicaActionDone(Map<ExecutionTask, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> logdirInfoByTask,
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
  static boolean isLeadershipMovementDone(Cluster cluster, ExecutionTask task) {
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
