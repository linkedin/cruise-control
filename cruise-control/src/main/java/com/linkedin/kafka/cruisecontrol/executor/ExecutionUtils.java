/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_LOG_FLUSH_TIME_MS_999TH;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_999TH;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_REQUEST_QUEUE_SIZE;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH;


public final class ExecutionUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionUtils.class);
  public static final long METADATA_REFRESH_BACKOFF = 100L;
  public static final long METADATA_EXPIRY_MS = Long.MAX_VALUE;
  // A special timestamp to indicate that a broker is a permanent part of recently removed or demoted broker set.
  public static final long PERMANENT_TIMESTAMP = 0L;
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
  static final Map<ConcurrencyType, Integer> ADDITIVE_INCREASE = new HashMap<>(ConcurrencyType.cachedValues().size());
  static final Map<ConcurrencyType, Integer> MULTIPLICATIVE_DECREASE = new HashMap<>(ConcurrencyType.cachedValues().size());
  static final Map<ConcurrencyType, Integer> MAX_CONCURRENCY = new HashMap<>(ConcurrencyType.cachedValues().size());
  static final Map<ConcurrencyType, Integer> MIN_CONCURRENCY = new HashMap<>(ConcurrencyType.cachedValues().size());
  static final Map<String, Double> CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME = new HashMap<>(5);


  private ExecutionUtils() { }

  /**
   * Initialize the concurrency adjuster limits.
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
    ADDITIVE_INCREASE.put(ConcurrencyType.INTER_BROKER_REPLICA,
                          config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_INTER_BROKER_REPLICA_CONFIG));
    ADDITIVE_INCREASE.put(ConcurrencyType.LEADERSHIP,
                          config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_LEADERSHIP_CONFIG));
    MULTIPLICATIVE_DECREASE.put(ConcurrencyType.INTER_BROKER_REPLICA,
                                config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_INTER_BROKER_REPLICA_CONFIG));
    MULTIPLICATIVE_DECREASE.put(ConcurrencyType.LEADERSHIP,
                                config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_LEADERSHIP_CONFIG));
    MAX_CONCURRENCY.put(ConcurrencyType.INTER_BROKER_REPLICA,
                        config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG));
    MAX_CONCURRENCY.put(ConcurrencyType.LEADERSHIP,
                        config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG));
    MIN_CONCURRENCY.put(ConcurrencyType.INTER_BROKER_REPLICA,
                        config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG));
    MIN_CONCURRENCY.put(ConcurrencyType.LEADERSHIP,
                        config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG));
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
   * Provide a recommended concurrency for the ongoing movements of the given concurrency type using selected current broker
   * metrics and based on additive-increase/multiplicative-decrease (AIMD) feedback control algorithm.
   *
   * @param currentMetricsByBroker Current metrics by broker.
   * @param currentMovementConcurrency The effective allowed movement concurrency.
   * @param concurrencyType The type of concurrency for which the recommendation is requested.
   * @return {@code null} to indicate recommendation for no change in allowed movement concurrency.
   * Otherwise an integer to indicate the recommended movement concurrency.
   */
  static Integer recommendedConcurrency(Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker,
                                        int currentMovementConcurrency,
                                        ConcurrencyType concurrencyType) {
    boolean withinAdjusterLimit = withinConcurrencyAdjusterLimit(currentMetricsByBroker);
    Integer recommendedConcurrency = null;
    if (withinAdjusterLimit) {
      int maxMovementsConcurrency = MAX_CONCURRENCY.get(concurrencyType);
      // Additive-increase reassignment concurrency (MAX: maxMovementsConcurrency).
      if (currentMovementConcurrency < maxMovementsConcurrency) {
        recommendedConcurrency = Math.min(maxMovementsConcurrency, currentMovementConcurrency + ADDITIVE_INCREASE.get(concurrencyType));
        LOG.info("Concurrency adjuster recommended an increase in {} movement concurrency to {}", concurrencyType, recommendedConcurrency);
      }
    } else {
      int minMovementsConcurrency = MIN_CONCURRENCY.get(concurrencyType);
      // Multiplicative-decrease reassignment concurrency (MIN: minMovementsConcurrency).
      if (currentMovementConcurrency > minMovementsConcurrency) {
        recommendedConcurrency = Math.max(minMovementsConcurrency, currentMovementConcurrency / MULTIPLICATIVE_DECREASE.get(concurrencyType));
        LOG.info("Concurrency adjuster recommended a decrease in {} movement concurrency to {}", concurrencyType, recommendedConcurrency);
      }
    }
    return recommendedConcurrency;
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
   *   <li>{@link ExecutionTaskState#IN_PROGRESS}: when the current replica list is the same as the new replica list
   *   and all replicas are in-sync.</li>
   *   <li>{@link ExecutionTaskState#ABORTING}: done when the current replica list is the same as the old replica list.
   *   Due to race condition, we also consider it done if the current replica list is the same as the new replica list
   *   and all replicas are in-sync.</li>
   *   <li>{@link ExecutionTaskState#DEAD}: always considered as done because we neither move forward or rollback.</li>
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
   *   <li>{@link ExecutionTaskState#IN_PROGRESS}: Done when either (1) the proposed leader becomes the leader, (2) the
   *   partition has no leader in the cluster (e.g. deleted or offline), or (3) the partition has another leader and the
   *   proposed leader is out of ISR.</li>
   *   <li>{@link ExecutionTaskState#ABORTING} or {@link ExecutionTaskState#DEAD}: Always considered as done. The
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
