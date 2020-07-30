/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import com.linkedin.kafka.cruisecontrol.executor.ExecutorNoopNotifier;
import com.linkedin.kafka.cruisecontrol.executor.strategy.BaseReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeLargeReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeSmallReplicaMovementStrategy;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.ConfigDef;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REASON_PARAM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;


/**
 * A class to keep Cruise Control Executor Configs and defaults.
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public class ExecutorConfig {
  private ExecutorConfig() {

  }

  /**
   * <code>zookeeper.connect</code>
   */
  public static final String ZOOKEEPER_CONNECT_CONFIG = KafkaConfig.ZkConnectProp();
  public static final String ZOOKEEPER_CONNECT_DOC = KafkaConfig.ZkConnectDoc();

  /**
   * <code>zookeeper.security.enabled</code>
   */
  public static final String ZOOKEEPER_SECURITY_ENABLED_CONFIG = "zookeeper.security.enabled";
  public static final boolean DEFAULT_ZOOKEEPER_SECURITY_ENABLED = false;
  public static final String ZOOKEEPER_SECURITY_ENABLED_DOC = "Specify if ZooKeeper is secured, true or false";

  /**
   * <code>num.concurrent.partition.movements.per.broker</code>
   */
  public static final String NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG = "num.concurrent.partition.movements.per.broker";
  public static final int DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER = 5;
  public static final String NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_DOC = "The maximum number of partitions "
      + "the executor will move to or out of a broker at the same time. e.g. setting the value to 10 means that the "
      + "executor will at most allow 10 partitions move out of a broker and 10 partitions move into a broker at any "
      + "given point. This is to avoid overwhelming the cluster by inter-broker partition movements.";

  /**
   * <code>num.concurrent.intra.broker.partition.movements</code>
   */
  public static final String NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG = "num.concurrent.intra.broker.partition.movements";
  public static final int DEFAULT_NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS = 2;
  public static final String NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_DOC = "The maximum number of partitions "
      + "the executor will move across disks within a broker at the same time. e.g. setting the value to 10 means that the "
      + "executor will at most allow 10 partitions to move across disks within a broker at any given point. This is to avoid "
      + "overwhelming the cluster by intra-broker partition movements.";

  /**
   * <code>num.concurrent.leader.movements</code>
   */
  public static final String NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG = "num.concurrent.leader.movements";
  public static final int DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS = 1000;
  public static final String NUM_CONCURRENT_LEADER_MOVEMENTS_DOC = "The maximum number of leader "
      + "movements the executor will take as one batch. This is mainly because the ZNode has a 1 MB size upper limit. And it "
      + "will also reduce the controller burden.";

  /**
   * <code>max.num.cluster.movements</code>
   */
  public static final String MAX_NUM_CLUSTER_MOVEMENTS_CONFIG = "max.num.cluster.movements";
  // Assumption: Each replica move request has a size smaller than 1MB / 1250 = 800 bytes. (1MB = default zNode size limit)
  public static final int DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG = 1250;
  public static final String MAX_NUM_CLUSTER_MOVEMENTS_DOC = "The maximum number of allowed movements (e.g. partition,"
      + " leadership) in cluster. This global limit cannot be exceeded regardless of the per-broker replica movement "
      + "concurrency. When determining this limit, ensure that the (number-of-allowed-movements * maximum-size-of-each-request)"
      + " is smaller than the default zNode size limit.";

  /**
   * <code>default.replication.throttle</code>
   */
  public static final String DEFAULT_REPLICATION_THROTTLE_CONFIG = "default.replication.throttle";
  public static final Long DEFAULT_DEFAULT_REPLICATION_THROTTLE = null;
  public static final String DEFAULT_REPLICATION_THROTTLE_DOC = "The replication throttle applied to replicas being "
      + "moved, in bytes per second.";

  /**
   * <code>replica.movement.strategies</code>
   */
  public static final String REPLICA_MOVEMENT_STRATEGIES_CONFIG = "replica.movement.strategies";
  public static final String DEFAULT_REPLICA_MOVEMENT_STRATEGIES = new StringJoiner(",")
      .add(PostponeUrpReplicaMovementStrategy.class.getName())
      .add(PrioritizeLargeReplicaMovementStrategy.class.getName())
      .add(PrioritizeSmallReplicaMovementStrategy.class.getName())
      .add(BaseReplicaMovementStrategy.class.getName()).toString();
  public static final String REPLICA_MOVEMENT_STRATEGIES_DOC = "A list of supported strategies used to determine execution"
      + " order for generated partition movement tasks.";

  /**
   * <code>default.replica.movement.strategies</code>
   */
  public static final String DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG = "default.replica.movement.strategies";
  public static final String DEFAULT_DEFAULT_REPLICA_MOVEMENT_STRATEGIES = BaseReplicaMovementStrategy.class.getName();
  public static final String DEFAULT_REPLICA_MOVEMENT_STRATEGIES_DOC = "The list of replica movement strategies that will be used "
      + "by default if no replica movement strategy list is provided.";

  /**
   * <code>execution.progress.check.interval.ms</code>
   */
  public static final String EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG = "execution.progress.check.interval.ms";
  public static final long DEFAULT_EXECUTION_PROGRESS_CHECK_INTERVAL_MS = 10000L;
  public static final String EXECUTION_PROGRESS_CHECK_INTERVAL_MS_DOC = "The interval in milliseconds that the executor "
      + "will check on the execution progress.";
  /*
   * <code>executor.notifier.class</code>
   */
  public static final String EXECUTOR_NOTIFIER_CLASS_CONFIG = "executor.notifier.class";
  public static final String DEFAULT_EXECUTOR_NOTIFIER_CLASS = ExecutorNoopNotifier.class.getName();
  public static final String EXECUTOR_NOTIFIER_CLASS_DOC = "The executor notifier class to trigger an alert when an "
      + "execution finishes or is stopped (by a user or by Cruise Control).";

  /**
   * <code>leader.movement.timeout.ms</code>
   */
  public static final String LEADER_MOVEMENT_TIMEOUT_MS_CONFIG = "leader.movement.timeout.ms";
  public static final long DEFAULT_LEADER_MOVEMENT_TIMEOUT_MS = 180000L;
  public static final String LEADER_MOVEMENT_TIMEOUT_MS_DOC = "The maximum time to wait for a leader movement to finish. "
      + "A leader movement will be marked as failed if it takes longer than this time to finish.";

  /**
   * <code>task.execution.alerting.threshold.ms</code>
   */
  public static final String TASK_EXECUTION_ALERTING_THRESHOLD_MS_CONFIG = "task.execution.alerting.threshold.ms";
  public static final long DEFAULT_TASK_EXECUTION_ALERTING_THRESHOLD_MS = 90000L;

  /**
   * <code>inter.broker.replica.movement.rate.alerting.threshold</code>
   */
  public static final String INTER_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD_CONFIG =
      "inter.broker.replica.movement.rate.alerting.threshold";
  public static final double DEFAULT_INTER_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD = 0.1;
  public static final String INTER_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD_DOC = "Threshold of data movement rate (in MB/s) "
      + "for inter-broker replica movement task. If the task's data movement rate is lower than this and the task's execution time exceeds "
      + "the threshold set via " + TASK_EXECUTION_ALERTING_THRESHOLD_MS_CONFIG + ", an alert will be sent via notifier set by config "
      + EXECUTOR_NOTIFIER_CLASS_CONFIG;

  /**
   * <code>intra.broker.replica.movement.rate.alerting.threshold</code>
   */
  public static final String INTRA_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD_CONFIG =
      "intra.broker.replica.movement.rate.alerting.threshold";
  public static final double DEFAULT_INTRA_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD = 0.2;
  public static final String INTRA_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD_DOC = "Threshold of data movement rate (in MB/s) "
      + "for intra-broker replica movement task. If task's data movement rate is lower than this and the task's execution time exceeds "
      + "the threshold set via " + TASK_EXECUTION_ALERTING_THRESHOLD_MS_CONFIG + ", an alert will be sent via notifier set by config "
      + EXECUTOR_NOTIFIER_CLASS_CONFIG;

  // Put here to avoid forward reference.
  public static final String TASK_EXECUTION_ALERTING_THRESHOLD_MS_DOC = "Threshold of execution time to alert a replica/leader movement"
      + " task. If the task's execution time exceeds this threshold and the data movement rate is lower than the threshold "
      + "set via " + INTER_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD_CONFIG + " (for inter-broker replica movement task) or "
      + INTRA_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD_CONFIG + " (for intra-broker replica movement task), an alert will be "
      + "sent via notifier set by config " + EXECUTOR_NOTIFIER_CLASS_CONFIG;

  /**
   * <code>request.reason.required</code>
   */
  public static final String REQUEST_REASON_REQUIRED_CONFIG = "request.reason.required";
  public static final boolean DEFAULT_REQUEST_REASON_REQUIRED = false;
  public static final String REQUEST_REASON_REQUIRED_DOC = "Require specifying reason via " + REASON_PARAM + " parameter "
      + "for non-dryrun rebalance/add_broker/remove_broker/demote_broker/fix_offline_replicas/topic_configuration request.";

  /**
   * <code>logdir.response.timeout.ms</code>
   */
  public static final String LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG = "logdir.response.timeout.ms";
  public static final long DEFAULT_LOGDIR_RESPONSE_TIMEOUT_MS = 10000L;
  public static final String LOGDIR_RESPONSE_TIMEOUT_MS_DOC = "Timeout in ms for broker logdir to respond";

  /**
   * <code>demotion.history.retention.time.ms</code>
   */
  public static final String DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG = "demotion.history.retention.time.ms";
  public static final long DEFAULT_DEMOTION_HISTORY_RETENTION_TIME_MS = TimeUnit.HOURS.toMillis(336);
  public static final String DEMOTION_HISTORY_RETENTION_TIME_MS_DOC = "The maximum time in milliseconds to retain the"
      + " demotion history of brokers.";

  /**
   * <code>removal.history.retention.time.ms</code>
   */
  public static final String REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG = "removal.history.retention.time.ms";
  public static final long DEFAULT_REMOVAL_HISTORY_RETENTION_TIME_MS = TimeUnit.HOURS.toMillis(336);
  public static final String REMOVAL_HISTORY_RETENTION_TIME_MS_DOC = "The maximum time in milliseconds to retain the"
      + " removal history of brokers.";

  /**
   * <code>concurrency.adjuster.interval.ms</code>
   */
  public static final String CONCURRENCY_ADJUSTER_INTERVAL_MS_CONFIG = "concurrency.adjuster.interval.ms";
  public static final long DEFAULT_CONCURRENCY_ADJUSTER_INTERVAL_MS = 360000L;
  public static final String CONCURRENCY_ADJUSTER_INTERVAL_MS_DOC = "The interval of concurrency auto adjustment for"
      + " inter-broker partition movements.";

  /**
   * <code>concurrency.adjuster.max.partition.movements.per.broker</code>
   */
  public static final String CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG =
      "concurrency.adjuster.max.partition.movements.per.broker";
  public static final int DEFAULT_CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER = 12;
  public static final String CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_DOC = "The maximum number of "
      + "partitions the concurrency auto adjustment will allow the executor to move in or out of a broker at the same time. "
      + "It enforces a cap on the maximum concurrent inter-broker partition movements to avoid overwhelming the cluster. It "
      + "must be greater than num.concurrent.partition.movements.per.broker and not more than max.num.cluster.movements.";

  /**
   * <code>concurrency.adjuster.enabled</code>
   */
  public static final String CONCURRENCY_ADJUSTER_ENABLED_CONFIG = "concurrency.adjuster.enabled";
  public static final boolean DEFAULT_CONCURRENCY_ADJUSTER_ENABLED = false;
  public static final String CONCURRENCY_ADJUSTER_ENABLED_DOC = "The flag to indicate whether the concurrency of "
      + "inter-broker partition movements will be auto-adjusted based on dynamically changing broker metrics.";

  /**
   * <code>concurrency.adjuster.limit.log.flush.time.ms</code>
   */
  public static final String CONCURRENCY_ADJUSTER_LIMIT_LOG_FLUSH_TIME_MS_CONFIG = "concurrency.adjuster.limit.log.flush.time.ms";
  public static final double DEFAULT_CONCURRENCY_ADJUSTER_LIMIT_LOG_FLUSH_TIME_MS = 2000.0;
  public static final String CONCURRENCY_ADJUSTER_LIMIT_LOG_FLUSH_TIME_MS_DOC = "The limit on the 99.9th percentile broker metric"
      + " value of log flush time. If any broker exceeds this limit during an ongoing inter-broker partition reassignment, the "
      + "concurrency adjuster (if enabled) attempts to decrease the number of allowed concurrent inter-broker partition movements.";

  /**
   * <code>concurrency.adjuster.limit.follower.fetch.local.time.ms</code>
   */
  public static final String CONCURRENCY_ADJUSTER_LIMIT_FOLLOWER_FETCH_LOCAL_TIME_MS_CONFIG
      = "concurrency.adjuster.limit.follower.fetch.local.time.ms";
  public static final double DEFAULT_CONCURRENCY_ADJUSTER_LIMIT_FOLLOWER_FETCH_LOCAL_TIME_MS = 500.0;
  public static final String CONCURRENCY_ADJUSTER_LIMIT_FOLLOWER_FETCH_LOCAL_TIME_MS_DOC = "The limit on the 99.9th percentile broker metric"
      + " value of follower fetch local time. If any broker exceeds this limit during an ongoing inter-broker partition reassignment, the "
      + "concurrency adjuster (if enabled) attempts to decrease the number of allowed concurrent inter-broker partition movements.";

  /**
   * <code>concurrency.adjuster.limit.produce.local.time.ms</code>
   */
  public static final String CONCURRENCY_ADJUSTER_LIMIT_PRODUCE_LOCAL_TIME_MS_CONFIG = "concurrency.adjuster.limit.produce.local.time.ms";
  public static final double DEFAULT_CONCURRENCY_ADJUSTER_LIMIT_PRODUCE_LOCAL_TIME_MS = 1000.0;
  public static final String CONCURRENCY_ADJUSTER_LIMIT_PRODUCE_LOCAL_TIME_MS_DOC = "The limit on the 99.9th percentile broker metric"
      + " value of produce local time. If any broker exceeds this limit during an ongoing inter-broker partition reassignment, the "
      + "concurrency adjuster (if enabled) attempts to decrease the number of allowed concurrent inter-broker partition movements.";

  /**
   * <code>concurrency.adjuster.limit.consumer.fetch.local.time.ms</code>
   */
  public static final String CONCURRENCY_ADJUSTER_LIMIT_CONSUMER_FETCH_LOCAL_TIME_MS_CONFIG
      = "concurrency.adjuster.limit.consumer.fetch.local.time.ms";
  public static final double DEFAULT_CONCURRENCY_ADJUSTER_LIMIT_CONSUMER_FETCH_LOCAL_TIME_MS = 500.0;
  public static final String CONCURRENCY_ADJUSTER_LIMIT_CONSUMER_FETCH_LOCAL_TIME_MS_DOC = "The limit on the 99.9th percentile broker metric"
      + " value of consumer fetch local time. If any broker exceeds this limit during an ongoing inter-broker partition reassignment, the "
      + "concurrency adjuster (if enabled) attempts to decrease the number of allowed concurrent inter-broker partition movements.";

  /**
   * <code>concurrency.adjuster.limit.request.queue.size</code>
   */
  public static final String CONCURRENCY_ADJUSTER_LIMIT_REQUEST_QUEUE_SIZE_CONFIG = "concurrency.adjuster.limit.request.queue.size";
  public static final double DEFAULT_CONCURRENCY_ADJUSTER_LIMIT_REQUEST_QUEUE_SIZE = 1000.0;
  public static final String CONCURRENCY_ADJUSTER_LIMIT_REQUEST_QUEUE_SIZE_DOC = "The limit on the broker metric value of request "
      + "queue size. If any broker exceeds this limit during an ongoing inter-broker partition reassignment, the concurrency adjuster"
      + " (if enabled) attempts to decrease the number of allowed concurrent inter-broker partition movements.";

  /**
   * Define configs for Executor.
   *
   * @param configDef Config definition.
   * @return The given ConfigDef after defining the configs for Executor.
   */
  public static ConfigDef define(ConfigDef configDef) {
    return configDef.define(ZOOKEEPER_CONNECT_CONFIG,
                            ConfigDef.Type.STRING,
                            ConfigDef.Importance.HIGH,
                            ZOOKEEPER_CONNECT_DOC)
                    .define(ZOOKEEPER_SECURITY_ENABLED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_ZOOKEEPER_SECURITY_ENABLED,
                            ConfigDef.Importance.HIGH,
                            ZOOKEEPER_SECURITY_ENABLED_DOC)
                    .define(NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER,
                            atLeast(1),
                            ConfigDef.Importance.MEDIUM,
                            NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_DOC)
                    .define(NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS,
                            atLeast(1),
                            ConfigDef.Importance.MEDIUM,
                            NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_DOC)
                    .define(NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS,
                            atLeast(1),
                            ConfigDef.Importance.MEDIUM,
                            NUM_CONCURRENT_LEADER_MOVEMENTS_DOC)
                    .define(MAX_NUM_CLUSTER_MOVEMENTS_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG,
                            atLeast(5),
                            ConfigDef.Importance.MEDIUM,
                            MAX_NUM_CLUSTER_MOVEMENTS_DOC)
                    .define(DEFAULT_REPLICATION_THROTTLE_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_DEFAULT_REPLICATION_THROTTLE,
                            ConfigDef.Importance.MEDIUM,
                            DEFAULT_REPLICATION_THROTTLE_DOC)
                    .define(REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_REPLICA_MOVEMENT_STRATEGIES,
                            ConfigDef.Importance.MEDIUM,
                            REPLICA_MOVEMENT_STRATEGIES_DOC)
                    .define(DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_DEFAULT_REPLICA_MOVEMENT_STRATEGIES,
                            ConfigDef.Importance.MEDIUM,
                            DEFAULT_REPLICA_MOVEMENT_STRATEGIES_DOC)
                    .define(EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_EXECUTION_PROGRESS_CHECK_INTERVAL_MS,
                            atLeast(0),
                            ConfigDef.Importance.LOW,
                            EXECUTION_PROGRESS_CHECK_INTERVAL_MS_DOC)
                    .define(EXECUTOR_NOTIFIER_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_EXECUTOR_NOTIFIER_CLASS,
                            ConfigDef.Importance.LOW,
                            EXECUTOR_NOTIFIER_CLASS_DOC)
                    .define(LEADER_MOVEMENT_TIMEOUT_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_LEADER_MOVEMENT_TIMEOUT_MS,
                            ConfigDef.Importance.LOW,
                            LEADER_MOVEMENT_TIMEOUT_MS_DOC)
                    .define(TASK_EXECUTION_ALERTING_THRESHOLD_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_TASK_EXECUTION_ALERTING_THRESHOLD_MS,
                            ConfigDef.Importance.LOW,
                            TASK_EXECUTION_ALERTING_THRESHOLD_MS_DOC)
                    .define(INTER_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_INTER_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD,
                            ConfigDef.Importance.LOW,
                            INTER_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD_DOC)
                    .define(INTRA_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_INTRA_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD,
                            ConfigDef.Importance.LOW,
                            INTRA_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD_DOC)
                    .define(REQUEST_REASON_REQUIRED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_REQUEST_REASON_REQUIRED,
                            ConfigDef.Importance.MEDIUM,
                            REQUEST_REASON_REQUIRED_DOC)
                    .define(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_LOGDIR_RESPONSE_TIMEOUT_MS,
                            ConfigDef.Importance.LOW,
                            LOGDIR_RESPONSE_TIMEOUT_MS_DOC)
                    .define(DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_DEMOTION_HISTORY_RETENTION_TIME_MS,
                            atLeast(0),
                            ConfigDef.Importance.MEDIUM,
                            DEMOTION_HISTORY_RETENTION_TIME_MS_DOC)
                    .define(REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_REMOVAL_HISTORY_RETENTION_TIME_MS,
                            atLeast(0),
                            ConfigDef.Importance.MEDIUM,
                            REMOVAL_HISTORY_RETENTION_TIME_MS_DOC)
                    .define(CONCURRENCY_ADJUSTER_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_CONCURRENCY_ADJUSTER_INTERVAL_MS,
                            atLeast(0),
                            ConfigDef.Importance.LOW,
                            CONCURRENCY_ADJUSTER_INTERVAL_MS_DOC)
                    .define(CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER,
                            atLeast(1),
                            ConfigDef.Importance.MEDIUM,
                            CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_DOC)
                    .define(CONCURRENCY_ADJUSTER_ENABLED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_CONCURRENCY_ADJUSTER_ENABLED,
                            ConfigDef.Importance.HIGH,
                            CONCURRENCY_ADJUSTER_ENABLED_DOC)
                    .define(CONCURRENCY_ADJUSTER_LIMIT_LOG_FLUSH_TIME_MS_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_CONCURRENCY_ADJUSTER_LIMIT_LOG_FLUSH_TIME_MS,
                            atLeast(10.0),
                            ConfigDef.Importance.MEDIUM,
                            CONCURRENCY_ADJUSTER_LIMIT_LOG_FLUSH_TIME_MS_DOC)
                    .define(CONCURRENCY_ADJUSTER_LIMIT_FOLLOWER_FETCH_LOCAL_TIME_MS_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_CONCURRENCY_ADJUSTER_LIMIT_FOLLOWER_FETCH_LOCAL_TIME_MS,
                            atLeast(10.0),
                            ConfigDef.Importance.MEDIUM,
                            CONCURRENCY_ADJUSTER_LIMIT_FOLLOWER_FETCH_LOCAL_TIME_MS_DOC)
                    .define(CONCURRENCY_ADJUSTER_LIMIT_PRODUCE_LOCAL_TIME_MS_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_CONCURRENCY_ADJUSTER_LIMIT_PRODUCE_LOCAL_TIME_MS,
                            atLeast(10.0),
                            ConfigDef.Importance.MEDIUM,
                            CONCURRENCY_ADJUSTER_LIMIT_PRODUCE_LOCAL_TIME_MS_DOC)
                    .define(CONCURRENCY_ADJUSTER_LIMIT_CONSUMER_FETCH_LOCAL_TIME_MS_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_CONCURRENCY_ADJUSTER_LIMIT_CONSUMER_FETCH_LOCAL_TIME_MS,
                            atLeast(10.0),
                            ConfigDef.Importance.MEDIUM,
                            CONCURRENCY_ADJUSTER_LIMIT_CONSUMER_FETCH_LOCAL_TIME_MS_DOC)
                    .define(CONCURRENCY_ADJUSTER_LIMIT_REQUEST_QUEUE_SIZE_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_CONCURRENCY_ADJUSTER_LIMIT_REQUEST_QUEUE_SIZE,
                            atLeast(10.0),
                            ConfigDef.Importance.MEDIUM,
                            CONCURRENCY_ADJUSTER_LIMIT_REQUEST_QUEUE_SIZE_DOC);
  }
}
