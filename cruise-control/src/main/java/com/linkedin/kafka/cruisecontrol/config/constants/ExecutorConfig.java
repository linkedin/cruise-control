/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import com.linkedin.kafka.cruisecontrol.executor.ExecutorNoopNotifier;
import com.linkedin.kafka.cruisecontrol.executor.strategy.BaseReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeLargeReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeMinIsrWithOfflineReplicasStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeOneAboveMinIsrWithOfflineReplicasStrategy;
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
public final class ExecutorConfig {

  /**
   * <code>zookeeper.connect</code>
   */
  public static final String ZOOKEEPER_CONNECT_CONFIG = KafkaConfig.ZkConnectProp();
  public static final String DEFAULT_ZOOKEEPER_CONNECT = null;
  public static final String ZOOKEEPER_CONNECT_DOC = KafkaConfig.ZkConnectDoc();

  /**
   * <code>zookeeper.security.enabled</code>
   */
  public static final String ZOOKEEPER_SECURITY_ENABLED_CONFIG = "zookeeper.security.enabled";
  public static final boolean DEFAULT_ZOOKEEPER_SECURITY_ENABLED = false;
  public static final String ZOOKEEPER_SECURITY_ENABLED_DOC = "Specify if ZooKeeper is secured, true or false";

  /**
   * <code>zookeeper.ssl.client.enable</code>
   */
  public static final String ZOOKEEPER_SSL_CLIENT_ENABLE_CONFIG = KafkaConfig.ZkSslClientEnableProp();
  public static final boolean DEFAULT_ZOOKEEPER_SSL_CLIENT_ENABLE = false;
  public static final String ZOOKEEPER_SSL_CLIENT_ENABLE_DOC = KafkaConfig.ZkSslClientEnableDoc();

  /**
   * <code>zookeeper.clientCnxnSocket</code>
   */
  public static final String ZOOKEEPER_SSL_CNXN_SOCKET_CONFIG = KafkaConfig.ZkClientCnxnSocketProp();
  public static final String DEFAULT_ZOOKEEPER_SSL_CNXN_SOCKET = null;
  public static final String ZOOKEEPER_SSL_CNXN_SOCKET_DOC = KafkaConfig.ZkClientCnxnSocketDoc();

  /**
   * <code>zookeeper.ssl.keystore.location</code>
   */
  public static final String ZOOKEEPER_SSL_KEYSTORE_LOCATION_CONFIG = KafkaConfig.ZkSslKeyStoreLocationProp();
  public static final String DEFAULT_ZOOKEEPER_SSL_KEYSTORE_LOCATION = null;
  public static final String ZOOKEEPER_SSL_KEYSTORE_LOCATION_DOC = KafkaConfig.ZkSslKeyStoreLocationDoc();

  /**
   * <code>zookeeper.ssl.keystore.password</code>
   */
  public static final String ZOOKEEPER_SSL_KEYSTORE_PASSWORD_CONFIG = KafkaConfig.ZkSslKeyStorePasswordProp();
  public static final String DEFAULT_ZOOKEEPER_SSL_KEYSTORE_PASSWORD = null;
  public static final String ZOOKEEPER_SSL_KEYSTORE_PASSWORD_DOC = KafkaConfig.ZkSslKeyStorePasswordDoc();

  /**
   * <code>zookeeper.ssl.keystore.type</code>
   */
  public static final String ZOOKEEPER_SSL_KEYSTORE_TYPE_CONFIG = KafkaConfig.ZkSslKeyStoreTypeProp();
  public static final String DEFAULT_ZOOKEEPER_SSL_KEYSTORE_TYPE = null;
  public static final String ZOOKEEPER_SSL_KEYSTORE_TYPE_DOC = KafkaConfig.ZkSslKeyStoreTypeDoc();

  /**
   * <code>zookeeper.ssl.truststore.location</code>
   */
  public static final String ZOOKEEPER_SSL_TRUSTSTORE_LOCATION_CONFIG = KafkaConfig.ZkSslTrustStoreLocationProp();
  public static final String DEFAULT_ZOOKEEPER_SSL_TRUSTSTORE_LOCATION = null;
  public static final String ZOOKEEPER_SSL_TRUSTSTORE_LOCATION_DOC = KafkaConfig.ZkSslTrustStoreLocationDoc();

  /**
   * <code>zookeeper.ssl.truststore.password</code>
   */
  public static final String ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD_CONFIG = KafkaConfig.ZkSslTrustStorePasswordProp();
  public static final String DEFAULT_ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD = null;
  public static final String ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD_DOC = KafkaConfig.ZkSslTrustStorePasswordDoc();

  /**
   * <code>zookeeper.ssl.truststore.type</code>
   */
  public static final String ZOOKEEPER_SSL_TRUSTSTORE_TYPE_CONFIG = KafkaConfig.ZkSslTrustStoreTypeProp();
  public static final String DEFAULT_ZOOKEEPER_SSL_TRUSTSTORE_TYPE = null;
  public static final String ZOOKEEPER_SSL_TRUSTSTORE_TYPE_DOC = KafkaConfig.ZkSslTrustStoreTypeDoc();

  /**
   * <code>zookeeper.ssl.protocol</code>
   */
  public static final String ZOOKEEPER_SSL_PROTOCOL_CONFIG = KafkaConfig.ZkSslProtocolProp();
  public static final String DEFAULT_ZOOKEEPER_SSL_PROTOCOL = "TLSv1.2";
  public static final String ZOOKEEPER_SSL_PROTOCOL_DOC = KafkaConfig.ZkSslProtocolDoc();

  /**
   * <code>zookeeper.ssl.enabled.protocols</code>
   */
  public static final String ZOOKEEPER_SSL_ENABLED_PROTOCOLS_CONFIG = KafkaConfig.ZkSslEnabledProtocolsProp();
  public static final String DEFAULT_ZOOKEEPER_SSL_ENABLED_PROTOCOLS = null;
  public static final String ZOOKEEPER_SSL_ENABLED_PROTOCOLS_DOC = KafkaConfig.ZkSslEnabledProtocolsDoc();

  /**
   * <code>zookeeper.ssl.cipher.suites</code>
   */
  public static final String ZOOKEEPER_SSL_CIPHER_SUITES_CONFIG = KafkaConfig.ZkSslCipherSuitesProp();
  public static final String DEFAULT_ZOOKEEPER_SSL_CIPHER_SUITES = null;
  public static final String ZOOKEEPER_SSL_CIPHER_SUITES_DOC = KafkaConfig.ZkSslCipherSuitesDoc();

  /**
   * <code>zookeeper.ssl.endpoint.identification.algorithm</code>
   */
  public static final String ZOOKEEPER_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG = KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp();
  public static final String DEFAULT_ZOOKEEPER_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = "HTTPS";
  public static final String ZOOKEEPER_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC = KafkaConfig.ZkSslEndpointIdentificationAlgorithmDoc();

  /**
   * <code>zookeeper.ssl.crl.enable</code>
   */
  public static final String ZOOKEEPER_SSL_CRL_ENABLE_CONFIG = KafkaConfig.ZkSslCrlEnableProp();
  public static final boolean DEFAULT_ZOOKEEPER_SSL_CRL_ENABLE = false;
  public static final String ZOOKEEPER_SSL_CRL_ENABLE_DOC = KafkaConfig.ZkSslCrlEnableDoc();

  /**
   * <code>zookeeper.ssl.ocsp.enable</code>
   */
  public static final String ZOOKEEPER_SSL_OCSP_ENABLE_CONFIG = KafkaConfig.ZkSslOcspEnableProp();
  public static final boolean DEFAULT_ZOOKEEPER_SSL_OCSP_ENABLE = false;
  public static final String ZOOKEEPER_SSL_OCSP_ENABLE_DOC = KafkaConfig.ZkSslOcspEnableDoc();

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
   * <code>max.num.cluster.partition.movements</code>
   */
  public static final String MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG = "max.num.cluster.partition.movements";
  // Keeping smaller than allowed cluster movements
  public static final int DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG = 1250;
  public static final String MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_DOC = "The maximum number of allowed partition movements in the cluster."
      + " This global limit cannot be exceeded regardless of the per-broker replica movement "
      + "concurrency. While max.num.cluster.movements corresponds to the default zNode size limit, "
      + "max.num.cluster.partition.movements throttles the maximum partition movements across the cluster";

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
      .add(PrioritizeMinIsrWithOfflineReplicasStrategy.class.getName())
      .add(PrioritizeOneAboveMinIsrWithOfflineReplicasStrategy.class.getName())
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
  public static final long DEFAULT_EXECUTION_PROGRESS_CHECK_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);
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
   * <code>admin.client.request.timeout.ms</code>
   */
  public static final String ADMIN_CLIENT_REQUEST_TIMEOUT_MS_CONFIG = "admin.client.request.timeout.ms";
  public static final int DEFAULT_ADMIN_CLIENT_REQUEST_TIMEOUT_MS = (int) TimeUnit.MINUTES.toMillis(3);
  public static final String ADMIN_CLIENT_REQUEST_TIMEOUT_MS_DOC = "The maximum time to wait for the response of an AdminClient request to "
      + "Kafka server. If the response is not received before this timeout elapses the admin client will fail the request.";

  /**
   * <code>leader.movement.timeout.ms</code>
   */
  public static final String LEADER_MOVEMENT_TIMEOUT_MS_CONFIG = "leader.movement.timeout.ms";
  public static final long DEFAULT_LEADER_MOVEMENT_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(3);
  public static final String LEADER_MOVEMENT_TIMEOUT_MS_DOC = "The maximum time to wait for a leader movement to finish. "
      + "A leader movement will be marked as failed if it takes longer than this time to finish.";

  /**
   * <code>task.execution.alerting.threshold.ms</code>
   */
  public static final String TASK_EXECUTION_ALERTING_THRESHOLD_MS_CONFIG = "task.execution.alerting.threshold.ms";
  public static final long DEFAULT_TASK_EXECUTION_ALERTING_THRESHOLD_MS = TimeUnit.SECONDS.toMillis(90);

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
  public static final long DEFAULT_LOGDIR_RESPONSE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
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
  public static final long DEFAULT_CONCURRENCY_ADJUSTER_INTERVAL_MS = TimeUnit.MINUTES.toMillis(6);
  public static final String CONCURRENCY_ADJUSTER_INTERVAL_MS_DOC = "The interval of concurrency auto adjustment.";

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
   * <code>concurrency.adjuster.max.leadership.movements</code>
   */
  public static final String CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG = "concurrency.adjuster.max.leadership.movements";
  public static final int DEFAULT_CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS = 1100;
  public static final String CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_DOC = "The maximum number of leadership movements "
      + "the concurrency auto adjustment will allow the executor to perform in one batch to avoid overwhelming the cluster. "
      + "It cannot be (1) smaller than num.concurrent.leader.movements and (2) greater than max.num.cluster.movements.";

  /**
   * <code>concurrency.adjuster.min.partition.movements.per.broker</code>
   */
  public static final String CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG =
      "concurrency.adjuster.min.partition.movements.per.broker";
  public static final int DEFAULT_CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER = 1;
  public static final String CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_DOC = "The minimum number of "
      + "partitions the concurrency auto adjustment will allow the executor to move in or out of a broker at the same time. "
      + "It enforces a cap on the minimum concurrent inter-broker partition movements to avoid an unacceptable execution pace."
      + " It cannot be greater than num.concurrent.partition.movements.per.broker.";

  /**
   * <code>concurrency.adjuster.min.leadership.movements</code>
   */
  public static final String CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG = "concurrency.adjuster.min.leadership.movements";
  public static final int DEFAULT_CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS = 100;
  public static final String CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_DOC = "The minimum number of leadership movements "
      + "the concurrency auto adjustment will allow the executor to perform in one batch to avoid an unacceptable execution pace."
      + " It cannot be greater than num.concurrent.leader.movements.";

  /**
   * <code>concurrency.adjuster.inter.broker.replica.enabled</code>
   */
  public static final String CONCURRENCY_ADJUSTER_INTER_BROKER_REPLICA_ENABLED_CONFIG = "concurrency.adjuster.inter.broker.replica.enabled";
  public static final boolean DEFAULT_CONCURRENCY_ADJUSTER_INTER_BROKER_REPLICA_ENABLED = false;
  public static final String CONCURRENCY_ADJUSTER_INTER_BROKER_REPLICA_ENABLED_DOC = "Enable concurrency adjuster for "
      + "inter-broker replica reassignments.";

  /**
   * <code>concurrency.adjuster.leadership.enabled</code>
   */
  public static final String CONCURRENCY_ADJUSTER_LEADERSHIP_ENABLED_CONFIG = "concurrency.adjuster.leadership.enabled";
  public static final boolean DEFAULT_CONCURRENCY_ADJUSTER_LEADERSHIP_ENABLED = false;
  public static final String CONCURRENCY_ADJUSTER_LEADERSHIP_ENABLED_DOC = "Enable concurrency adjuster for leadership reassignments.";

  /**
   * <code>concurrency.adjuster.limit.log.flush.time.ms</code>
   */
  public static final String CONCURRENCY_ADJUSTER_LIMIT_LOG_FLUSH_TIME_MS_CONFIG = "concurrency.adjuster.limit.log.flush.time.ms";
  public static final double DEFAULT_CONCURRENCY_ADJUSTER_LIMIT_LOG_FLUSH_TIME_MS = TimeUnit.SECONDS.toMillis(2);
  public static final String CONCURRENCY_ADJUSTER_LIMIT_LOG_FLUSH_TIME_MS_DOC = "The limit on the 99.9th percentile broker metric"
      + " value of log flush time. If any broker exceeds this limit during an ongoing reassignment, the relevant concurrency "
      + "adjuster (if enabled) attempts to decrease the number of allowed concurrent movements.";

  /**
   * <code>concurrency.adjuster.limit.follower.fetch.local.time.ms</code>
   */
  public static final String CONCURRENCY_ADJUSTER_LIMIT_FOLLOWER_FETCH_LOCAL_TIME_MS_CONFIG
      = "concurrency.adjuster.limit.follower.fetch.local.time.ms";
  public static final double DEFAULT_CONCURRENCY_ADJUSTER_LIMIT_FOLLOWER_FETCH_LOCAL_TIME_MS = 500.0;
  public static final String CONCURRENCY_ADJUSTER_LIMIT_FOLLOWER_FETCH_LOCAL_TIME_MS_DOC = "The limit on the 99.9th percentile broker metric"
      + " value of follower fetch local time. If any broker exceeds this limit during an ongoing reassignment, the relevant concurrency "
      + "adjuster (if enabled) attempts to decrease the number of allowed concurrent movements.";

  /**
   * <code>concurrency.adjuster.limit.produce.local.time.ms</code>
   */
  public static final String CONCURRENCY_ADJUSTER_LIMIT_PRODUCE_LOCAL_TIME_MS_CONFIG = "concurrency.adjuster.limit.produce.local.time.ms";
  public static final double DEFAULT_CONCURRENCY_ADJUSTER_LIMIT_PRODUCE_LOCAL_TIME_MS = (double) TimeUnit.SECONDS.toMillis(1);
  public static final String CONCURRENCY_ADJUSTER_LIMIT_PRODUCE_LOCAL_TIME_MS_DOC = "The limit on the 99.9th percentile broker metric"
      + " value of produce local time. If any broker exceeds this limit during an ongoing reassignment, the relevant concurrency "
      + "adjuster (if enabled) attempts to decrease the number of allowed concurrent movements.";

  /**
   * <code>concurrency.adjuster.limit.consumer.fetch.local.time.ms</code>
   */
  public static final String CONCURRENCY_ADJUSTER_LIMIT_CONSUMER_FETCH_LOCAL_TIME_MS_CONFIG
      = "concurrency.adjuster.limit.consumer.fetch.local.time.ms";
  public static final double DEFAULT_CONCURRENCY_ADJUSTER_LIMIT_CONSUMER_FETCH_LOCAL_TIME_MS = 500.0;
  public static final String CONCURRENCY_ADJUSTER_LIMIT_CONSUMER_FETCH_LOCAL_TIME_MS_DOC = "The limit on the 99.9th percentile broker metric"
      + " value of consumer fetch local time. If any broker exceeds this limit during an ongoing reassignment, the relevant concurrency "
      + "adjuster (if enabled) attempts to decrease the number of allowed concurrent movements.";

  /**
   * <code>concurrency.adjuster.limit.request.queue.size</code>
   */
  public static final String CONCURRENCY_ADJUSTER_LIMIT_REQUEST_QUEUE_SIZE_CONFIG = "concurrency.adjuster.limit.request.queue.size";
  public static final double DEFAULT_CONCURRENCY_ADJUSTER_LIMIT_REQUEST_QUEUE_SIZE = 1000.0;
  public static final String CONCURRENCY_ADJUSTER_LIMIT_REQUEST_QUEUE_SIZE_DOC = "The limit on the broker metric value of request "
      + "queue size. If any broker exceeds this limit during an ongoing reassignment, the relevant concurrency adjuster"
      + " (if enabled) attempts to decrease the number of allowed concurrent movements.";

  /**
   * <code>concurrency.adjuster.additive.increase.inter.broker.replica</code>
   */
  public static final String CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_INTER_BROKER_REPLICA_CONFIG
      = "concurrency.adjuster.additive.increase.inter.broker.replica";
  public static final int DEFAULT_CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_INTER_BROKER_REPLICA = 1;
  public static final String CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_INTER_BROKER_REPLICA_DOC = "The fixed number by which the "
      + "concurrency cap on inter-broker replica movements will be increased by the concurrency adjuster (if enabled) when all "
      + "considered metrics are within the concurrency adjuster limit.";

  /**
   * <code>concurrency.adjuster.additive.increase.leadership</code>
   */
  public static final String CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_LEADERSHIP_CONFIG
      = "concurrency.adjuster.additive.increase.leadership";
  public static final int DEFAULT_CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_LEADERSHIP = 100;
  public static final String CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_LEADERSHIP_DOC = "The fixed number by which the "
      + "concurrency cap on leadership movements will be increased by the concurrency adjuster (if enabled) when all "
      + "considered metrics are within the concurrency adjuster limit.";

  /**
   * <code>concurrency.adjuster.multiplicative.decrease.inter.broker.replica</code>
   */
  public static final String CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_INTER_BROKER_REPLICA_CONFIG
      = "concurrency.adjuster.multiplicative.decrease.inter.broker.replica";
  public static final int DEFAULT_CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_INTER_BROKER_REPLICA = 2;
  public static final String CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_INTER_BROKER_REPLICA_DOC = "The fixed number by which the "
      + "concurrency cap on inter-broker replica movements will be divided by the concurrency adjuster (if enabled) when any "
      + "considered metric exceeds the concurrency adjuster limit.";

  /**
   * <code>concurrency.adjuster.multiplicative.decrease.leadership</code>
   */
  public static final String CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_LEADERSHIP_CONFIG
      = "concurrency.adjuster.multiplicative.decrease.leadership";
  public static final int DEFAULT_CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_LEADERSHIP = 2;
  public static final String CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_LEADERSHIP_DOC = "The fixed number by which the "
      + "concurrency cap on leadership movements will be divided by the concurrency adjuster (if enabled) when any "
      + "considered metric exceeds the concurrency adjuster limit.";

  /**
   * <code>list.partition.reassignment.timeout.ms</code>
   */
  public static final String LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS_CONFIG = "list.partition.reassignment.timeout.ms";
  public static final long DEFAULT_LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS = 60000L;
  public static final String LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS_DOC = "The maximum time to wait for the initial response of "
      + "an Admin#listPartitionReassignments() request to be available.";

  /**
   * <code>list.partition.reassignment.max.attempts</code>
   */
  public static final String LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS_CONFIG = "list.partition.reassignment.max.attempts";
  public static final int DEFAULT_LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS = 3;
  public static final String LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS_DOC = "The maximum number of attempts to get an available"
      + " response for an Admin#listPartitionReassignments() request in case of a timeout. Each attempt recalculates the allowed"
      + " timeout using: list-partition-reassignments-timeout-for-the-initial-response * (base-backoff ^ attempt).";

  /**
   * <code>min.execution.progress.check.interval.ms</code>
   */
  public static final String MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG = "min.execution.progress.check.interval.ms";
  public static final long DEFAULT_MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5);
  public static final String MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_DOC = "The minimum execution progress check interval that users "
      + "can dynamically set the execution progress check interval to.";

  /**
   * <code>slow.task.alerting.backoff.ms</code>
   */
  public static final String SLOW_TASK_ALERTING_BACKOFF_TIME_MS_CONFIG = "slow.task.alerting.backoff.ms";
  public static final long DEFAULT_SLOW_TASK_ALERTING_BACKOFF_TIME_MS = TimeUnit.MINUTES.toMillis(1);
  public static final String SLOW_TASK_ALERTING_BACKOFF_TIME_MS_DOC = "The minimum interval between slow task alerts. This backoff "
      + "helps bundling slow tasks to report rather than individually reporting them upon detection.";

  /**
   * <code>concurrency.adjuster.num.min.isr.check</code>
   */
  public static final String CONCURRENCY_ADJUSTER_NUM_MIN_ISR_CHECK_CONFIG = "concurrency.adjuster.num.min.isr.check";
  public static final int DEFAULT_CONCURRENCY_ADJUSTER_NUM_MIN_ISR_CHECK = 5;
  public static final String CONCURRENCY_ADJUSTER_NUM_MIN_ISR_CHECK_DOC = "The number of times that (At/Under)MinISR status of partitions in "
      + "the cluster will be checked during each concurrency auto adjustment interval. For example, if the concurrency auto adjustment interval "
      + "is 6 minutes and this config is 5, then (At/Under)MinISR status of partitions in the cluster will be checked once in every 72 seconds.";

  /**
   * <code>concurrency.adjuster.min.isr.check.enabled</code>
   */
  public static final String CONCURRENCY_ADJUSTER_MIN_ISR_CHECK_ENABLED_CONFIG = "concurrency.adjuster.min.isr.check.enabled";
  public static final boolean DEFAULT_CONCURRENCY_ADJUSTER_MIN_ISR_CHECK_ENABLED = false;
  public static final String CONCURRENCY_ADJUSTER_MIN_ISR_CHECK_ENABLED_DOC = "Enable concurrency adjustment based on (At/Under)MinISR status"
      + " of partitions. This check is in addition to the metric-based concurrency adjustment and is relevant only if concurrency adjuster "
      + "itself is enabled.";

  /**
   * <code>concurrency.adjuster.min.isr.cache.size</code>
   */
  public static final String CONCURRENCY_ADJUSTER_MIN_ISR_CACHE_SIZE_CONFIG = "concurrency.adjuster.min.isr.cache.size";
  public static final int DEFAULT_CONCURRENCY_ADJUSTER_MIN_ISR_CACHE_SIZE = 200000;
  public static final String CONCURRENCY_ADJUSTER_MIN_ISR_CACHE_SIZE_DOC = "The concurrency adjuster is enabled based on (At/Under)MinISR "
      + "status of partitions, it caches the min.insync.replicas of topics for fast query. This configuration configures the maximum number"
      + " of cache slot to maintain.";

  /**
   * <code>concurrency.adjuster.min.isr.retention.ms</code>
   */
  public static final String CONCURRENCY_ADJUSTER_MIN_ISR_RETENTION_MS_CONFIG = "concurrency.adjuster.min.isr.retention.ms";
  public static final long DEFAULT_CONCURRENCY_ADJUSTER_MIN_ISR_RETENTION_MS = TimeUnit.HOURS.toMillis(12);
  public static final String CONCURRENCY_ADJUSTER_MIN_ISR_RETENTION_MS_DOC = "The maximum time in ms to cache min.insync.replicas of topics."
      + " Relevant only if concurrency adjuster is enabled based on (At/Under)MinISR status of partitions.";

  /**
   * <code>auto.stop.external.agent</code>
   */
  public static final String AUTO_STOP_EXTERNAL_AGENT_CONFIG = "auto.stop.external.agent";
  public static final boolean DEFAULT_AUTO_STOP_EXTERNAL_AGENT = true;
  public static final String AUTO_STOP_EXTERNAL_AGENT_DOC = "When starting a new proposal execution while external agent is reassigning partitions,"
      + " automatically stop the external agent and start the execution."
      + " Set to false to keep the external agent reassignment and skip starting the execution.";
  private ExecutorConfig() {
  }

  /**
   * Define configs for Executor.
   *
   * @param configDef Config definition.
   * @return The given ConfigDef after defining the configs for Executor.
   */
  public static ConfigDef define(ConfigDef configDef) {
    return configDef.define(ZOOKEEPER_CONNECT_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_ZOOKEEPER_CONNECT,
                            ConfigDef.Importance.HIGH,
                            ZOOKEEPER_CONNECT_DOC)
                    .define(ZOOKEEPER_SECURITY_ENABLED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_ZOOKEEPER_SECURITY_ENABLED,
                            ConfigDef.Importance.HIGH,
                            ZOOKEEPER_SECURITY_ENABLED_DOC)
                    .define(ZOOKEEPER_SSL_CLIENT_ENABLE_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_ZOOKEEPER_SSL_CLIENT_ENABLE,
                            ConfigDef.Importance.MEDIUM,
                            ZOOKEEPER_SSL_CLIENT_ENABLE_DOC)
                    .define(ZOOKEEPER_SSL_CNXN_SOCKET_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_ZOOKEEPER_SSL_CNXN_SOCKET,
                            ConfigDef.Importance.MEDIUM,
                            ZOOKEEPER_SSL_CNXN_SOCKET_DOC)
                    .define(ZOOKEEPER_SSL_KEYSTORE_LOCATION_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_ZOOKEEPER_SSL_KEYSTORE_LOCATION,
                            ConfigDef.Importance.MEDIUM,
                            ZOOKEEPER_SSL_KEYSTORE_LOCATION_DOC)
                    .define(ZOOKEEPER_SSL_KEYSTORE_PASSWORD_CONFIG,
                            ConfigDef.Type.PASSWORD,
                            DEFAULT_ZOOKEEPER_SSL_KEYSTORE_PASSWORD,
                            ConfigDef.Importance.MEDIUM,
                            ZOOKEEPER_SSL_KEYSTORE_PASSWORD_DOC)
                    .define(ZOOKEEPER_SSL_KEYSTORE_TYPE_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_ZOOKEEPER_SSL_KEYSTORE_TYPE,
                            ConfigDef.Importance.MEDIUM,
                            ZOOKEEPER_SSL_KEYSTORE_TYPE_DOC)
                    .define(ZOOKEEPER_SSL_TRUSTSTORE_LOCATION_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_ZOOKEEPER_SSL_TRUSTSTORE_LOCATION,
                            ConfigDef.Importance.MEDIUM,
                            ZOOKEEPER_SSL_TRUSTSTORE_LOCATION_DOC)
                    .define(ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD_CONFIG,
                            ConfigDef.Type.PASSWORD,
                            DEFAULT_ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD,
                            ConfigDef.Importance.MEDIUM,
                            ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD_DOC)
                    .define(ZOOKEEPER_SSL_TRUSTSTORE_TYPE_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_ZOOKEEPER_SSL_TRUSTSTORE_TYPE,
                            ConfigDef.Importance.MEDIUM,
                            ZOOKEEPER_SSL_TRUSTSTORE_TYPE_DOC)
                    .define(ZOOKEEPER_SSL_PROTOCOL_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_ZOOKEEPER_SSL_PROTOCOL,
                            ConfigDef.Importance.LOW,
                            ZOOKEEPER_SSL_PROTOCOL_DOC)
                    .define(ZOOKEEPER_SSL_ENABLED_PROTOCOLS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_ZOOKEEPER_SSL_ENABLED_PROTOCOLS,
                            ConfigDef.Importance.LOW,
                            ZOOKEEPER_SSL_ENABLED_PROTOCOLS_DOC)
                    .define(ZOOKEEPER_SSL_CIPHER_SUITES_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_ZOOKEEPER_SSL_CIPHER_SUITES,
                            ConfigDef.Importance.LOW,
                            ZOOKEEPER_SSL_CIPHER_SUITES_DOC)
                    .define(ZOOKEEPER_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_ZOOKEEPER_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM,
                            ConfigDef.Importance.LOW,
                            ZOOKEEPER_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
                    .define(ZOOKEEPER_SSL_CRL_ENABLE_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_ZOOKEEPER_SSL_CRL_ENABLE,
                            ConfigDef.Importance.LOW,
                            ZOOKEEPER_SSL_CRL_ENABLE_DOC)
                    .define(ZOOKEEPER_SSL_OCSP_ENABLE_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_ZOOKEEPER_SSL_OCSP_ENABLE,
                            ConfigDef.Importance.LOW,
                            ZOOKEEPER_SSL_OCSP_ENABLE_DOC)
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
                    .define(MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG,
                            atLeast(1),
                            ConfigDef.Importance.MEDIUM,
                            MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_DOC)
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
                    .define(ADMIN_CLIENT_REQUEST_TIMEOUT_MS_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_ADMIN_CLIENT_REQUEST_TIMEOUT_MS,
                            atLeast(0),
                            ConfigDef.Importance.MEDIUM,
                            ADMIN_CLIENT_REQUEST_TIMEOUT_MS_DOC)
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
                    .define(CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS,
                            atLeast(1),
                            ConfigDef.Importance.LOW,
                            CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_DOC)
                    .define(CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER,
                            atLeast(1),
                            ConfigDef.Importance.LOW,
                            CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_DOC)
                    .define(CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS,
                            atLeast(1),
                            ConfigDef.Importance.LOW,
                            CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_DOC)
                    .define(CONCURRENCY_ADJUSTER_INTER_BROKER_REPLICA_ENABLED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_CONCURRENCY_ADJUSTER_INTER_BROKER_REPLICA_ENABLED,
                            ConfigDef.Importance.HIGH,
                            CONCURRENCY_ADJUSTER_INTER_BROKER_REPLICA_ENABLED_DOC)
                    .define(CONCURRENCY_ADJUSTER_LEADERSHIP_ENABLED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_CONCURRENCY_ADJUSTER_LEADERSHIP_ENABLED,
                            ConfigDef.Importance.HIGH,
                            CONCURRENCY_ADJUSTER_LEADERSHIP_ENABLED_DOC)
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
                            CONCURRENCY_ADJUSTER_LIMIT_REQUEST_QUEUE_SIZE_DOC)
                    .define(CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_INTER_BROKER_REPLICA_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_INTER_BROKER_REPLICA,
                            atLeast(1),
                            ConfigDef.Importance.LOW,
                            CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_INTER_BROKER_REPLICA_DOC)
                    .define(CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_LEADERSHIP_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_LEADERSHIP,
                            atLeast(1),
                            ConfigDef.Importance.LOW,
                            CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_LEADERSHIP_DOC)
                    .define(CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_INTER_BROKER_REPLICA_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_INTER_BROKER_REPLICA,
                            atLeast(2),
                            ConfigDef.Importance.LOW,
                            CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_INTER_BROKER_REPLICA_DOC)
                    .define(CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_LEADERSHIP_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_LEADERSHIP,
                            atLeast(2),
                            ConfigDef.Importance.LOW,
                            CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_LEADERSHIP_DOC)
                    .define(LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS,
                            atLeast(1),
                            ConfigDef.Importance.LOW,
                            LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS_DOC)
                    .define(LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS,
                            atLeast(1),
                            ConfigDef.Importance.LOW,
                            LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS_DOC)
                    .define(MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS,
                            atLeast(1),
                            ConfigDef.Importance.MEDIUM,
                            MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_DOC)
                    .define(SLOW_TASK_ALERTING_BACKOFF_TIME_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_SLOW_TASK_ALERTING_BACKOFF_TIME_MS,
                            atLeast(0),
                            ConfigDef.Importance.MEDIUM,
                            SLOW_TASK_ALERTING_BACKOFF_TIME_MS_DOC)
                    .define(CONCURRENCY_ADJUSTER_NUM_MIN_ISR_CHECK_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_CONCURRENCY_ADJUSTER_NUM_MIN_ISR_CHECK,
                            atLeast(1),
                            ConfigDef.Importance.MEDIUM,
                            CONCURRENCY_ADJUSTER_NUM_MIN_ISR_CHECK_DOC)
                    .define(CONCURRENCY_ADJUSTER_MIN_ISR_CHECK_ENABLED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_CONCURRENCY_ADJUSTER_MIN_ISR_CHECK_ENABLED,
                            ConfigDef.Importance.HIGH,
                            CONCURRENCY_ADJUSTER_MIN_ISR_CHECK_ENABLED_DOC)
                    .define(CONCURRENCY_ADJUSTER_MIN_ISR_CACHE_SIZE_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_CONCURRENCY_ADJUSTER_MIN_ISR_CACHE_SIZE,
                            atLeast(1),
                            ConfigDef.Importance.LOW,
                            CONCURRENCY_ADJUSTER_MIN_ISR_CACHE_SIZE_DOC)
                    .define(CONCURRENCY_ADJUSTER_MIN_ISR_RETENTION_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_CONCURRENCY_ADJUSTER_MIN_ISR_RETENTION_MS,
                            atLeast(1),
                            ConfigDef.Importance.LOW,
                            CONCURRENCY_ADJUSTER_MIN_ISR_RETENTION_MS_DOC)
                    .define(AUTO_STOP_EXTERNAL_AGENT_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_AUTO_STOP_EXTERNAL_AGENT,
                            ConfigDef.Importance.MEDIUM,
                            AUTO_STOP_EXTERNAL_AGENT_DOC);
  }
}
