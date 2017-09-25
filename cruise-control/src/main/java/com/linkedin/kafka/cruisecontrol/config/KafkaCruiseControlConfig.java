/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoals;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.detector.notifier.NoopNotifier;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsReporterSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.DefaultMetricSamplerPartitionAssignor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import java.util.Arrays;
import joptsimple.internal.Strings;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;

/**
 * The configuration class of Kafka Cruise Control.
 */
public class KafkaCruiseControlConfig extends AbstractConfig {
  private static final String DEFAULT_FAILED_BROKERS_ZK_PATH = "/CruiseControlBrokerList";
  // We have to define this so we don't need to move every package to scala src folder.
  private static final String DEFAULT_ANOMALY_NOTIFIER_CLASS = NoopNotifier.class.getName();

  private static final ConfigDef CONFIG;

  // Monitor configs
  /**
   * <code>bootstrap.servers</code>
   */
  public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

  /**
   * <code>metadata.max.age.ms</code>
   */
  public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;
  private static final String METADATA_MAX_AGE_DOC = CommonClientConfigs.METADATA_MAX_AGE_DOC;

  /**
   * <code>client.id</code>
   */
  public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;

  /**
   * <code>send.buffer.bytes</code>
   */
  public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

  /**
   * <code>receive.buffer.bytes</code>
   */
  public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

  /**
   * <code>connections.max.idle.ms</code>
   */
  public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

  /**
   * <code>reconnect.backoff.ms</code>
   */
  public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

  /**
   * <code>request.timeout.ms</code>
   */
  public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
  private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;

  /**
   * <code>num.metric.fetchers</code>
   */
  public static final String NUM_METRIC_FETCHERS_CONFIG = "num.metric.fetchers";
  private static final String NUM_METRIC_FETCHERS_DOC = "The number of metric fetchers to fetch from the Kafka cluster.";

  /**
   * <code>metric.sampler.class</code>
   */
  public static final String METRIC_SAMPLER_CLASS_CONFIG = "metric.sampler.class";
  private static final String METRIC_SAMPLER_CLASS_DOC = "The class name of the metric sampler";

  /**
   * <code>metric.sampler.partition.assignor.class</code>
   */
  public static final String METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS_CONFIG = "metric.sampler.partition.assignor.class";
  private static final String METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS_DOC = "The class used to assign the partitions to " +
      "the metric samplers.";

  /**
   * <code>metric.sampling.interval.ms</code>
   */
  public static final String METRIC_SAMPLING_INTERVAL_MS_CONFIG = "metric.sampling.interval.ms";
  private static final String METRIC_SAMPLING_INTERVAL_MS_DOC = "The interval of metric sampling.";

  /**
   * <code>load.snapshot.interval.ms</code>
   */
  public static final String LOAD_SNAPSHOT_WINDOW_MS_CONFIG = "load.snapshot.window.ms";
  private static final String LOAD_SNAPSHOT_WINDOW_MS_DOC = "The interval in millisecond that is covered by each " +
      "load snapshot. The load snapshot will aggregate all the metric samples whose timestamp fall into its window. " +
      "The load snapshot window must be greater than the metric.sampling.interval.ms";

  /**
   * <code>num.load.snapshots</code>
   */
  public static final String NUM_LOAD_SNAPSHOTS_CONFIG = "num.load.snapshots";
  private static final String NUM_LOAD_SNAPSHOTS_DOC = "The maximum number of load snapshots the load monitor would keep. " +
      "Each snapshot covers a time window defined by load.snapshot.window.ms";

  /**
   * <code>min.samples.per.load.snapshot</code>
   */
  public static final String MIN_SAMPLES_PER_LOAD_SNAPSHOT_CONFIG = "min.samples.per.load.snapshot";
  private static final String MIN_SAMPLES_PER_LOAD_SNAPSHOT_DOC = "The minimum number of metric samples a valid load " +
      "snapshot should have. If a partition does not have enough samples in a snapshot window, the topic of the partition " +
      "will be removed from the snapshot due to in sufficient data.";

  /**
   * <code>broker.capacity.config.resolver.class</code>
   */
  public static final String BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG = "broker.capacity.config.resolver.class";
  private static final String BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_DOC = "The broker capacity configuration resolver "
      + "class name. The broker capacity configuration resolver is responsible for getting the broker capacity. The "
      + "default implementation is a file based solution.";

  /**
   * <code>min.monitored.partition.percentage</code>
   */
  public static final String MIN_MONITORED_PARTITION_PERCENTAGE_CONFIG = "min.monitored.partition.percentage";
  private static final String MIN_MONITORED_PARTITION_PERCENTAGE_DOC = "The minimum percentage of the total partitions " +
      "required to be monitored in order to generate a valid load model. Because the topic and partitions in a " +
      "Kafka cluster are dynamically changing. The load monitor will exclude some of the topics that does not have " +
      "sufficient metric samples. This configuration defines the minimum required percentage of the partitions that " +
      "must be included in the load model.";

  /**
   * <code>leader.network.inbound.weight.for.cpu.util</code>
   */
  public static final String LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG = "leader.network.inbound.weight.for.cpu.util";
  private static final String LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_DOC = "Kafka Cruise Control uses the " +
      "following model to derive replica level CPU utilization: " +
      "REPLICA_CPU_UTIL = a * LEADER_BYTES_IN_RATE + b * LEADER_BYTES_OUT_RATE + c * FOLLOWER_BYTES_IN_RATE." +
      "This configuration will be used as the weight for LEADER_BYTES_IN_RATE.";

  /**
   * <code>leader.network.outbound.weight.for.cpu.util</code>
   */
  public static final String LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG = "leader.network.outbound.weight.for.cpu.util";
  private static final String LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_DOC = "Kafka Cruise Control uses the " +
      "following model to derive replica level CPU utilization: " +
      "REPLICA_CPU_UTIL = a * LEADER_BYTES_IN_RATE + b * LEADER_BYTES_OUT_RATE + c * FOLLOWER_BYTES_IN_RATE." +
      "This configuration will be used as the weight for LEADER_BYTES_OUT_RATE.";

  /**
   * <code>follower.network.inbound.weight.for.cpu.util</code>
   */
  public static final String FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG = "follower.network.inbound.weight.for.cpu.util";
  private static final String FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_DOC = "Kafka Cruise Control uses the " +
      "following model to derive replica level CPU utilization: " +
      "REPLICA_CPU_UTIL = a * LEADER_BYTES_IN_RATE + b * LEADER_BYTES_OUT_RATE + c * FOLLOWER_BYTES_IN_RATE." +
      "This configuration will be used as the weight for FOLLOWER_BYTES_IN_RATE.";

  /**
   * <code>linear.regression.model.cpu.util.bucket.size</code>
   */
  public static final String LINEAR_REGRESSION_MODEL_CPU_UTIL_BUCKET_SIZE_CONFIG = "linear.regression.model.cpu.util.bucket.size";
  private static final String LINEAR_REGRESSION_MODEL_CPU_UTIL_BUCKET_SIZE_DOC = "The CPU utilization bucket size for linear"
      + " regression model training data. The unit is percents.";

  /**
   * <code>linear.regression.model.required.samples.per.bucket</code>
   */
  public static final String LINEAR_REGRESSION_MODEL_REQUIRED_SAMPLES_PER_CPU_UTIL_BUCKET_CONFIG =
      "linear.regression.model.required.samples.per.bucket";
  private static final String LINEAR_REGRESSION_MODEL_REQUIRED_SAMPLES_PER_CPU_UTIL_BUCKET_DOC = "The number of training samples"
      + " required in each CPU utilization bucket specified by linear.regression.model.cpu.util.bucket";

  /**
   * <code>linear.regression.model.min.num.cpu.util.buckets</code>
   */
  public static final String LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS_CONFIG =
      "linear.regression.model.min.num.cpu.util.buckets";
  private static final String LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS_DOC = "The minimum number of full CPU"
      + " utilization buckets required to generate a linear regression model.";

  // Analyzer configs
  /**
   * <code>cpu.balance.threshold</code>
   */
  public static final String CPU_BALANCE_THRESHOLD_CONFIG = "cpu.balance.threshold";
  private static final String CPU_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for CPU utilization. " +
      "For example, 1.10 means the highest CPU usage of a broker should not be above 1.10x of average " +
      "CPU utilization of all the brokers.";

  /**
   * <code>disk.balance.threshold</code>
   */
  public static final String DISK_BALANCE_THRESHOLD_CONFIG = "disk.balance.threshold";
  private static final String DISK_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for disk utilization. " +
      "For example, 1.10 means the highest disk usage of a broker should not be above 1.10x of average " +
      "disk utilization of all the brokers.";

  /**
   * <code>network.inbound.balance.threshold</code>
   */
  public static final String NETWORK_INBOUND_BALANCE_THRESHOLD_CONFIG = "network.inbound.balance.threshold";
  private static final String NETWORK_INBOUND_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for " +
      "network inbound usage. For example, 1.10 means the highest network inbound usage of a broker should not " +
      "be above 1.10x of average network inbound usage of all the brokers.";

  /**
   * <code>network.outbound.balance.threshold</code>
   */
  public static final String NETWORK_OUTBOUND_BALANCE_THRESHOLD_CONFIG = "network.outbound.balance.threshold";
  private static final String NETWORK_OUTBOUND_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for " +
      "network outbound usage. For example, 1.10 means the highest network outbound usage of a broker should not " +
      "be above 1.10x of average network outbound usage of all the brokers.";

  /**
   * <code>cpu.capacity.threshold</code>
   */
  public static final String CPU_CAPACITY_THRESHOLD_CONFIG = "cpu.capacity.threshold";
  private static final String CPU_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total broker.cpu.capacity " +
      "that is allowed to be used on a broker. The analyzer will enforce a hard goal that the cpu utilization " +
      "of a broker cannot be higher than (broker.cpu.capacity * cpu.capacity.threshold).";

  /**
   * <code>cpu.capacity.threshold</code>
   */
  public static final String DISK_CAPACITY_THRESHOLD_CONFIG = "disk.capacity.threshold";
  private static final String DISK_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total broker.disk.capacity " +
      "that is allowed to be used on a broker. The analyzer will enforce a hard goal that the disk usage " +
      "of a broker cannot be higher than (broker.disk.capacity * disk.capacity.threshold).";

  /**
   * <code>network.inbound.capacity.threshold</code>
   */
  public static final String NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG = "network.inbound.capacity.threshold";
  private static final String NETWORK_INBOUND_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total " +
      "broker.network.inbound.capacity that is allowed to be used on a broker. The analyzer will enforce a hard goal " +
      "that the disk usage of a broker cannot be higher than " +
      "(broker.network.inbound.capacity * network.inbound.capacity.threshold).";

  /**
   * <code>network.outbound.capacity.threshold</code>
   */
  public static final String NETWORK_OUTBOUND_CAPACITY_THRESHOLD_CONFIG = "network.outbound.capacity.threshold";
  private static final String NETWORK_OUTBOUND_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total " +
      "broker.network.outbound.capacity that is allowed to be used on a broker. The analyzer will enforce a hard goal " +
      "that the disk usage of a broker cannot be higher than " +
      "(broker.network.outbound.capacity * network.outbound.capacity.threshold).";

  /**
   * <code>cpu.low.utilization.threshold</code>
   */
  public static final String CPU_LOW_UTILIZATION_THRESHOLD_CONFIG = "cpu.low.utilization.threshold";
  private static final String CPU_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold for Kafka Cruise Control to define " +
      "the utilization of CPU is low enough that rebalance is not worthwhile. The cluster will only be in a low " +
      "utilization state when all the brokers are below the low utilization threshold. The threshold is in percentage.";

  /**
   * <code>disk.low.utilization.threshold</code>
   */
  public static final String DISK_LOW_UTILIZATION_THRESHOLD_CONFIG = "disk.low.utilization.threshold";
  private static final String DISK_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold for Kafka Cruise Control to define " +
      "the utilization of DISK is low enough that rebalance is not worthwhile. The cluster will only be in a low " +
      "utilization state when all the brokers are below the low utilization threshold. The threshold is in percentage.";

  /**
   * <code>network.inbound.low.utilization.threshold</code>
   */
  public static final String NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG = "network.inbound.low.utilization.threshold";
  private static final String NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold for Kafka Cruise Control to define " +
      "the utilization of network inbound rate is low enough that rebalance is not worthwhile. The cluster will only be in a low " +
      "utilization state when all the brokers are below the low utilization threshold. The threshold is in percentage.";

  /**
   * <code>network.outbound.low.utilization.threshold</code>
   */
  public static final String NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG = "network.outbound.low.utilization.threshold";
  private static final String NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold for Kafka Cruise Control to define " +
      "the utilization of network outbound rate is low enough that rebalance is not worthwhile. The cluster will only be in a low " +
      "utilization state when all the brokers are below the low utilization threshold. The threshold is in percentage.";

  /**
   * <code>max.proposal.candidates</code>
   */
  public static final String MAX_PROPOSAL_CANDIDATES_CONFIG = "max.proposal.candidates";
  private static final String MAX_PROPOSAL_CANDIDATES_DOC = "Kafka cruise control precomputes the optimization proposal"
      + "candidates continuously in the background. This config sets the maximum number of candidate proposals to "
      + "precompute for each cluster workload model. The more proposal candidates are generated, the more likely a "
      + "better optimization proposal will be found, but more CPU will be used as well.";

  /**
   * <code>proposal.expiration.ms</code>
   */
  public static final String PROPOSAL_EXPIRATION_MS_CONFIG = "proposal.expiration.ms";
  private static final String PROPOSAL_EXPIRATION_MS_DOC = "Kafka cruise control will cache one of the best proposal "
      + "among all the optimization proposal candidates it recently computed. This configuration defines when will the"
      + "cached proposal be invalidated and needs a recomputation. If proposal.expiration.ms is set to 0, cruise control"
      + "will continuously compute the proposal candidates.";

  /**
   * <code>num.proposal.precompute.threads</code>
   */
  public static final String NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG = "num.proposal.precompute.threads";
  private static final String NUM_PROPOSAL_PRECOMPUTE_THREADS_DOC = "The number of thread used to precompute the "
      + "optimization proposal candidates. The more threads are used, the more memory and CPU resource will be used.";

  // Executor configs
  /**
   * <code>zookeeper.connect</code>
   */
  public static final String ZOOKEEPER_CONNECT_CONFIG = "zookeeper.connect";
  private static final String ZOOKEEPER_CONNECT_DOC = "The zookeeper path used by the Kafka cluster.";

  /**
   * <code>num.concurrent.partition.movements</code>
   */
  public static final String NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG =
      "num.concurrent.partition.movements.per.broker";
  private static final String NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_DOC = "The maximum number of partitions " +
      "the executor will move to or out of a broker at the same time. e.g. setting the value to 10 means that the " +
      "executor will at most allow 10 partitions move out of a broker and 10 partitions move into a broker at any " +
      "given point. This is to avoid overwhelming the cluster by partition movements.";

  /**
   * <code>num.concurrent.leader.movements</code>
   */
  public static final String NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG =
      "num.concurrent.leader.movements";
  private static final String NUM_CONCURRENT_LEADER_MOVEMENTS_DOC = "The maximum number of leader " +
      "movements the executor will take as one batch. This is mainly because the ZNode has a 1 MB size upper limit. And it " +
      "will also reduce the controller burden.";

  /**
   * <code>execution.progress.check.interval.ms</code>
   */
  public static final String EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG = "execution.progress.check.interval.ms";
  private static final String EXECUTION_PROGRESS_CHECK_INTERVAL_MS_DOC = "The interval in milliseconds that the " +
      "executor will check on the execution progress.";

  /**
   * <code>goals</code>
   */
  public static final String GOALS_CONFIG = "goals";
  private static final String GOALS_DOC = "A list of goals in the order of priority. The high priority goals will be " +
      "executed first.";

  /**
   * <code>anomaly.notifier.class</code>
   */
  public static final String ANOMALY_NOTIFIER_CLASS_CONFIG = "anomaly.notifier.class";
  private static final String ANOMALY_NOTIFIER_CLASS_DOC = "The notifier class to trigger an alert when an "
      + "anomaly is violated. The anomaly could be either a goal violation or a broker failure.";

  /**
   * <code>anomaly.detection.interval.ms</code>
   */
  public static final String ANOMALY_DETECTION_INTERVAL_MS_CONFIG = "anomaly.detection.interval.ms";
  private static final String ANOMALY_DETECTION_INTERVAL_MS_DOC = "The interval in millisecond that the detectors will "
      + "run to detect the anomalies.";

  /**
   * <code>anomaly.detection.goals</code>
   */
  public static final String ANOMALY_DETECTION_GOALS_CONFIG = "anomaly.detection.goals";
  private static final String ANOMALY_DETECTION_GOALS_DOC = "The goals that anomaly detector should detect if they are"
      + "violated.";

  /**
   * <code>failed.brokers.zk.path</code>
   */
  public static final String FAILED_BROKERS_ZK_PATH_CONFIG = "failed.brokers.zk.path";
  private static final String FAILED_BROKERS_ZK_PATH_DOC = "The zk path to store the failed broker list. This is to "
      + "persist the broker failure time in case Cruise Control failed and restarted when some brokers are down.";

  /**
   * <code>use.linear.regression.model</code>
   */
  public static final String USE_LINEAR_REGRESSION_MODEL_CONFIG = "use.linear.regression.model";
  private static final String USE_LINEAR_REGRESSION_MODEL_DOC = "Use the linear regression model to estimate the "
      + "cpu utilization.";

  /**
   * <code>topics.excluded.from.partition.movement</code>
   */
  public static final String TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG = "topics.excluded.from.partition.movement";
  private static final String TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_DOC = "The topics that should be excluded from the "
      + "partition movement. It is a regex. Notice that this regex will be ignored when decommission a broker is invoked.";


  public static final String SAMPLE_STORE_CLASS_CONFIG = "sample.store.class";
  private static final String SAMPLE_STORE_CLASS_DOC = "The sample store class name. User may configure a sample store "
      + "that persist the metric samples that have already been aggregated into Kafka Cruise Control. Later on the "
      + "persisted samples can be reloaded from the sample store to Kafka Cruise Control.";

  static {
    CONFIG = new ConfigDef()
        .define(BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH,
                CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
        .define(CLIENT_ID_CONFIG, ConfigDef.Type.STRING, "kafka-cruise-control", ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.CLIENT_ID_DOC)
        .define(SEND_BUFFER_CONFIG, ConfigDef.Type.INT, 128 * 1024, atLeast(0), ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.SEND_BUFFER_DOC)
        .define(RECEIVE_BUFFER_CONFIG, ConfigDef.Type.INT, 32 * 1024, atLeast(0), ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.RECEIVE_BUFFER_DOC)
        .define(RECONNECT_BACKOFF_MS_CONFIG, ConfigDef.Type.LONG, 50L, atLeast(0L), ConfigDef.Importance.LOW,
                CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
        .define(METADATA_MAX_AGE_CONFIG, ConfigDef.Type.LONG, 5 * 60 * 1000, atLeast(0), ConfigDef.Importance.LOW,
                METADATA_MAX_AGE_DOC)
        .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                ConfigDef.Type.LONG,
                9 * 60 * 1000,
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
        .define(REQUEST_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                30 * 1000,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                REQUEST_TIMEOUT_MS_DOC)
        .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                ConfigDef.Type.STRING,
                CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.SECURITY_PROTOCOL_DOC)
        .define(NUM_METRIC_FETCHERS_CONFIG,
                ConfigDef.Type.INT,
                1,
                ConfigDef.Importance.HIGH,
                NUM_METRIC_FETCHERS_DOC)
        .define(METRIC_SAMPLER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                CruiseControlMetricsReporterSampler.class.getName(),
                ConfigDef.Importance.HIGH,
                METRIC_SAMPLER_CLASS_DOC)
        .define(METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                DefaultMetricSamplerPartitionAssignor.class.getName(),
                ConfigDef.Importance.LOW,
                METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS_DOC)
        .define(METRIC_SAMPLING_INTERVAL_MS_CONFIG,
                ConfigDef.Type.LONG,
                60 * 1000,
                atLeast(0),
                ConfigDef.Importance.HIGH,
                METRIC_SAMPLING_INTERVAL_MS_DOC)
        .define(LOAD_SNAPSHOT_WINDOW_MS_CONFIG,
                ConfigDef.Type.LONG,
                60 * 60 * 1000,
                atLeast(0),
                ConfigDef.Importance.HIGH,
                LOAD_SNAPSHOT_WINDOW_MS_DOC)
        .define(NUM_LOAD_SNAPSHOTS_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                NUM_LOAD_SNAPSHOTS_DOC)
        .define(MIN_SAMPLES_PER_LOAD_SNAPSHOT_CONFIG,
                ConfigDef.Type.INT,
                3,
                atLeast(1),
                ConfigDef.Importance.LOW,
                MIN_SAMPLES_PER_LOAD_SNAPSHOT_DOC)
        .define(BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                BrokerCapacityConfigFileResolver.class.getName(),
                ConfigDef.Importance.MEDIUM,
                BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_DOC)
        .define(MIN_MONITORED_PARTITION_PERCENTAGE_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.995,
                between(0, 1),
                ConfigDef.Importance.HIGH,
                MIN_MONITORED_PARTITION_PERCENTAGE_DOC)
        .define(LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.6,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_DOC)
        .define(LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.1,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_DOC)
        .define(FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.3,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_DOC)
        .define(LINEAR_REGRESSION_MODEL_CPU_UTIL_BUCKET_SIZE_CONFIG,
                ConfigDef.Type.INT,
                5,
                between(0, 100),
                ConfigDef.Importance.MEDIUM,
                LINEAR_REGRESSION_MODEL_CPU_UTIL_BUCKET_SIZE_DOC)
        .define(LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS_CONFIG,
                ConfigDef.Type.INT,
                5,
                ConfigDef.Importance.MEDIUM,
                LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS_DOC)
        .define(LINEAR_REGRESSION_MODEL_REQUIRED_SAMPLES_PER_CPU_UTIL_BUCKET_CONFIG,
                ConfigDef.Type.INT,
                100,
                atLeast(1),
                ConfigDef.Importance.MEDIUM,
                LINEAR_REGRESSION_MODEL_REQUIRED_SAMPLES_PER_CPU_UTIL_BUCKET_DOC)
        .define(CPU_BALANCE_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                1.10,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                CPU_BALANCE_THRESHOLD_DOC)
        .define(DISK_BALANCE_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                1.10,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                DISK_BALANCE_THRESHOLD_DOC)
        .define(NETWORK_INBOUND_BALANCE_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                1.10,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                NETWORK_INBOUND_BALANCE_THRESHOLD_DOC)
        .define(NETWORK_OUTBOUND_BALANCE_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                1.10,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                NETWORK_OUTBOUND_BALANCE_THRESHOLD_DOC)
        .define(CPU_CAPACITY_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.8,
                between(0, 1),
                ConfigDef.Importance.HIGH,
                CPU_CAPACITY_THRESHOLD_DOC)
        .define(DISK_CAPACITY_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.8,
                between(0, 1),
                ConfigDef.Importance.HIGH,
                DISK_CAPACITY_THRESHOLD_DOC)
        .define(NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.8,
                between(0, 1),
                ConfigDef.Importance.HIGH,
                NETWORK_INBOUND_CAPACITY_THRESHOLD_DOC)
        .define(NETWORK_OUTBOUND_CAPACITY_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.8,
                between(0, 1),
                ConfigDef.Importance.HIGH,
                NETWORK_OUTBOUND_CAPACITY_THRESHOLD_DOC)
        .define(CPU_LOW_UTILIZATION_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.0,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                CPU_LOW_UTILIZATION_THRESHOLD_DOC)
        .define(DISK_LOW_UTILIZATION_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.0,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                DISK_LOW_UTILIZATION_THRESHOLD_DOC)
        .define(NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.0,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_DOC)
        .define(NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.0,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_DOC)
        .define(MAX_PROPOSAL_CANDIDATES_CONFIG,
                ConfigDef.Type.INT,
                10,
                atLeast(1),
                ConfigDef.Importance.MEDIUM,
                MAX_PROPOSAL_CANDIDATES_DOC)
        .define(PROPOSAL_EXPIRATION_MS_CONFIG,
                ConfigDef.Type.LONG,
                900000,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                PROPOSAL_EXPIRATION_MS_DOC)
        .define(NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG,
                ConfigDef.Type.INT,
                1,
                atLeast(1),
                ConfigDef.Importance.LOW,
                NUM_PROPOSAL_PRECOMPUTE_THREADS_DOC)
        .define(ZOOKEEPER_CONNECT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, ZOOKEEPER_CONNECT_DOC)
        .define(NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG,
                ConfigDef.Type.INT,
                10,
                atLeast(1),
                ConfigDef.Importance.MEDIUM,
                NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_DOC)
        .define(NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG,
                ConfigDef.Type.INT,
                1000,
                atLeast(1),
                ConfigDef.Importance.MEDIUM,
                NUM_CONCURRENT_LEADER_MOVEMENTS_DOC)
        .define(EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG,
                ConfigDef.Type.LONG,
                10000L,
                atLeast(0),
                ConfigDef.Importance.LOW,
                EXECUTION_PROGRESS_CHECK_INTERVAL_MS_DOC)
        .define(GOALS_CONFIG,
                ConfigDef.Type.LIST,
                Strings.join(Arrays.asList(RackAwareCapacityGoal.class.getName(),
                                           PotentialNwOutGoal.class.getName(),
                                           DiskUsageDistributionGoal.class.getName(),
                                           NetworkInboundUsageDistributionGoal.class.getName(),
                                           NetworkOutboundUsageDistributionGoal.class.getName(),
                                           CpuUsageDistributionGoal.class.getName(),
                                           LeaderBytesInDistributionGoals.class.getName(),
                                           TopicReplicaDistributionGoal.class.getName(),
                                           ReplicaDistributionGoal.class.getName()), ","),
                ConfigDef.Importance.HIGH,
                GOALS_DOC)
        .define(ANOMALY_NOTIFIER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                DEFAULT_ANOMALY_NOTIFIER_CLASS,
                ConfigDef.Importance.LOW, ANOMALY_NOTIFIER_CLASS_DOC)
        .define(ANOMALY_DETECTION_INTERVAL_MS_CONFIG,
                ConfigDef.Type.LONG,
                300000L,
                ConfigDef.Importance.LOW,
                ANOMALY_DETECTION_INTERVAL_MS_DOC)
        .define(ANOMALY_DETECTION_GOALS_CONFIG,
                ConfigDef.Type.LIST,
                Strings.join(Arrays.asList(RackAwareCapacityGoal.class.getName(),
                                           PotentialNwOutGoal.class.getName(),
                                           DiskUsageDistributionGoal.class.getName(),
                                           NetworkInboundUsageDistributionGoal.class.getName(),
                                           NetworkOutboundUsageDistributionGoal.class.getName(),
                                           CpuUsageDistributionGoal.class.getName(),
                                           LeaderBytesInDistributionGoals.class.getName(),
                                           TopicReplicaDistributionGoal.class.getName(),
                                           ReplicaDistributionGoal.class.getName()), ","),
                ConfigDef.Importance.MEDIUM,
                ANOMALY_DETECTION_GOALS_DOC)
        .define(FAILED_BROKERS_ZK_PATH_CONFIG,
                ConfigDef.Type.STRING,
                DEFAULT_FAILED_BROKERS_ZK_PATH,
                ConfigDef.Importance.LOW, FAILED_BROKERS_ZK_PATH_DOC)
        .define(USE_LINEAR_REGRESSION_MODEL_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.MEDIUM,
                USE_LINEAR_REGRESSION_MODEL_DOC)
        .define(TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.LOW,
                TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_DOC)
        .define(SAMPLE_STORE_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                KafkaSampleStore.class.getName(),
                ConfigDef.Importance.LOW,
                SAMPLE_STORE_CLASS_DOC)
        .withClientSslSupport();
  }

  public KafkaCruiseControlConfig(Map<?, ?> originals) {
    super(CONFIG, originals);
  }

  public KafkaCruiseControlConfig(Map<?, ?> originals, boolean doLog) {
    super(CONFIG, originals, doLog);
  }
}
