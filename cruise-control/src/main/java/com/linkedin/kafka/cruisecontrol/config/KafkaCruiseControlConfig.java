/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.KafkaNetworkClientProvider;
import com.linkedin.kafka.cruisecontrol.detector.NoopMetricAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.detector.notifier.NoopNotifier;
import com.linkedin.kafka.cruisecontrol.executor.strategy.BaseReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeLargeReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeSmallReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsReporterSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.DefaultMetricSamplerPartitionAssignor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import org.apache.kafka.common.config.ConfigException;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;

/**
 * The configuration class of Kafka Cruise Control.
 */
public class KafkaCruiseControlConfig extends AbstractConfig {
  private static final String DEFAULT_FAILED_BROKERS_ZK_PATH = "/CruiseControlBrokerList";
  // We have to define this so we don't need to move every package to scala src folder.
  private static final String DEFAULT_ANOMALY_NOTIFIER_CLASS = NoopNotifier.class.getName();
  // We have to define this to support the use of network clients with different Kafka client versions.
  private static final String DEFAULT_NETWORK_CLIENT_PROVIDER_CLASS = KafkaNetworkClientProvider.class.getName();
  private static final String DEFAULT_METRIC_ANOMALY_FINDER_CLASS = NoopMetricAnomalyFinder.class.getName();

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
   * <code>partition.metrics.windows.ms</code>
   */
  public static final String PARTITION_METRICS_WINDOW_MS_CONFIG = "partition.metrics.window.ms";
  private static final String PARTITION_METRICS_WINDOW_MS_DOC = "The size of the window in milliseconds to aggregate "
      + "the Kafka partition metrics.";

  /**
   * <code>num.partition.metrics.windows</code>
   */
  public static final String NUM_PARTITION_METRICS_WINDOWS_CONFIG = "num.partition.metrics.windows";
  private static final String NUM_PARTITION_METRICS_WINDOWS_DOC = "The total number of windows to keep for partition "
      + "metric samples";

  /**
   * <code>min.samples.per.partition.metrics.window</code>
   */
  public static final String MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG = "min.samples.per.partition.metrics.window";
  private static final String MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_DOC = "The minimum number of "
      + "PartitionMetricSamples needed to make a partition metrics window valid without extrapolation.";

  /**
   * <code>max.allowed.extrapolations.per.partition</code>
   */
  public static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION_CONFIG = "max.allowed.extrapolations.per.partition";
  private static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION_DOC = "The maximum allowed number of extrapolations "
      + "for each partition. A partition will be considered as invalid if the total number extrapolations in all the "
      + "windows goes above this number.";

  /**
   * <code>partition.metric.sample.aggregator.completeness.cache.size</code>
   */
  public static final String PARTITION_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG =
      "partition.metric.sample.aggregator.completeness.cache.size";
  private static final String PARTITION_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_DOC = "The metric sample "
      + "aggregator caches the completeness metadata for fast query. The completeness describes the confidence "
      + "level of the data in the metric sample aggregator. It is primarily measured by the validity of the metrics"
      + "samples in different windows. This configuration configures The number of completeness cache slots to "
      + "maintain.";

  /**
   * <code>broker.metrics.window.ms</code>
   */
  public static final String BROKER_METRICS_WINDOW_MS_CONFIG = "broker.metrics.window.ms";
  private static final String BROKER_METRICS_WINDOW_MS_DOC = "The size of the window in milliseconds to aggregate the"
      + " Kafka broker metrics.";

  /**
   * <code>num.broker.metrics.windows</code>
   */
  public static final String NUM_BROKER_METRICS_WINDOWS_CONFIG = "num.broker.metrics.windows";
  private static final String NUM_BROKER_METRICS_WINDOWS_DOC = "The total number of windows to keep for broker metric"
      + " samples";

  /**
   * <code>min.samples.per.broker.metrics.window</code>
   */
  public static final String MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_CONFIG = "min.samples.per.broker.metrics.window";
  private static final String MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_DOC = "The minimum number of BrokerMetricSamples "
      + "needed to make a broker metrics window valid without extrapolation.";

  /**
   * <code>max.allowed.extrapolations.per.broker</code>
   */
  public static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER_CONFIG = "max.allowed.extrapolations.per.broker";
  private static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER_DOC = "The maximum allowed number of extrapolations "
      + "for each broker. A broker will be considered as invalid if the total number extrapolations in all the windows"
      + " goes above this number.";

  /**
   * <code>broker.metric.sample.aggregator.completeness.cache.size</code>
   */
  public static final String BROKER_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG =
      "broker.metric.sample.aggregator.completeness.cache.size";
  private static final String BROKER_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_DOC = "The metric sample "
      + "aggregator caches the completeness metadata for fast query. The completeness describes the confidence "
      + "level of the data in the metric sample aggregator. It is primarily measured by the validity of the metrics"
      + "samples in different windows. This configuration configures The number of completeness cache slots to "
      + "maintain.";

  /**
   * <code>num.metric.fetchers</code>
   */
  public static final String NUM_METRIC_FETCHERS_CONFIG = "num.metric.fetchers";
  private static final String NUM_METRIC_FETCHERS_DOC = "The number of metric fetchers to fetch from the Kafka cluster.";

  /**
   * <code>num.cached.recent.anomaly.states</code>
   */
  public static final String NUM_CACHED_RECENT_ANOMALY_STATES_CONFIG = "num.cached.recent.anomaly.states";
  public static final String NUM_CACHED_RECENT_ANOMALY_STATES_DOC = "The number of recent anomaly states cached for "
      + "different anomaly types presented via the anomaly substate response of the state endpoint.";

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
   * <code>broker.capacity.config.resolver.class</code>
   */
  public static final String BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG = "broker.capacity.config.resolver.class";
  private static final String BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_DOC = "The broker capacity configuration resolver "
      + "class name. The broker capacity configuration resolver is responsible for getting the broker capacity. The "
      + "default implementation is a file based solution.";

  /**
   * <code>min.monitored.partition.percentage</code>
   */
  public static final String MIN_VALID_PARTITION_RATIO_CONFIG = "min.valid.partition.ratio";
  private static final String MIN_VALID_PARTITION_RATIO_DOC = "The minimum percentage of the total partitions " +
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
   * <code>replica.count.balance.threshold</code>
   */
  public static final String REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG = "replica.count.balance.threshold";
  private static final String REPLICA_COUNT_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for replica "
      + "distribution. For example, 1.10 means the highest replica count of a broker should not be above 1.10x of "
      + "average replica count of all brokers.";

  /**
   * <code>topic.replica.count.balance.threshold</code>
   */
  public static final String TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG = "topic.replica.count.balance.threshold";
  private static final String TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for "
      + "replica distribution from each topic. For example, 1.80 means the highest topic replica count of a broker "
      + "should not be above 1.80x of average replica count of all brokers for the same topic.";

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
   * <code>metric.anomaly.finder.class</code>
   */
  public static final String METRIC_ANOMALY_FINDER_CLASSES_CONFIG = "metric.anomaly.finder.class";
  private static final String METRIC_ANOMALY_FINDER_CLASSES_DOC = "A list of metric anomaly finder classes to find "
                                                                    + "the current state to identify metric anomalies.";

  /**
   * <code>proposal.expiration.ms</code>
   */
  public static final String PROPOSAL_EXPIRATION_MS_CONFIG = "proposal.expiration.ms";
  private static final String PROPOSAL_EXPIRATION_MS_DOC = "Kafka Cruise Control will cache one of the best proposal "
      + "among all the optimization proposal candidates it recently computed. This configuration defines when will the"
      + "cached proposal be invalidated and needs a recomputation. If proposal.expiration.ms is set to 0, Cruise Control"
      + "will continuously compute the proposal candidates.";

  /**
   * <code>max.replicas.per.broker</code>
   */
  public static final String MAX_REPLICAS_PER_BROKER_CONFIG = "max.replicas.per.broker";
  private static final String MAX_REPLICAS_PER_BROKER_DOC = "The maximum number of replicas allowed to reside on a "
      + "broker. The analyzer will enforce a hard goal that the number of replica on a broker cannot be higher than "
      + "this config.";

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
   * <code>num.concurrent.partition.movements.per.broker</code>
   */
  public static final String NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG = "num.concurrent.partition.movements.per.broker";
  private static final String NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_DOC = "The maximum number of partitions " +
      "the executor will move to or out of a broker at the same time. e.g. setting the value to 10 means that the " +
      "executor will at most allow 10 partitions move out of a broker and 10 partitions move into a broker at any " +
      "given point. This is to avoid overwhelming the cluster by inter-broker partition movements.";

  /**
   * <code>num.concurrent.leader.movements</code>
   */
  public static final String NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG =
      "num.concurrent.leader.movements";
  private static final String NUM_CONCURRENT_LEADER_MOVEMENTS_DOC = "The maximum number of leader " +
      "movements the executor will take as one batch. This is mainly because the ZNode has a 1 MB size upper limit. And it " +
      "will also reduce the controller burden.";

  /**
   * <code>replica.movement.strategies</code>
   */
  public static final String REPLICA_MOVEMENT_STRATEGIES_CONFIG = "replica.movement.strategies";
  private static final String REPLICA_MOVEMENT_STRATEGIES_DOC = "A list of supported strategies used to determine execution order "
      + "for generated partition movement tasks.";

  /**
   * <code>default.replica.movement.strategies</code>
   */
  public static final String DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG = "default.replica.movement.strategies";
  private static final String DEFAULT_REPLICA_MOVEMENT_STRATEGIES_DOC = "The list of replica movement strategies that will be used "
      + "by default if no replica movement strategy list is provided.";

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
  private static final String GOALS_DOC = "A list of case insensitive goals in the order of priority. The high "
      + "priority goals will be executed first.";

  /**
   * <code>hard.goals</code>
   */
  public static final String HARD_GOALS_CONFIG = "hard.goals";
  private static final String HARD_GOALS_DOC = "A list of case insensitive hard goals. Hard goals will be enforced to execute "
      + "if Cruise Control runs in non-kafka-assigner mode and skip_hard_goal_check parameter is not set in request.";

  /**
   * <code>default.goals</code>
   */
  public static final String DEFAULT_GOALS_CONFIG = "default.goals";
  private static final String DEFAULT_GOALS_DOC = "The list of goals that will be used by default if no goal list "
      + "is provided. This list of goal will also be used for proposal pre-computation. If default.goals is not "
      + "specified, it will be default to goals config.";

  /**
   * <code>anomaly.notifier.class</code>
   */
  public static final String ANOMALY_NOTIFIER_CLASS_CONFIG = "anomaly.notifier.class";
  private static final String ANOMALY_NOTIFIER_CLASS_DOC = "The notifier class to trigger an alert when an "
      + "anomaly is violated. The anomaly could be either a goal violation, broker failure, or metric anomaly.";

  /**
   * <code>network.client.provider.class</code>
   */
  public static final String NETWORK_CLIENT_PROVIDER_CLASS_CONFIG = "network.client.provider.class";
  private static final String NETWORK_CLIENT_PROVIDER_CLASS_DOC = "The network client provider class to generate a "
      + "network client with given properties.";

  /**
   * <code>anomaly.detection.interval.ms</code>
   */
  public static final String ANOMALY_DETECTION_INTERVAL_MS_CONFIG = "anomaly.detection.interval.ms";
  private static final String ANOMALY_DETECTION_INTERVAL_MS_DOC = "The interval in millisecond that the detectors will "
      + "run to detect the anomalies.";

  /**
   * <code>anomaly.detection.allow.capacity.estimation</code>
   */
  public static final String ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG = "anomaly.detection.allow.capacity.estimation";
  private static final String ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_DOC = "The flag to indicate whether anomaly "
      + "detection threads allow capacity estimation in the generated cluster model they use.";

  /**
   * <code>anomaly.detection.goals</code>
   */
  public static final String ANOMALY_DETECTION_GOALS_CONFIG = "anomaly.detection.goals";
  private static final String ANOMALY_DETECTION_GOALS_DOC = "The goals that anomaly detector should detect if they are"
      + "violated.";

  /**
   * <code>broker.failure.exclude.recently.demoted.brokers</code>
   */
  public static final String BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG = "broker.failure.exclude.recently.demoted.brokers";
  private static final String BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_DOC = "True if recently demoted brokers "
      + "are excluded from optimizations during broker failure self healing, false otherwise.";

  /**
   * <code>broker.failure.exclude.recently.removed.brokers</code>
   */
  public static final String BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG = "broker.failure.exclude.recently.removed.brokers";
  private static final String BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_DOC = "True if recently removed brokers "
      + "are excluded from optimizations during broker failure self healing, false otherwise.";

  /**
   * <code>goal.violation.exclude.recently.demoted.brokers</code>
   */
  public static final String GOAL_VIOLATION_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG = "goal.violation.exclude.recently.demoted.brokers";
  private static final String GOAL_VIOLATION_EXCLUDE_RECENTLY_DEMOTED_BROKERS_DOC = "True if recently demoted brokers "
      + "are excluded from optimizations during goal violation self healing, false otherwise.";

  /**
   * <code>goal.violation.exclude.recently.removed.brokers</code>
   */
  public static final String GOAL_VIOLATION_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG = "goal.violation.exclude.recently.removed.brokers";
  private static final String GOAL_VIOLATION_EXCLUDE_RECENTLY_REMOVED_BROKERS_DOC = "True if recently removed brokers "
      + "are excluded from optimizations during goal violation self healing, false otherwise.";

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


  /**
   * <code>sample.store.class</code>
   */
  public static final String SAMPLE_STORE_CLASS_CONFIG = "sample.store.class";
  private static final String SAMPLE_STORE_CLASS_DOC = "The sample store class name. User may configure a sample store "
      + "that persist the metric samples that have already been aggregated into Kafka Cruise Control. Later on the "
      + "persisted samples can be reloaded from the sample store to Kafka Cruise Control.";

  /**
   * <code>completed.user.task.retention.time.ms</code>
   */
  public static final String COMPLETED_USER_TASK_RETENTION_TIME_MS_CONFIG = "completed.user.task.retention.time.ms";
  private static final String COMPLETED_USER_TASK_RETENTION_TIME_MS_DOC = "The maximum time in milliseconds to store the"
      + " response and access details of a completed user task.";

  /**
   * <code>demotion.history.retention.time.ms</code>
   */
  public static final String DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG = "demotion.history.retention.time.ms";
  private static final String DEMOTION_HISTORY_RETENTION_TIME_MS_DOC = "The maximum time in milliseconds to retain the"
      + " demotion history of brokers.";

  /**
   * <code>removal.history.retention.time.ms</code>
   */
  public static final String REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG = "removal.history.retention.time.ms";
  private static final String REMOVAL_HISTORY_RETENTION_TIME_MS_DOC = "The maximum time in milliseconds to retain the"
      + " removal history of brokers.";

  /**
   * <code>max.cached.completed.user.tasks</code>
   */
  public static final String MAX_CACHED_COMPLETED_USER_TASKS_CONFIG = "max.cached.completed.user.tasks";
  private static final String MAX_CACHED_COMPLETED_USER_TASKS_DOC = "The maximum number of completed user tasks for "
      + "which the response and access details will be cached.";

  /**
   * <code>max.active.user.tasks</code>
   */
  public static final String MAX_ACTIVE_USER_TASKS_CONFIG = "max.active.user.tasks";
  private static final String MAX_ACTIVE_USER_TASKS_DOC = "The maximum number of user tasks for concurrently running in "
       + "async endpoints across all users.";

  // Web Server Configurations
  /**
   * <code>webserver.http.port</code>
   */
  public static final String WEBSERVER_HTTP_PORT_CONFIG = "webserver.http.port";
  private static final String WEBSERVER_HTTP_PORT_DOC = "Cruise Control Webserver bind port.";

  /**
   * <code>webserver.http.address</code>
   */
  public static final String WEBSERVER_HTTP_ADDRESS_CONFIG = "webserver.http.address";
  private static final String WEBSERVER_HTTP_ADDRESS_DOC = "Cruise Control Webserver bind ip address.";

  /**
   * <code>webserver.http.cors.enabled</code>
   */
  public static final String WEBSERVER_HTTP_CORS_ENABLED_CONFIG = "webserver.http.cors.enabled";
  private static final String WEBSERVER_HTTP_CORS_ENABLED_DOC = "CORS enablement flag. true if enabled, false otherwise";

  /**
   * <code>webserver.http.cors.origin</code>
   */
  public static final String WEBSERVER_HTTP_CORS_ORIGIN_CONFIG = "webserver.http.cors.origin";
  private static final String WEBSERVER_HTTP_CORS_ORIGIN_DOC = "Value for the Access-Control-Allow-Origin header.";

  /**
   * <code>webserver.http.cors.allowmethods</code>
   */
  public static final String WEBSERVER_HTTP_CORS_ALLOWMETHODS_CONFIG = "webserver.http.cors.allowmethods";
  private static final String WEBSERVER_HTTP_CORS_ALLOWMETHODS_DOC = "Value for the Access-Control-Request-Method header.";

  /**
   * <code>webserver.http.cors.exposeheaders</code>
   */
  public static final String WEBSERVER_HTTP_CORS_EXPOSEHEADERS_CONFIG = "webserver.http.cors.exposeheaders";
  private static final String WEBSERVER_HTTP_CORS_EXPOSEHEADERS_DOC = "Value for the Access-Control-Expose-Headers header.";

  /**
   * <code>webserver.api.urlprefix</code>
   */
  public static final String WEBSERVER_API_URLPREFIX = "webserver.api.urlprefix";
  private static final String WEBSERVER_API_URLPREFIX_DOC = "REST API default url prefix";

  /**
   * <code>webserver.ui.diskpath</code>
   */
  public static final String WEBSERVER_UI_DISKPATH = "webserver.ui.diskpath";
  private static final String WEBSERVER_UI_DISKPATH_DOC = "Location where the Cruise Control frontend is deployed";

  /**
   * <code>webserver.ui.urlprefix</code>
   */
  public static final String WEBSERVER_UI_URLPREFIX = "webserver.ui.urlprefix";
  private static final String WEBSERVER_UI_URLPREFIX_DOC = "URL Path where UI is served from";

  /**
   * <code>webserver.request.maxBlockTimeMs</code>
   */
  public static final String WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS = "webserver.request.maxBlockTimeMs";
  private static final String WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS_DOC = "Time after which request is converted to Async";

  /**
   * <code>webserver.session.maxExpiryTimeMs</code>
   */
  public static final String WEBSERVER_SESSION_EXPIRY_MS = "webserver.session.maxExpiryTimeMs";
  private static final String WEBSERVER_SESSION_EXPIRY_MS_DOC = "Default Session Expiry Period";

  /**
   * <code>webserver.session.path</code>
   */
  public static final String WEBSERVER_SESSION_PATH = "webserver.session.path";
  private static final String WEBSERVER_SESSION_PATH_DOC = "Default Session Path (for cookies)";

  /**
   * <code>webserver.accesslog.enabled</code>
   */
  public static final String WEBSERVER_ACCESSLOG_ENABLED = "webserver.accesslog.enabled";
  private static final String WEBSERVER_ACCESSLOG_ENABLED_DOC = "true if access log is enabled";


  /**
   * <code>webserver.accesslog.path</code>
   */
  public static final String WEBSERVER_ACCESSLOG_PATH = "webserver.accesslog.path";
  private static final String WEBSERVER_ACCESSLOG_PATH_DOC = "HTTP Request log path";

  /**
   * <code>webserver.accesslog.retention.days</code>
   */
  public static final String WEBSERVER_ACCESSLOG_RETENTION_DAYS = "webserver.accesslog.retention.days";
  private static final String WEBSERVER_ACCESSLOG_RETENTION_DAYS_DOC = "HTTP Request log retention days";

  /**
   * <code>two.step.verification.enabled</code>
   */
  public static final String TWO_STEP_VERIFICATION_ENABLED_CONFIG = "two.step.verification.enabled";
  private static final String TWO_STEP_VERIFICATION_ENABLED_DOC = "Enable two-step verification for processing POST requests.";

  /**
   * <code>two.step.purgatory.retention.time.ms</code>
   */
  public static final String TWO_STEP_PURGATORY_RETENTION_TIME_MS_CONFIG = "two.step.purgatory.retention.time.ms";
  private static final String TWO_STEP_PURGATORY_RETENTION_TIME_MS_DOC = "The maximum time in milliseconds to "
      + "retain the requests in two-step (verification) purgatory.";

  /**
   * <code>two.step.purgatory.max.requests</code>
   */
  public static final String TWO_STEP_PURGATORY_MAX_REQUESTS_CONFIG = "two.step.purgatory.max.requests";
  private static final String TWO_STEP_PURGATORY_MAX_REQUESTS_DOC = "The maximum number of requests in two-step "
      + "(verification) purgatory.";

  static {
    CONFIG = new ConfigDef()
        .define(WEBSERVER_HTTP_PORT_CONFIG, ConfigDef.Type.INT, 9090, atLeast(0), ConfigDef.Importance.HIGH,
                WEBSERVER_HTTP_PORT_DOC)
        .define(WEBSERVER_HTTP_ADDRESS_CONFIG, ConfigDef.Type.STRING, "127.0.0.1", ConfigDef.Importance.HIGH,
                WEBSERVER_HTTP_ADDRESS_DOC)
        .define(WEBSERVER_HTTP_CORS_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                WEBSERVER_HTTP_CORS_ENABLED_DOC)
        .define(TWO_STEP_VERIFICATION_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
                TWO_STEP_VERIFICATION_ENABLED_DOC)
        .define(WEBSERVER_HTTP_CORS_ORIGIN_CONFIG, ConfigDef.Type.STRING, "*", ConfigDef.Importance.LOW,
                WEBSERVER_HTTP_CORS_ORIGIN_DOC)
        .define(WEBSERVER_HTTP_CORS_ALLOWMETHODS_CONFIG, ConfigDef.Type.STRING, "OPTIONS, GET, POST", ConfigDef.Importance.HIGH,
                WEBSERVER_HTTP_CORS_ALLOWMETHODS_DOC)
        .define(WEBSERVER_HTTP_CORS_EXPOSEHEADERS_CONFIG, ConfigDef.Type.STRING, "User-Task-ID", ConfigDef.Importance.HIGH,
                WEBSERVER_HTTP_CORS_EXPOSEHEADERS_DOC)
        .define(WEBSERVER_API_URLPREFIX, ConfigDef.Type.STRING, "/kafkacruisecontrol/*", ConfigDef.Importance.HIGH,
                WEBSERVER_API_URLPREFIX_DOC)
        .define(WEBSERVER_UI_DISKPATH, ConfigDef.Type.STRING, "./cruise-control-ui/dist/", ConfigDef.Importance.MEDIUM,
                WEBSERVER_UI_DISKPATH_DOC)
        .define(WEBSERVER_UI_URLPREFIX, ConfigDef.Type.STRING, "/*", ConfigDef.Importance.MEDIUM,
                WEBSERVER_UI_URLPREFIX_DOC)
        .define(WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS, ConfigDef.Type.LONG, 10000L, atLeast(0L), ConfigDef.Importance.HIGH,
                WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS_DOC)
        .define(WEBSERVER_SESSION_EXPIRY_MS, ConfigDef.Type.LONG, 60000L, atLeast(0L), ConfigDef.Importance.HIGH,
                WEBSERVER_SESSION_EXPIRY_MS_DOC)
        .define(WEBSERVER_SESSION_PATH, ConfigDef.Type.STRING, "/", ConfigDef.Importance.HIGH,
                WEBSERVER_SESSION_PATH_DOC)
        .define(WEBSERVER_ACCESSLOG_ENABLED, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                WEBSERVER_ACCESSLOG_ENABLED_DOC)
        .define(WEBSERVER_ACCESSLOG_PATH, ConfigDef.Type.STRING, "access.log", ConfigDef.Importance.LOW,
                WEBSERVER_ACCESSLOG_PATH_DOC)
        .define(WEBSERVER_ACCESSLOG_RETENTION_DAYS, ConfigDef.Type.INT, 7, atLeast(0), ConfigDef.Importance.LOW,
                WEBSERVER_ACCESSLOG_RETENTION_DAYS_DOC)
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
        .define(METADATA_MAX_AGE_CONFIG, ConfigDef.Type.LONG, 55 * 1000, atLeast(0), ConfigDef.Importance.LOW,
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
        .define(PARTITION_METRICS_WINDOW_MS_CONFIG,
                ConfigDef.Type.LONG,
                60 * 60 * 1000,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                PARTITION_METRICS_WINDOW_MS_DOC)
        .define(NUM_PARTITION_METRICS_WINDOWS_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                NUM_PARTITION_METRICS_WINDOWS_DOC)
        .define(MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG,
                ConfigDef.Type.INT,
                3,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_DOC)
        .define(MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION_DOC)
        .define(PARTITION_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(0),
                ConfigDef.Importance.LOW,
                PARTITION_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_DOC)
        .define(BROKER_METRICS_WINDOW_MS_CONFIG,
                ConfigDef.Type.LONG,
                60 * 60 * 1000,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                BROKER_METRICS_WINDOW_MS_DOC)
        .define(COMPLETED_USER_TASK_RETENTION_TIME_MS_CONFIG,
                ConfigDef.Type.LONG,
                TimeUnit.HOURS.toMillis(24),
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                COMPLETED_USER_TASK_RETENTION_TIME_MS_DOC)
        .define(DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG,
                ConfigDef.Type.LONG,
                TimeUnit.HOURS.toMillis(336),
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                DEMOTION_HISTORY_RETENTION_TIME_MS_DOC)
        .define(REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG,
                ConfigDef.Type.LONG,
                TimeUnit.HOURS.toMillis(336),
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                REMOVAL_HISTORY_RETENTION_TIME_MS_DOC)
        .define(TWO_STEP_PURGATORY_RETENTION_TIME_MS_CONFIG,
                ConfigDef.Type.LONG,
                TimeUnit.HOURS.toMillis(336),
                atLeast(TimeUnit.HOURS.toMillis(1)),
                ConfigDef.Importance.MEDIUM, TWO_STEP_PURGATORY_RETENTION_TIME_MS_DOC)
        .define(TWO_STEP_PURGATORY_MAX_REQUESTS_CONFIG,
                ConfigDef.Type.INT,
                25,
                atLeast(1),
                ConfigDef.Importance.MEDIUM, TWO_STEP_PURGATORY_MAX_REQUESTS_DOC)
        .define(MAX_CACHED_COMPLETED_USER_TASKS_CONFIG,
                ConfigDef.Type.INT,
                100,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                MAX_CACHED_COMPLETED_USER_TASKS_DOC)
        .define(MAX_ACTIVE_USER_TASKS_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                MAX_ACTIVE_USER_TASKS_DOC)
        .define(NUM_BROKER_METRICS_WINDOWS_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                NUM_BROKER_METRICS_WINDOWS_DOC)
        .define(MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_CONFIG,
                ConfigDef.Type.INT,
                3,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_DOC)
        .define(MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER_DOC)
        .define(BROKER_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(0),
                ConfigDef.Importance.LOW,
                BROKER_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_DOC)
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
        .define(NUM_CACHED_RECENT_ANOMALY_STATES_CONFIG,
            ConfigDef.Type.INT,
            10,
            between(1, 100),
            ConfigDef.Importance.LOW,
            NUM_CACHED_RECENT_ANOMALY_STATES_DOC)
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
        .define(BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                BrokerCapacityConfigFileResolver.class.getName(),
                ConfigDef.Importance.MEDIUM,
                BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_DOC)
        .define(MIN_VALID_PARTITION_RATIO_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.995,
                between(0, 1),
                ConfigDef.Importance.HIGH, MIN_VALID_PARTITION_RATIO_DOC)
        .define(LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.7,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_DOC)
        .define(LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.15,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_DOC)
        .define(FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.15,
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
        .define(REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                1.10,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                REPLICA_COUNT_BALANCE_THRESHOLD_DOC)
        .define(TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                1.80,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_DOC)
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
        .define(METRIC_ANOMALY_FINDER_CLASSES_CONFIG,
                ConfigDef.Type.LIST,
                DEFAULT_METRIC_ANOMALY_FINDER_CLASS,
                ConfigDef.Importance.MEDIUM, METRIC_ANOMALY_FINDER_CLASSES_DOC)
        .define(PROPOSAL_EXPIRATION_MS_CONFIG,
                ConfigDef.Type.LONG,
                900000,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                PROPOSAL_EXPIRATION_MS_DOC)
        .define(MAX_REPLICAS_PER_BROKER_CONFIG,
            ConfigDef.Type.LONG,
            10000,
            atLeast(0),
            ConfigDef.Importance.MEDIUM,
            MAX_REPLICAS_PER_BROKER_DOC)
        .define(NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG,
                ConfigDef.Type.INT,
                1,
                between(0, 1),
                ConfigDef.Importance.LOW,
                NUM_PROPOSAL_PRECOMPUTE_THREADS_DOC)
        .define(ZOOKEEPER_CONNECT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, ZOOKEEPER_CONNECT_DOC)
        .define(NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(1),
                ConfigDef.Importance.MEDIUM,
                NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_DOC)
        .define(NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG,
                ConfigDef.Type.INT,
                1000,
                atLeast(1),
                ConfigDef.Importance.MEDIUM,
                NUM_CONCURRENT_LEADER_MOVEMENTS_DOC)
        .define(REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                ConfigDef.Type.LIST,
                new StringJoiner(",")
                    .add(PostponeUrpReplicaMovementStrategy.class.getName())
                    .add(PrioritizeLargeReplicaMovementStrategy.class.getName())
                    .add(PrioritizeSmallReplicaMovementStrategy.class.getName())
                    .add(BaseReplicaMovementStrategy.class.getName()).toString(),
                ConfigDef.Importance.MEDIUM,
                REPLICA_MOVEMENT_STRATEGIES_DOC)
        .define(DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                ConfigDef.Type.LIST,
                BaseReplicaMovementStrategy.class.getName(),
                ConfigDef.Importance.MEDIUM,
                DEFAULT_REPLICA_MOVEMENT_STRATEGIES_DOC)
        .define(EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG,
                ConfigDef.Type.LONG,
                10000L,
                atLeast(0),
                ConfigDef.Importance.LOW,
                EXECUTION_PROGRESS_CHECK_INTERVAL_MS_DOC)
        .define(GOALS_CONFIG,
                ConfigDef.Type.LIST,
                new StringJoiner(",")
                    .add(RackAwareGoal.class.getName())
                    .add(ReplicaCapacityGoal.class.getName())
                    .add(DiskCapacityGoal.class.getName())
                    .add(NetworkInboundCapacityGoal.class.getName())
                    .add(NetworkOutboundCapacityGoal.class.getName())
                    .add(CpuCapacityGoal.class.getName())
                    .add(ReplicaDistributionGoal.class.getName())
                    .add(PotentialNwOutGoal.class.getName())
                    .add(DiskUsageDistributionGoal.class.getName())
                    .add(NetworkInboundUsageDistributionGoal.class.getName())
                    .add(NetworkOutboundUsageDistributionGoal.class.getName())
                    .add(CpuUsageDistributionGoal.class.getName())
                    .add(LeaderBytesInDistributionGoal.class.getName())
                    .add(TopicReplicaDistributionGoal.class.getName()).toString(),
                ConfigDef.Importance.HIGH,
                GOALS_DOC)
        .define(HARD_GOALS_CONFIG,
                ConfigDef.Type.LIST,
                new StringJoiner(",")
                    .add(RackAwareGoal.class.getName())
                    .add(ReplicaCapacityGoal.class.getName())
                    .add(DiskCapacityGoal.class.getName())
                    .add(NetworkInboundCapacityGoal.class.getName())
                    .add(NetworkOutboundCapacityGoal.class.getName())
                    .add(CpuCapacityGoal.class.getName()).toString(),
                ConfigDef.Importance.HIGH,
                HARD_GOALS_DOC)
        .define(DEFAULT_GOALS_CONFIG,
                ConfigDef.Type.LIST,
                ConfigDef.Importance.HIGH,
                DEFAULT_GOALS_DOC)
        .define(ANOMALY_NOTIFIER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                DEFAULT_ANOMALY_NOTIFIER_CLASS,
                ConfigDef.Importance.LOW, ANOMALY_NOTIFIER_CLASS_DOC)
        .define(NETWORK_CLIENT_PROVIDER_CLASS_CONFIG,
                ConfigDef.Type.CLASS, DEFAULT_NETWORK_CLIENT_PROVIDER_CLASS,
                ConfigDef.Importance.LOW,
                NETWORK_CLIENT_PROVIDER_CLASS_DOC)
        .define(ANOMALY_DETECTION_INTERVAL_MS_CONFIG,
                ConfigDef.Type.LONG,
                300000L,
                ConfigDef.Importance.LOW,
                ANOMALY_DETECTION_INTERVAL_MS_DOC)
        .define(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.LOW,
                ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_DOC)
        .define(ANOMALY_DETECTION_GOALS_CONFIG,
                ConfigDef.Type.LIST,
                new StringJoiner(",")
                    .add(RackAwareGoal.class.getName())
                    .add(ReplicaCapacityGoal.class.getName())
                    .add(DiskCapacityGoal.class.getName()).toString(),
                ConfigDef.Importance.MEDIUM,
                ANOMALY_DETECTION_GOALS_DOC)
        .define(BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.MEDIUM,
                BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_DOC)
        .define(BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.MEDIUM,
                BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_DOC)
        .define(GOAL_VIOLATION_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.MEDIUM,
                GOAL_VIOLATION_EXCLUDE_RECENTLY_DEMOTED_BROKERS_DOC)
        .define(GOAL_VIOLATION_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.MEDIUM,
                GOAL_VIOLATION_EXCLUDE_RECENTLY_REMOVED_BROKERS_DOC)
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
        .withClientSslSupport()
        .withClientSaslSupport();
  }

  @Override
  public <T> T getConfiguredInstance(String key, Class<T> t) {
    T o = super.getConfiguredInstance(key, t);
    if (o instanceof CruiseControlConfigurable) {
      ((CruiseControlConfigurable) o).configure(originals());
    }
    return o;
  }

  @Override
  public <T> List<T> getConfiguredInstances(String key, Class<T> t) {
    List<T> objects = super.getConfiguredInstances(key, t);
    for (T o : objects) {
      if (o instanceof CruiseControlConfigurable) {
        ((CruiseControlConfigurable) o).configure(originals());
      }
    }
    return objects;
  }

  @Override
  public <T> List<T> getConfiguredInstances(String key, Class<T> t, Map<String, Object> configOverrides) {
    List<T> objects = super.getConfiguredInstances(key, t, configOverrides);
    Map<String, Object> configPairs = originals();
    configPairs.putAll(configOverrides);
    for (T o : objects) {
      if (o instanceof CruiseControlConfigurable) {
        ((CruiseControlConfigurable) o).configure(configPairs);
      }
    }
    return objects;
  }

  /**
   * Sanity check for
   * (1) {@link KafkaCruiseControlConfig#GOALS_CONFIG} is non-empty.
   * (2) Case insensitive goal names.
   * (3) {@link KafkaCruiseControlConfig#DEFAULT_GOALS_CONFIG} is non-empty.
   * (4) {@link KafkaCruiseControlConfig#ANOMALY_DETECTION_GOALS_CONFIG} is a sublist of {@link KafkaCruiseControlConfig#GOALS_CONFIG}.
   */
  private void sanityCheckGoalNames() {
    List<String> goalNames = getList(KafkaCruiseControlConfig.GOALS_CONFIG);
    // Ensure that goals is non-empty.
    if (goalNames.isEmpty()) {
      throw new ConfigException("Attempt to configure goals configuration with an empty list of goals.");
    }

    Set<String> caseInsensitiveGoalNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    for (String goalName: goalNames) {
      if (!caseInsensitiveGoalNames.add(goalName.replaceAll(".*\\.", ""))) {
        throw new ConfigException("Attempt to configure goals with case sensitive names.");
      }
    }
    // Ensure that default goals is non-empty.
    List<String> defaultGoalNames = getList(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG);
    if (defaultGoalNames.isEmpty()) {
      throw new ConfigException("Attempt to configure default goals configuration with an empty list of goals.");
    }

    // Ensure that goals used for anomaly detection are supported goals.
    List<String> anomalyDetectionGoalNames = getList(KafkaCruiseControlConfig.ANOMALY_DETECTION_GOALS_CONFIG);
    if (anomalyDetectionGoalNames.stream().anyMatch(g -> !defaultGoalNames.contains(g))) {
      throw new ConfigException("Attempt to configure anomaly detection goals with unsupported goals.");
    }
  }

  /**
   * Sanity check to ensure that {@link KafkaCruiseControlConfig#METADATA_MAX_AGE_CONFIG} is not longer than
   * {@link KafkaCruiseControlConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG}.
   *
   * Sampling process involves a potential metadata update if the current metadata is stale. The configuration
   * {@link KafkaCruiseControlConfig#METADATA_MAX_AGE_CONFIG} indicates the timeout of such a metadata update. Hence,
   * this subprocess of the sampling process cannot be set with a timeout larger than the total sampling timeout of
   * {@link KafkaCruiseControlConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG}.
   */
  private void sanityCheckSamplingPeriod() {
    long samplingPeriodMs = getLong(KafkaCruiseControlConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG);
    long metadataTimeoutMs = getLong(KafkaCruiseControlConfig.METADATA_MAX_AGE_CONFIG);
    if (metadataTimeoutMs >  samplingPeriodMs) {
      throw new ConfigException("Attempt to set metadata refresh timeout [" + metadataTimeoutMs +
          "] to be longer than sampling period [" + samplingPeriodMs + "].");
    }
  }

  public KafkaCruiseControlConfig(Map<?, ?> originals) {
    super(CONFIG, originals);
    sanityCheckGoalNames();
    sanityCheckSamplingPeriod();
  }

  public KafkaCruiseControlConfig(Map<?, ?> originals, boolean doLog) {
    super(CONFIG, originals, doLog);
    sanityCheckGoalNames();
    sanityCheckSamplingPeriod();
  }
}
