/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import java.util.StringJoiner;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;


/**
 * A class to keep Cruise Control Analyzer Configs and defaults.
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public class AnalyzerConfig {
  private AnalyzerConfig() {

  }

  /**
   * <code>cpu.balance.threshold</code>
   */
  public static final String CPU_BALANCE_THRESHOLD_CONFIG = "cpu.balance.threshold";
  public static final String CPU_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for CPU utilization. "
      + "For example, 1.10 means the highest CPU usage of a broker should not be above 1.10x of average CPU utilization"
      + " of all the brokers.";

  /**
   * <code>disk.balance.threshold</code>
   */
  public static final String DISK_BALANCE_THRESHOLD_CONFIG = "disk.balance.threshold";
  public static final String DISK_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for disk utilization. "
      + "For example, 1.10 means the highest disk usage of a broker should not be above 1.10x of average disk utilization"
      + " of all the brokers.";

  /**
   * <code>network.inbound.balance.threshold</code>
   */
  public static final String NETWORK_INBOUND_BALANCE_THRESHOLD_CONFIG = "network.inbound.balance.threshold";
  public static final String NETWORK_INBOUND_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for network "
      + "inbound usage. For example, 1.10 means the highest network inbound usage of a broker should not be above "
      + "1.10x of average network inbound usage of all the brokers.";

  /**
   * <code>network.outbound.balance.threshold</code>
   */
  public static final String NETWORK_OUTBOUND_BALANCE_THRESHOLD_CONFIG = "network.outbound.balance.threshold";
  public static final String NETWORK_OUTBOUND_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for network "
      + "outbound usage. For example, 1.10 means the highest network outbound usage of a broker should not be above "
      + "1.10x of average network outbound usage of all the brokers.";

  /**
   * <code>replica.count.balance.threshold</code>
   */
  public static final String REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG = "replica.count.balance.threshold";
  public static final String REPLICA_COUNT_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for replica "
      + "distribution. For example, 1.10 means the highest replica count of a broker should not be above 1.10x of "
      + "average replica count of all brokers.";

  /**
   * <code>leader.replica.count.balance.threshold</code>
   */
  public static final String LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG = "leader.replica.count.balance.threshold";
  public static final String LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for "
      + "leader replica distribution. For example, 1.10 means the highest leader replica count of a broker should not be "
      + "above 1.10x of average leader replica count of all alive brokers.";

  /**
   * <code>topic.replica.count.balance.threshold</code>
   */
  public static final String TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG = "topic.replica.count.balance.threshold";
  public static final String TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for "
      + "replica distribution from each topic. For example, 1.80 means the highest topic replica count of a broker "
      + "should not be above 1.80x of average replica count of all brokers for the same topic.";

  /**
   * <code>cpu.capacity.threshold</code>
   */
  public static final String CPU_CAPACITY_THRESHOLD_CONFIG = "cpu.capacity.threshold";
  public static final String CPU_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total broker.cpu.capacity that is "
      + "allowed to be used on a broker. The analyzer will enforce a hard goal that the cpu utilization "
      + "of a broker cannot be higher than (broker.cpu.capacity * cpu.capacity.threshold).";

  /**
   * <code>cpu.capacity.threshold</code>
   */
  public static final String DISK_CAPACITY_THRESHOLD_CONFIG = "disk.capacity.threshold";
  public static final String DISK_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total broker.disk.capacity that is "
      + "allowed to be used on a broker. The analyzer will enforce a hard goal that the disk usage "
      + "of a broker cannot be higher than (broker.disk.capacity * disk.capacity.threshold).";

  /**
   * <code>network.inbound.capacity.threshold</code>
   */
  public static final String NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG = "network.inbound.capacity.threshold";
  public static final String NETWORK_INBOUND_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total "
      + "broker.network.inbound.capacity that is allowed to be used on a broker. The analyzer will enforce a hard goal "
      + "that the disk usage of a broker cannot be higher than (broker.network.inbound.capacity * "
      + "network.inbound.capacity.threshold).";

  /**
   * <code>network.outbound.capacity.threshold</code>
   */
  public static final String NETWORK_OUTBOUND_CAPACITY_THRESHOLD_CONFIG = "network.outbound.capacity.threshold";
  public static final String NETWORK_OUTBOUND_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total "
      + "broker.network.outbound.capacity that is allowed to be used on a broker. The analyzer will enforce a hard goal "
      + "that the disk usage of a broker cannot be higher than (broker.network.outbound.capacity * "
      + "network.outbound.capacity.threshold).";

  /**
   * <code>cpu.low.utilization.threshold</code>
   */
  public static final String CPU_LOW_UTILIZATION_THRESHOLD_CONFIG = "cpu.low.utilization.threshold";
  public static final String CPU_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold for Kafka Cruise Control to define the "
      + "utilization of CPU is low enough that rebalance is not worthwhile. The cluster will only be in a low "
      + "utilization state when all the brokers are below the low utilization threshold. The threshold is in percentage.";

  /**
   * <code>disk.low.utilization.threshold</code>
   */
  public static final String DISK_LOW_UTILIZATION_THRESHOLD_CONFIG = "disk.low.utilization.threshold";
  public static final String DISK_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold for Kafka Cruise Control to define the "
      + "utilization of DISK is low enough that rebalance is not worthwhile. The cluster will only be in a low "
      + "utilization state when all the brokers are below the low utilization threshold. The threshold is in percentage.";

  /**
   * <code>network.inbound.low.utilization.threshold</code>
   */
  public static final String NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG = "network.inbound.low.utilization.threshold";
  public static final String NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold for Kafka Cruise Control to define the"
      + " utilization of network inbound rate is low enough that rebalance is not worthwhile. The cluster will only be in "
      + "a low utilization state when all the brokers are below the low utilization threshold. The threshold is in percentage.";

  /**
   * <code>network.outbound.low.utilization.threshold</code>
   */
  public static final String NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG = "network.outbound.low.utilization.threshold";
  public static final String NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold for Kafka Cruise Control to define the"
      + " utilization of network outbound rate is low enough that rebalance is not worthwhile. The cluster will only be in a "
      + "low utilization state when all the brokers are below the low utilization threshold. The threshold is in percentage.";

  /**
   * <code>proposal.expiration.ms</code>
   */
  public static final String PROPOSAL_EXPIRATION_MS_CONFIG = "proposal.expiration.ms";
  public static final String PROPOSAL_EXPIRATION_MS_DOC = "Kafka Cruise Control will cache one of the best proposal among all "
      + "the optimization proposal candidates it recently computed. This configuration defines when will the"
      + "cached proposal be invalidated and needs a recomputation. If proposal.expiration.ms is set to 0, Cruise Control"
      + "will continuously compute the proposal candidates.";

  /**
   * <code>max.replicas.per.broker</code>
   */
  public static final String MAX_REPLICAS_PER_BROKER_CONFIG = "max.replicas.per.broker";
  public static final String MAX_REPLICAS_PER_BROKER_DOC = "The maximum number of replicas allowed to reside on a "
      + "broker. The analyzer will enforce a hard goal that the number of replica on a broker cannot be higher than "
      + "this config.";

  /**
   * <code>num.proposal.precompute.threads</code>
   */
  public static final String NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG = "num.proposal.precompute.threads";
  public static final String NUM_PROPOSAL_PRECOMPUTE_THREADS_DOC = "The number of thread used to precompute the "
      + "optimization proposal candidates. The more threads are used, the more memory and CPU resource will be used.";

  /**
   * <code>goals</code>
   */
  public static final String GOALS_CONFIG = "goals";
  public static final String DEFAULT_GOALS = new StringJoiner(",").add(RackAwareGoal.class.getName())
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
                                                                  .add(LeaderReplicaDistributionGoal.class.getName())
                                                                  .add(LeaderBytesInDistributionGoal.class.getName())
                                                                  .add(TopicReplicaDistributionGoal.class.getName())
                                                                  .add(KafkaAssignerDiskUsageDistributionGoal.class.getName())
                                                                  .add(KafkaAssignerEvenRackAwareGoal.class.getName())
                                                                  .add(PreferredLeaderElectionGoal.class.getName()).toString();
  public static final String GOALS_DOC = "A list of case insensitive goals in the order of priority. The high "
      + "priority goals will be executed first.";

  /**
   * <code>intra.broker.goals</code>
   */
  public static final String INTRA_BROKER_GOALS_CONFIG = "intra.broker.goals";
  public static final String DEFAULT_INTRA_BROKER_GOALS = new StringJoiner(",").add(IntraBrokerDiskCapacityGoal.class.getName())
                                                                               .add(IntraBrokerDiskUsageDistributionGoal.class.getName()).toString();
  public static final String INTRA_BROKER_GOALS_DOC = "A list of case insensitive intra-broker goals in the order of priority. "
      + "The high priority goals will be executed first. The intra-broker goals are only relevant if intra-broker operation is "
      + "supported(i.e. in  Cruise Control versions above 2.*), otherwise this list should be empty.";

  /**
   * <code>hard.goals</code>
   */
  public static final String HARD_GOALS_CONFIG = "hard.goals";
  public static final String DEFAULT_HARD_GOALS = new StringJoiner(",").add(RackAwareGoal.class.getName())
                                                                       .add(ReplicaCapacityGoal.class.getName())
                                                                       .add(DiskCapacityGoal.class.getName())
                                                                       .add(NetworkInboundCapacityGoal.class.getName())
                                                                       .add(NetworkOutboundCapacityGoal.class.getName())
                                                                       .add(CpuCapacityGoal.class.getName()).toString();
  public static final String HARD_GOALS_DOC = "A list of case insensitive hard goals. Hard goals will be enforced to execute "
      + "if Cruise Control runs in non-kafka-assigner mode and skip_hard_goal_check parameter is not set in request.";

  /**
   * <code>default.goals</code>
   */
  public static final String DEFAULT_GOALS_CONFIG = "default.goals";
  public static final String DEFAULT_DEFAULT_GOALS = new StringJoiner(",").add(RackAwareGoal.class.getName())
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
                                                                          .add(TopicReplicaDistributionGoal.class.getName())
                                                                          .add(LeaderReplicaDistributionGoal.class.getName())
                                                                          .add(LeaderBytesInDistributionGoal.class.getName()).toString();
  public static final String DEFAULT_GOALS_DOC = "The list of goals that will be used by default if no goal list "
      + "is provided. This list of goal will also be used for proposal pre-computation";

  /**
   * <code>goal.balancedness.priority.weight</code>
   */
  public static final String GOAL_BALANCEDNESS_PRIORITY_WEIGHT_CONFIG = "goal.balancedness.priority.weight";
  public static final String GOAL_BALANCEDNESS_PRIORITY_WEIGHT_DOC = "The impact of having one level higher goal priority"
      + " on the relative balancedness score. For example, 1.1 means that a goal with higher priority will have the 1.1x"
      + " balancedness weight of the lower priority goal (assuming the same goal.balancedness.strictness.weight values for"
      + " both goals).";

  /**
   * <code>goal.balancedness.strictness.weight</code>
   */
  public static final String GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_CONFIG = "goal.balancedness.strictness.weight";
  public static final String GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_DOC = "The impact of strictness (i.e. hard or soft goal)"
      + " on the relative balancedness score. For example, 1.5 means that a hard goal will have the 1.5x balancedness "
      + "weight of a soft goal (assuming goal.balancedness.priority.weight is 1).";

  /**
   * <code>allow.capacity.estimation.on.proposal.precompute</code>
   */
  public static final String ALLOW_CAPACITY_ESTIMATION_ON_PROPOSAL_PRECOMPUTE_CONFIG = "allow.capacity.estimation.on.proposal.precompute";
  public static final String ALLOW_CAPACITY_ESTIMATION_ON_PROPOSAL_PRECOMPUTE_DOC = "The flag to indicate whether to "
      + "allow capacity estimation on proposal precomputation.";

  /**
   * <code>topics.excluded.from.partition.movement</code>
   */
  public static final String TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG = "topics.excluded.from.partition.movement";
  public static final String TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_DOC = "The topics that should be excluded from the "
      + "partition movement. It is a regex. Notice that this regex will be ignored when decommission a broker is invoked.";

  /**
   * <code>goal.violation.distribution.threshold.multiplier</code>
   */
  public static final String GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG = "goal.violation.distribution.threshold.multiplier";
  public static final String GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_DOC = "The multiplier applied to the threshold"
      + " of distribution goals used for detecting and fixing anomalies. For example, 2.50 means the threshold for each "
      + "distribution goal (i.e. Replica Distribution, Leader Replica Distribution, Resource Distribution, and Topic Replica "
      + "Distribution Goals) will be 2.50x of the value used in manual goal optimization requests (e.g. rebalance).";

  /**
   * Define configs for Analyzer.
   *
   * @param configDef Config definition.
   * @return The given ConfigDef after defining the configs for Analyzer.
   */
  public static ConfigDef define(ConfigDef configDef) {
    return configDef.define(CPU_BALANCE_THRESHOLD_CONFIG,
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
                    .define(LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            1.10,
                            atLeast(1),
                            ConfigDef.Importance.HIGH,
                            LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_DOC)
                    .define(TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            3.00,
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
                    .define(GOALS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_GOALS,
                            ConfigDef.Importance.HIGH,
                            GOALS_DOC)
                    .define(INTRA_BROKER_GOALS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_INTRA_BROKER_GOALS,
                            ConfigDef.Importance.HIGH,
                            INTRA_BROKER_GOALS_DOC)
                    .define(HARD_GOALS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_HARD_GOALS,
                            ConfigDef.Importance.HIGH,
                            HARD_GOALS_DOC)
                    .define(DEFAULT_GOALS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_DEFAULT_GOALS,
                            ConfigDef.Importance.HIGH,
                            DEFAULT_GOALS_DOC)
                    .define(GOAL_BALANCEDNESS_PRIORITY_WEIGHT_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            1.1,
                            between(1, 2),
                            ConfigDef.Importance.LOW,
                            GOAL_BALANCEDNESS_PRIORITY_WEIGHT_DOC)
                    .define(GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            1.5,
                            between(1, 2),
                            ConfigDef.Importance.LOW,
                            GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_DOC)
                    .define(ALLOW_CAPACITY_ESTIMATION_ON_PROPOSAL_PRECOMPUTE_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            true,
                            ConfigDef.Importance.LOW,
                            ALLOW_CAPACITY_ESTIMATION_ON_PROPOSAL_PRECOMPUTE_DOC)
                    .define(TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG,
                            ConfigDef.Type.STRING,
                            "",
                            ConfigDef.Importance.LOW,
                            TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_DOC)
                    .define(GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            1.00,
                            atLeast(1),
                            ConfigDef.Importance.MEDIUM, GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_DOC);
  }
}
