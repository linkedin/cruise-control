/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import kafka.log.LogConfig;
import kafka.server.ConfigType;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * See https://kafka.apache.org/documentation/#rep-throttle
 */
class ReplicationThrottleHelper {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationThrottleHelper.class);

  static final String LEADER_THROTTLED_RATE = "leader.replication.throttled.rate";
  static final String FOLLOWER_THROTTLED_RATE = "follower.replication.throttled.rate";
  static final String LEADER_THROTTLED_REPLICAS = LogConfig.LeaderReplicationThrottledReplicasProp();
  static final String FOLLOWER_THROTTLED_REPLICAS = LogConfig.FollowerReplicationThrottledReplicasProp();

  private final KafkaZkClient _kafkaZkClient;
  private final AdminZkClient _adminZkClient;
  private final Long _throttleRate;

  ReplicationThrottleHelper(KafkaZkClient kafkaZkClient, Long throttleRate) {
    this._kafkaZkClient = kafkaZkClient;
    this._adminZkClient = new AdminZkClient(kafkaZkClient);
    this._throttleRate = throttleRate;
  }

  void setThrottles(List<ExecutionProposal> replicaMovementProposals) {
    if (throttlingEnabled()) {
      LOG.info("Setting a rebalance throttle of {} bytes/sec", _throttleRate);
      Set<Integer> participatingBrokers = getParticipatingBrokers(replicaMovementProposals);
      Map<String, Set<String>> throttledReplicas = getThrottledReplicasByTopic(replicaMovementProposals);
      participatingBrokers.forEach(this::setLeaderThrottledRateIfUnset);
      participatingBrokers.forEach(this::setFollowerThrottledRateIfUnset);
      throttledReplicas.forEach(this::setLeaderThrottledReplicas);
      throttledReplicas.forEach(this::setFollowerThrottledReplicas);
    }
  }

  // Determines if a candidate task is ready to have its throttles removed.
  boolean shouldRemoveThrottleForTask(ExecutionTask task) {
    return
      // the task should not be in progress
      task.state() != ExecutionTask.State.IN_PROGRESS &&
        // the task should not be pending
        task.state() != ExecutionTask.State.PENDING &&
        // replica throttles only apply to inter-broker replica movement
        task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
  }

  // determines if a candidate task is in progress and related to inter-broker
  // replica movement.
  boolean taskIsInProgress(ExecutionTask task) {
    return
      task.state() == ExecutionTask.State.IN_PROGRESS &&
        task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
  }

  // clear throttles for a specific list of execution tasks
  void clearThrottles(List<ExecutionTask> completedTasks, List<ExecutionTask> inProgressTasks) {
    if (throttlingEnabled()) {
      List<ExecutionProposal> completedProposals =
        completedTasks
          .stream()
          // Filter for completed tasks related to inter-broker replica movement
          .filter(this::shouldRemoveThrottleForTask)
          .map(ExecutionTask::proposal)
          .collect(Collectors.toList());

      // These are the brokers which have completed a task with
      // inter-broker replica movement
      Set<Integer> participatingBrokers = getParticipatingBrokers(completedProposals);

      List<ExecutionProposal> inProgressProposals =
        inProgressTasks
          .stream()
          .filter(this::taskIsInProgress)
          .map(ExecutionTask::proposal)
          .collect(Collectors.toList());

      // These are the brokers which currently have in-progress
      // inter-broker replica movement
      Set<Integer> brokersWithInProgressTasks = getParticipatingBrokers(inProgressProposals);

      // Remove the brokers with in-progress replica moves from the brokers that have
      // completed inter-broker replica moves
      Set<Integer> brokersToRemoveThrottlesFrom = new TreeSet<>(participatingBrokers);
      brokersToRemoveThrottlesFrom.removeAll(brokersWithInProgressTasks);

      LOG.info("Removing replica movement throttles from brokers in the cluster: {}", brokersToRemoveThrottlesFrom);
      brokersToRemoveThrottlesFrom.forEach(this::removeThrottledRateFromBroker);

      Map<String, Set<String>> throttledReplicas = getThrottledReplicasByTopic(completedProposals);
      throttledReplicas.forEach(this::removeThrottledReplicasFromTopic);
    }
  }

  private boolean throttlingEnabled() {
    return _throttleRate != null;
  }

  private Set<Integer> getParticipatingBrokers(List<ExecutionProposal> replicaMovementProposals) {
    Set<Integer> participatingBrokers = new TreeSet<>();
    for (ExecutionProposal proposal : replicaMovementProposals) {
      participatingBrokers.addAll(proposal.oldReplicas().stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet()));
      participatingBrokers.addAll(proposal.newReplicas().stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet()));
    }
    return participatingBrokers;
  }

  private Map<String, Set<String>> getThrottledReplicasByTopic(List<ExecutionProposal> replicaMovementProposals) {
    Map<String, Set<String>> throttledReplicasByTopic = new HashMap<>();
    for (ExecutionProposal proposal : replicaMovementProposals) {
      String topic = proposal.topic();
      int partitionId = proposal.partitionId();
      Stream<Integer> brokers = Stream.concat(
        proposal.oldReplicas().stream().map(ReplicaPlacementInfo::brokerId),
        proposal.replicasToAdd().stream().map(ReplicaPlacementInfo::brokerId));
      Set<String> throttledReplicas = throttledReplicasByTopic
        .computeIfAbsent(topic, (x) -> new TreeSet<>());
      brokers.forEach((brokerId) -> throttledReplicas.add(partitionId + ":" + brokerId));
    }
    return throttledReplicasByTopic;
  }

  private void setLeaderThrottledRateIfUnset(int brokerId) {
    setThrottledRateIfUnset(brokerId, LEADER_THROTTLED_RATE);
  }

  private void setFollowerThrottledRateIfUnset(int brokerId) {
    setThrottledRateIfUnset(brokerId, FOLLOWER_THROTTLED_RATE);
  }

  private void setThrottledRateIfUnset(int brokerId, String configKey) {
    assert (_throttleRate != null);
    assert (configKey.equals(LEADER_THROTTLED_RATE) || configKey.equals(FOLLOWER_THROTTLED_RATE));
    Properties config = _kafkaZkClient.getEntityConfigs(ConfigType.Broker(), String.valueOf(brokerId));
    Object oldThrottleRate = config.setProperty(configKey, String.valueOf(_throttleRate));
    if (oldThrottleRate == null) {
      LOG.debug("Setting {} to {} bytes/second for broker {}", configKey, _throttleRate, brokerId);
      ExecutorUtils.changeBrokerConfig(_adminZkClient, brokerId, config);
    } else {
      LOG.debug("Not setting {} for broker {} because pre-existing throttle of {} was already set",
        configKey, brokerId, oldThrottleRate);
    }
  }

  private void setLeaderThrottledReplicas(String topic, Set<String> replicas) {
    setThrottledReplicas(topic, replicas, LEADER_THROTTLED_REPLICAS);
  }

  private void setFollowerThrottledReplicas(String topic, Set<String> replicas) {
    setThrottledReplicas(topic, replicas, FOLLOWER_THROTTLED_REPLICAS);
  }

  private void setThrottledReplicas(String topic, Set<String> replicas, String configKey) {
    assert (configKey.equals(LEADER_THROTTLED_REPLICAS) || configKey.equals(FOLLOWER_THROTTLED_REPLICAS));
    Properties config = _kafkaZkClient.getEntityConfigs(ConfigType.Topic(), topic);
    // Merge new throttled replicas with existing configuration values.
    Set<String> newThrottledReplicas = new TreeSet<>(replicas);
    String oldThrottledReplicas = config.getProperty(configKey);
    if (oldThrottledReplicas != null) {
      newThrottledReplicas.addAll(Arrays.asList(oldThrottledReplicas.split(",")));
    }
    config.setProperty(configKey, String.join(",", newThrottledReplicas));
    ExecutorUtils.changeTopicConfig(_adminZkClient, topic, config);
  }

  static String removeReplicasFromConfig(String throttleConfig, Set<String> replicas) {
    ArrayList<String> throttles = new ArrayList<>(Arrays.asList(throttleConfig.split(",")));
    throttles.removeIf(replicas::contains);
    return String.join(",", throttles);
  }

  private void removeLeaderThrottledReplicasFromTopic(Properties config, String topic, Set<String> replicas) {
    String oldLeaderThrottledReplicas = config.getProperty(LEADER_THROTTLED_REPLICAS);
    if (oldLeaderThrottledReplicas != null) {
      replicas.forEach(r -> LOG.debug("Removing leader throttles for topic {} on replica {}", topic, r));
      String newLeaderThrottledReplicas = removeReplicasFromConfig(oldLeaderThrottledReplicas, replicas);
      if (newLeaderThrottledReplicas.isEmpty()) {
        config.remove(LEADER_THROTTLED_REPLICAS);
      } else {
        config.setProperty(LEADER_THROTTLED_REPLICAS, newLeaderThrottledReplicas);
      }
    }
  }

  private void removeFollowerThrottledReplicasFromTopic(Properties config, String topic, Set<String> replicas) {
    String oldLeaderThrottledReplicas = config.getProperty(FOLLOWER_THROTTLED_REPLICAS);
    if (oldLeaderThrottledReplicas != null) {
      replicas.forEach(r -> LOG.debug("Removing follower throttles for topic {} and replica {}", topic, r));
      String newLeaderThrottledReplicas = removeReplicasFromConfig(oldLeaderThrottledReplicas, replicas);
      if (newLeaderThrottledReplicas.isEmpty()) {
        config.remove(FOLLOWER_THROTTLED_REPLICAS);
      } else {
        config.setProperty(FOLLOWER_THROTTLED_REPLICAS, newLeaderThrottledReplicas);
      }
    }
  }

  private void removeThrottledReplicasFromTopic(String topic, Set<String> replicas) {
    Properties config = _kafkaZkClient.getEntityConfigs(ConfigType.Topic(), topic);
    removeLeaderThrottledReplicasFromTopic(config, topic, replicas);
    removeFollowerThrottledReplicasFromTopic(config, topic, replicas);
    ExecutorUtils.changeTopicConfig(_adminZkClient, topic, config);
  }

  private void removeAllThrottledReplicasFromTopic(String topic) {
    Properties config = _kafkaZkClient.getEntityConfigs(ConfigType.Topic(), topic);
    Object oldLeaderThrottle = config.remove(LEADER_THROTTLED_REPLICAS);
    Object oldFollowerThrottle = config.remove(FOLLOWER_THROTTLED_REPLICAS);
    if (oldLeaderThrottle != null) {
      LOG.debug("Removing leader throttled replicas for topic {}", topic);
    }
    if (oldFollowerThrottle != null) {
      LOG.debug("Removing follower throttled replicas for topic {}", topic);
    }
    if (oldLeaderThrottle != null || oldFollowerThrottle != null) {
      ExecutorUtils.changeTopicConfig(_adminZkClient, topic, config);
    }
  }

  private void removeThrottledRateFromBroker(Integer brokerId) {
    Properties config = _kafkaZkClient.getEntityConfigs(ConfigType.Broker(), String.valueOf(brokerId));
    Object oldLeaderThrottle = config.remove(LEADER_THROTTLED_RATE);
    Object oldFollowerThrottle = config.remove(FOLLOWER_THROTTLED_RATE);
    if (oldLeaderThrottle != null) {
      LOG.debug("Removing leader throttle on broker {}", brokerId);
    }
    if (oldFollowerThrottle != null) {
      LOG.debug("Removing follower throttle on broker {}", brokerId);
    }
    if (oldLeaderThrottle != null || oldFollowerThrottle != null) {
      ExecutorUtils.changeBrokerConfig(_adminZkClient, brokerId, config);
    }
  }
}
