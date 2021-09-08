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
  static final String WILDCARD_ASTERISK = "*";
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
      task.state() != ExecutionTaskState.IN_PROGRESS
      // the task should not be pending
      && task.state() != ExecutionTaskState.PENDING
      // replica throttles only apply to inter-broker replica movement
      && task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
  }

  // determines if a candidate task is in progress and related to inter-broker
  // replica movement.
  boolean taskIsInProgress(ExecutionTask task) {
    return task.state() == ExecutionTaskState.IN_PROGRESS && task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
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
        .computeIfAbsent(topic, x -> new TreeSet<>());
      brokers.forEach(brokerId -> throttledReplicas.add(partitionId + ":" + brokerId));
    }
    return throttledReplicasByTopic;
  }

  private void setLeaderThrottledRateIfUnset(int brokerId) {
    setThrottledRateIfUnset(brokerId, true);
  }

  private void setFollowerThrottledRateIfUnset(int brokerId) {
    setThrottledRateIfUnset(brokerId, false);
  }

  private void setThrottledRateIfUnset(int brokerId, boolean throttleLeaderReplicaRate) {
    if (_throttleRate == null) {
      throw new IllegalStateException("Throttle rate cannot be null");
    }
    String replicaThrottleRateConfigKey = throttleLeaderReplicaRate ? LEADER_THROTTLED_RATE : FOLLOWER_THROTTLED_RATE;
    Properties brokerConfigs = _kafkaZkClient.getEntityConfigs(ConfigType.Broker(), String.valueOf(brokerId));
    Object currThrottleRate = brokerConfigs.setProperty(replicaThrottleRateConfigKey, String.valueOf(_throttleRate));
    if (currThrottleRate == null) {
      LOG.debug("Setting {} to {} bytes/second for broker {}", replicaThrottleRateConfigKey, _throttleRate, brokerId);
      ExecutorUtils.changeBrokerConfig(_adminZkClient, brokerId, brokerConfigs);
    } else {
      LOG.debug("Not setting {} for broker {} because pre-existing throttle of {} was already set",
          replicaThrottleRateConfigKey, brokerId, currThrottleRate);
    }
  }

  private void setLeaderThrottledReplicas(String topic, Set<String> replicas) {
    setThrottledReplicas(topic, replicas, true);
  }

  private void setFollowerThrottledReplicas(String topic, Set<String> replicas) {
    setThrottledReplicas(topic, replicas, false);
  }

  private void setThrottledReplicas(String topic, Set<String> replicas, boolean throttleLeaderReplica) {
    String replicaThrottleConfigKey = throttleLeaderReplica ? LEADER_THROTTLED_REPLICAS : FOLLOWER_THROTTLED_REPLICAS;
    Properties topicConfigs = _kafkaZkClient.getEntityConfigs(ConfigType.Topic(), topic);
    String currThrottledReplicas = topicConfigs.getProperty(replicaThrottleConfigKey);
    if (currThrottledReplicas != null && currThrottledReplicas.trim().equals(WILDCARD_ASTERISK)) {
      // The existing setup throttles all replica. So, nothing needs to be changed.
      return;
    }

    // Merge new throttled replicas with existing configuration values.
    Set<String> newThrottledReplicas = new TreeSet<>(replicas);
    if (currThrottledReplicas != null) {
      newThrottledReplicas.addAll(Arrays.asList(currThrottledReplicas.split(",")));
    }
    topicConfigs.setProperty(replicaThrottleConfigKey, String.join(",", newThrottledReplicas));
    changeTopicConfigs(topic, topicConfigs);
  }

  /**
   * Update topic configuration properties if a topic exist. No-op for non-existent topic
   *
   * @param topicName topic name
   * @param topicConfigs configuration properties to update
   */
  private void changeTopicConfigs(String topicName, Properties topicConfigs) {
    try {
      ExecutorUtils.changeTopicConfig(_adminZkClient, topicName, topicConfigs);
    } catch (Exception e) {
      if (!_kafkaZkClient.topicExists(topicName)) {
        LOG.debug("Failed to change configs for topic {} since it does not exist", topicName);
        return;
      }
      throw e;
    }
  }

  static String removeReplicasFromConfig(String throttleConfig, Set<String> replicas) {
    ArrayList<String> throttles = new ArrayList<>(Arrays.asList(throttleConfig.split(",")));
    throttles.removeIf(replicas::contains);
    return String.join(",", throttles);
  }

  /**
   * It gets whether there is any throttled leader replica specified in the configuration property. If there is and the
   * specified throttled leader replica does not equal to "*", it modifies the configuration property by removing a
   * given set of replicas from the a set of throttled leader replica
   *
   * @param config configuration properties
   * @param topic name of topic which contains <code>replicas</code>
   * @param replicas replicas to remove from the configuration properties
   * @return {@code true} if the given configuration properties are modified and {@code false} otherwise
   */
  private boolean removeLeaderThrottledReplicasFromTopic(Properties config, String topic, Set<String> replicas) {
    String currLeaderThrottledReplicas = config.getProperty(LEADER_THROTTLED_REPLICAS);
    if (currLeaderThrottledReplicas != null) {
      if (currLeaderThrottledReplicas.equals(WILDCARD_ASTERISK)) {
        LOG.debug("Existing config throttles all leader replicas. So, do not remove any leader replica throttle");
        return false;
      }

      replicas.forEach(r -> LOG.debug("Removing leader throttles for topic {} on replica {}", topic, r));
      String newLeaderThrottledReplicas = removeReplicasFromConfig(currLeaderThrottledReplicas, replicas);
      if (newLeaderThrottledReplicas.isEmpty()) {
        config.remove(LEADER_THROTTLED_REPLICAS);
      } else {
        config.setProperty(LEADER_THROTTLED_REPLICAS, newLeaderThrottledReplicas);
      }
      return true;
    }
    return false;
  }

  /**
   * It gets whether there is any throttled follower replica specified in the configuration property. If there is and the
   * specified throttled follower replica does not equal to "*", it modifies the configuration property by removing a
   * given set of replicas from the a set of throttled follower replica
   *
   * @param config configuration properties
   * @param topic name of topic which contains <code>replicas</code>
   * @param replicas replicas to remove from the configuration properties
   * @return {@code true} if the given configuration properties are modified and {@code false} otherwise
   */
  private boolean removeFollowerThrottledReplicasFromTopic(Properties config, String topic, Set<String> replicas) {
    String currLeaderThrottledReplicas = config.getProperty(FOLLOWER_THROTTLED_REPLICAS);
    if (currLeaderThrottledReplicas != null) {
      if (currLeaderThrottledReplicas.equals(WILDCARD_ASTERISK)) {
        LOG.debug("Existing config throttles all follower replicas. So, do not remove any follower replica throttle");
        return false;
      }

      replicas.forEach(r -> LOG.debug("Removing follower throttles for topic {} and replica {}", topic, r));
      String newLeaderThrottledReplicas = removeReplicasFromConfig(currLeaderThrottledReplicas, replicas);
      if (newLeaderThrottledReplicas.isEmpty()) {
        config.remove(FOLLOWER_THROTTLED_REPLICAS);
      } else {
        config.setProperty(FOLLOWER_THROTTLED_REPLICAS, newLeaderThrottledReplicas);
      }
      return true;
    }
    return false;
  }

  private void removeThrottledReplicasFromTopic(String topic, Set<String> replicas) {
    Properties topicConfigs = _kafkaZkClient.getEntityConfigs(ConfigType.Topic(), topic);
    if (topicConfigs.isEmpty()) {
      LOG.debug("Skip removing throttled replicas {} from topic {} since no configs can be read", String.join(",", replicas), topic);
      return;
    }
    boolean removedLeaderThrottle = removeLeaderThrottledReplicasFromTopic(topicConfigs, topic, replicas);
    boolean removedFollowerThrottle = removeFollowerThrottledReplicasFromTopic(topicConfigs, topic, replicas);

    if (removedLeaderThrottle || removedFollowerThrottle) {
      changeTopicConfigs(topic, topicConfigs);
    }
  }

  private void removeThrottledRateFromBroker(Integer brokerId) {
    Properties brokerConfigs = _kafkaZkClient.getEntityConfigs(ConfigType.Broker(), String.valueOf(brokerId));
    Object currLeaderThrottle = brokerConfigs.get(LEADER_THROTTLED_RATE);
    Object currFollowerThrottle = brokerConfigs.get(FOLLOWER_THROTTLED_RATE);
    boolean configChange = false;

    if (currLeaderThrottle != null) {
      if (currLeaderThrottle.equals(WILDCARD_ASTERISK)) {
        LOG.debug("Existing config throttles all leader replicas. So, do not remove any leader replica throttle on broker {}", brokerId);
      } else {
        LOG.debug("Removing leader throttle on broker {}", brokerId);
        brokerConfigs.remove(LEADER_THROTTLED_RATE);
        configChange = true;
      }
    }
    if (currFollowerThrottle != null) {
      if (currFollowerThrottle.equals(WILDCARD_ASTERISK)) {
        LOG.debug("Existing config throttles all follower replicas. So, do not remove any follower replica throttle on broker {}", brokerId);
      } else {
        LOG.debug("Removing follower throttle on broker {}", brokerId);
        brokerConfigs.remove(FOLLOWER_THROTTLED_RATE);
        configChange = true;
      }
    }
    if (configChange) {
      ExecutorUtils.changeBrokerConfig(_adminZkClient, brokerId, brokerConfigs);
    }
  }
}
