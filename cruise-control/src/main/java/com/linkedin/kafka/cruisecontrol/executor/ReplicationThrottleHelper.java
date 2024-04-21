/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
  // LogConfig class in Kafka 3.5+
  private static final String LOG_CONFIG_IN_KAFKA_3_5_AND_LATER = "org.apache.kafka.storage.internals.log.LogConfig";
  // LogConfig class in Kafka 3.4-
  private static final String LOG_CONFIG_IN_KAFKA_3_4_AND_EARLIER = "kafka.log.LogConfig";
  static final String LEADER_THROTTLED_REPLICAS = getLogConfig(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG);
  static final String FOLLOWER_THROTTLED_REPLICAS = getLogConfig(LogConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG);
  public static final long CLIENT_REQUEST_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
  static final int RETRIES = 30;

  private final AdminClient _adminClient;
  private final int _retries;
  private final Set<Integer> _deadBrokers;

  ReplicationThrottleHelper(AdminClient adminClient) {
    this(adminClient, RETRIES);
  }

  ReplicationThrottleHelper(AdminClient adminClient, Set<Integer> deadBrokers) {
    this(adminClient, RETRIES, deadBrokers);
  }

  // for testing
  ReplicationThrottleHelper(AdminClient adminClient, int retries) {
    this._adminClient = adminClient;
    this._retries = retries;
    this._deadBrokers = new HashSet<Integer>();
  }

  ReplicationThrottleHelper(AdminClient adminClient, int retries, Set<Integer> deadBrokers) {
    this._adminClient = adminClient;
    this._retries = retries;
    this._deadBrokers = deadBrokers;
  }

  void setThrottles(List<ExecutionProposal> replicaMovementProposals, long throttleRate)
  throws ExecutionException, InterruptedException, TimeoutException {
    LOG.info("Setting a rebalance throttle of {} bytes/sec", throttleRate);
    Set<Integer> participatingBrokers = getParticipatingBrokers(replicaMovementProposals);
    Map<String, Set<String>> throttledReplicas = getThrottledReplicasByTopic(replicaMovementProposals);
    for (int broker : participatingBrokers) {
      setReplicationThrottledRateIfNecessary(broker, throttleRate);
    }
    for (Map.Entry<String, Set<String>> entry : throttledReplicas.entrySet()) {
      setThrottledReplicas(entry.getKey(), entry.getValue());
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
  void clearThrottles(List<ExecutionTask> completedTasks, List<ExecutionTask> inProgressTasks)
  throws ExecutionException, InterruptedException, TimeoutException {
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
    for (int broker : brokersToRemoveThrottlesFrom) {
      removeReplicationThrottledRateFromBroker(broker);
    }

    Map<String, Set<String>> throttledReplicas = getThrottledReplicasByTopic(completedProposals);
    for (Map.Entry<String, Set<String>> entry : throttledReplicas.entrySet()) {
      removeThrottledReplicasFromTopic(entry.getKey(), entry.getValue());
    }
  }

  private Set<Integer> getParticipatingBrokers(List<ExecutionProposal> replicaMovementProposals) {
    Set<Integer> participatingBrokers = new TreeSet<>();
    for (ExecutionProposal proposal : replicaMovementProposals) {
      participatingBrokers.addAll(proposal.oldReplicas().stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet()));
      participatingBrokers.addAll(proposal.newReplicas().stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet()));
    }
    participatingBrokers.removeAll(_deadBrokers);
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

  private void setReplicationThrottledRateIfNecessary(int brokerId, long throttleRate) throws ExecutionException, InterruptedException, TimeoutException {
    Config brokerConfigs = getBrokerConfigs(brokerId);
    List<AlterConfigOp> ops = new ArrayList<>();
    for (String replicaThrottleRateConfigKey : Arrays.asList(LEADER_THROTTLED_RATE, FOLLOWER_THROTTLED_RATE)) {
      ConfigEntry currThrottleRate = brokerConfigs.get(replicaThrottleRateConfigKey);
      if (currThrottleRate == null || !currThrottleRate.value().equals(String.valueOf(throttleRate))) {
        LOG.debug("Setting {} to {} bytes/second for broker {}", replicaThrottleRateConfigKey, throttleRate, brokerId);
        ops.add(new AlterConfigOp(new ConfigEntry(replicaThrottleRateConfigKey, String.valueOf(throttleRate)), AlterConfigOp.OpType.SET));
      }
    }
    if (!ops.isEmpty()) {
      changeBrokerConfigs(brokerId, ops);
    }
  }

  private Config getTopicConfigs(String topic) throws ExecutionException, InterruptedException, TimeoutException {
    try {
      return getEntityConfigs(new ConfigResource(ConfigResource.Type.TOPIC, topic));
    } catch (Exception e) {
      if (!topicExists(topic)) {
        return new Config(Collections.emptyList());
      }
      throw e;
    }
  }

  private Config getBrokerConfigs(int brokerId) throws ExecutionException, InterruptedException, TimeoutException {
    return getEntityConfigs(new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId)));
  }

  private Config getEntityConfigs(ConfigResource cf) throws ExecutionException, InterruptedException, TimeoutException {
    Map<ConfigResource, Config> configs = _adminClient.describeConfigs(Collections.singletonList(cf)).all()
        .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    return configs.get(cf);
  }

  private void setThrottledReplicas(String topic, Set<String> replicas)
  throws ExecutionException, InterruptedException, TimeoutException {
    Config topicConfigs = getTopicConfigs(topic);
    List<AlterConfigOp> ops = new ArrayList<>();
    for (String replicaThrottleConfigKey : Arrays.asList(LEADER_THROTTLED_REPLICAS, FOLLOWER_THROTTLED_REPLICAS)) {
      ConfigEntry currThrottledReplicas = topicConfigs.get(replicaThrottleConfigKey);
      if (currThrottledReplicas != null && currThrottledReplicas.value().trim().equals(WILDCARD_ASTERISK)) {
        // The existing setup throttles all replica. So, nothing needs to be changed.
        continue;
      }

      // Merge new throttled replicas with existing configuration values.
      Set<String> newThrottledReplicas = new TreeSet<>(replicas);
      if (currThrottledReplicas != null && !currThrottledReplicas.value().equals("")) {
        newThrottledReplicas.addAll(Arrays.asList(currThrottledReplicas.value().split(",")));
      }
      ops.add(new AlterConfigOp(new ConfigEntry(replicaThrottleConfigKey, String.join(",", newThrottledReplicas)), AlterConfigOp.OpType.SET));
    }
    if (!ops.isEmpty()) {
      changeTopicConfigs(topic, ops);
    }
  }

  void changeTopicConfigs(String topic, Collection<AlterConfigOp> ops)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    Map<ConfigResource, Collection<AlterConfigOp>> configs = Collections.singletonMap(cf, ops);
    try {
      _adminClient.incrementalAlterConfigs(configs).all()
          .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      waitForConfigs(cf, ops);
    } catch (Exception e) {
      if (!topicExists(topic)) {
        LOG.debug("Failed to change configs for topic {} since it does not exist", topic);
        return;
      }
      throw e;
    }
  }

  void changeBrokerConfigs(int brokerId, Collection<AlterConfigOp> ops)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    Map<ConfigResource, Collection<AlterConfigOp>> configs = Collections.singletonMap(cf, ops);
    _adminClient.incrementalAlterConfigs(configs).all()
        .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    waitForConfigs(cf, ops);
  }

  boolean topicExists(String topic) throws InterruptedException, TimeoutException, ExecutionException {
    try {
      return _adminClient.listTopics().names().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).contains(topic);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      LOG.error("Unable to check if topic {} exists due to {}", topic, e.getMessage());
      throw e;
    }
  }

  static String removeReplicasFromConfig(String throttleConfig, Set<String> replicas) {
    List<String> throttles = new ArrayList<>(Arrays.asList(throttleConfig.split(",")));
    throttles.removeIf(replicas::contains);
    return String.join(",", throttles);
  }

  /**
   * It gets whether there is any throttled replica specified in the configuration property. If there is and the
   * specified throttled replica does not equal to "*", it modifies the configuration property by removing a
   * given set of replicas from the set of throttled replicas
   *
   * @param topic name of topic which contains <code>replicas</code>
   * @param replicas replicas to remove from the configuration properties
   */
  private void removeThrottledReplicasFromTopic(String topic, Set<String> replicas)
  throws ExecutionException, InterruptedException, TimeoutException {
    Config topicConfigs = getTopicConfigs(topic);
    if (topicConfigs == null) {
      LOG.debug("Skip removing throttled replicas {} from topic {} since no configs can be read", String.join(",", replicas), topic);
      return;
    }
    List<AlterConfigOp> ops = new ArrayList<>();

    ConfigEntry currentLeaderThrottledReplicas = topicConfigs.get(LEADER_THROTTLED_REPLICAS);
    if (currentLeaderThrottledReplicas != null) {
      if (currentLeaderThrottledReplicas.value().equals(WILDCARD_ASTERISK)) {
        LOG.debug("Existing config throttles all leader replicas. So, do not remove any leader replica throttle");
      } else {
        replicas.forEach(r -> LOG.debug("Removing leader throttles for topic {} and replica {}", topic, r));
        String newThrottledReplicas = removeReplicasFromConfig(currentLeaderThrottledReplicas.value(), replicas);
        if (newThrottledReplicas.isEmpty()) {
          ops.add(new AlterConfigOp(new ConfigEntry(LEADER_THROTTLED_REPLICAS, null), AlterConfigOp.OpType.DELETE));
        } else {
          ops.add(new AlterConfigOp(new ConfigEntry(LEADER_THROTTLED_REPLICAS, newThrottledReplicas), AlterConfigOp.OpType.SET));
        }
      }
    }
    ConfigEntry currentFollowerThrottledReplicas = topicConfigs.get(FOLLOWER_THROTTLED_REPLICAS);
    if (currentFollowerThrottledReplicas != null) {
      if (currentFollowerThrottledReplicas.value().equals(WILDCARD_ASTERISK)) {
        LOG.debug("Existing config throttles all follower replicas. So, do not remove any follower replica throttle");
      } else {
        replicas.forEach(r -> LOG.debug("Removing follower throttles for topic {} and replica {}", topic, r));
        String newThrottledReplicas = removeReplicasFromConfig(currentFollowerThrottledReplicas.value(), replicas);
        if (newThrottledReplicas.isEmpty()) {
          ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_THROTTLED_REPLICAS, null), AlterConfigOp.OpType.DELETE));
        } else {
          ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_THROTTLED_REPLICAS, newThrottledReplicas), AlterConfigOp.OpType.SET));
        }
      }
    }
    if (!ops.isEmpty()) {
      changeTopicConfigs(topic, ops);
    }
  }

  private void removeReplicationThrottledRateFromBroker(Integer brokerId)
  throws ExecutionException, InterruptedException, TimeoutException {
    Config brokerConfigs = getBrokerConfigs(brokerId);
    ConfigEntry currLeaderThrottle = brokerConfigs.get(LEADER_THROTTLED_RATE);
    ConfigEntry currFollowerThrottle = brokerConfigs.get(FOLLOWER_THROTTLED_RATE);
    List<AlterConfigOp> ops = new ArrayList<>();
    if (currLeaderThrottle != null) {
      if (currLeaderThrottle.source().equals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)) {
        LOG.debug("Skipping removal for static leader throttle rate: {}", currFollowerThrottle);
      } else {
        LOG.debug("Removing leader throttle rate: {} on broker {}", currLeaderThrottle, brokerId);
        ops.add(new AlterConfigOp(new ConfigEntry(LEADER_THROTTLED_RATE, null), AlterConfigOp.OpType.DELETE));
      }
    }
    if (currFollowerThrottle != null) {
      if (currFollowerThrottle.source().equals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)) {
        LOG.debug("Skipping removal for static follower throttle rate: {}", currFollowerThrottle);
      } else {
        LOG.debug("Removing follower throttle rate: {} on broker {}", currFollowerThrottle, brokerId);
        ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_THROTTLED_RATE, null), AlterConfigOp.OpType.DELETE));
      }
    }
    if (!ops.isEmpty()) {
      changeBrokerConfigs(brokerId, ops);
    }
  }

  // Retries until we can read the configs changes we just wrote
  void waitForConfigs(ConfigResource cf, Collection<AlterConfigOp> ops) {
    // Use HashMap::new instead of Collectors.toMap to allow inserting null values
    Map<String, String> expectedConfigs = ops.stream()
            .collect(HashMap::new, (m, o) -> m.put(o.configEntry().name(), o.configEntry().value()), HashMap::putAll);
    boolean retryResponse = CruiseControlMetricsUtils.retry(() -> {
      try {
        return !configsEqual(getEntityConfigs(cf), expectedConfigs);
      } catch (ExecutionException | InterruptedException | TimeoutException e) {
        return false;
      }
    }, _retries);
    if (!retryResponse) {
      throw new IllegalStateException("The following configs " + ops + " were not applied to " + cf + " within the time limit");
    }
  }

  static boolean configsEqual(Config configs, Map<String, String> expectedValues) {
    for (Map.Entry<String, String> entry : expectedValues.entrySet()) {
      ConfigEntry configEntry = configs.get(entry.getKey());
      if (configEntry == null || configEntry.value() == null || configEntry.value().isEmpty()) {
        if (entry.getValue() != null) {
          return false;
        }
      } else if (configEntry.source().equals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG) && entry.getValue() == null) {
        LOG.debug("Found static broker config: {}, skipping comparison", configEntry);
      } else if (!Objects.equals(entry.getValue(), configEntry.value())) {
        return false;
      }
    }
    return true;
  }

  private enum LogConfig {
    LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
    FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
  }

  /**
   * Starting with Kafka 3.5.0, the location of the "LogConfig" class is "org.apache.kafka.storage.internals.log.LogConfig"
   * and provides new constant fields in place of some methods of the original class.
   *
   *   - LogConfig class in Kafka 3.5+: org.apache.kafka.storage.internals.log.LogConfig
   *   - LogConfig class in Kafka 3.4-: kafka.log.LogConfig
   **
   * The older LogConfig class does not work with the newer versions of Kafka. Therefore, if the new class exists, we use it and if
   * it doesn't exist we will fall back on the older one.
   *
   * Once CC supports only 3.5.0 and newer, we can clean this up and use the LogConfig class from
   * `org.apache.kafka.storage.internals.log.LogConfig`all the time.
   * @param config LogConfig config name
   * @return LogConfig config name in the format of a Kafka configuration property
   */
  private static String getLogConfig(LogConfig config) {
    Class<?> logConfigClass;

    try {
      // First we try to get the LogConfig class for Kafka 3.5+
      logConfigClass = Class.forName(LOG_CONFIG_IN_KAFKA_3_5_AND_LATER);

      Field field = logConfigClass.getDeclaredField(config.toString());
      return field.get(null).toString();
    } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
      LOG.info("Failed to read config {} from LogConfig class since we are probably on kafka 3.4 or older: {}", config, e);
    }

    // We did not find the LogConfig class or field from Kafka 3.5+.
    // So we are probably on older Kafka version => we will try the older class for Kafka 3.4-.
    try {
      logConfigClass = Class.forName(LOG_CONFIG_IN_KAFKA_3_4_AND_EARLIER);

      String nameOfMethod = "";
      if (config == LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG) {
        nameOfMethod = "LeaderReplicationThrottledReplicasProp";
      } else if (config == LogConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG) {
        nameOfMethod = "FollowerReplicationThrottledReplicasProp";
      }

      Method method = logConfigClass.getMethod(nameOfMethod);
      return method.invoke(null).toString();
      } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      // No class or method was found for any Kafka version => we should fail
      throw new RuntimeException("Failed to read config " + config + " from LogConfig class:", e);
      }
    }
}
