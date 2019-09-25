/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.TimeZone;
import java.util.stream.Collectors;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Util class for convenience.
 */
public class KafkaCruiseControlUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlUtils.class);
  public static final double MAX_BALANCEDNESS_SCORE = 100.0;
  public static final int ZK_SESSION_TIMEOUT = 30000;
  public static final int ZK_CONNECTION_TIMEOUT = 30000;
  public static final String DATE_FORMAT = "YYYY-MM-dd_HH:mm:ss z";
  public static final String DATE_FORMAT2 = "dd/MM/yyyy HH:mm:ss";
  public static final String TIME_ZONE = "UTC";
  public static final int SEC_TO_MS = 1000;
  private static final int MIN_TO_MS = SEC_TO_MS * 60;
  private static final int HOUR_TO_MS = MIN_TO_MS * 60;
  private static final int DAY_TO_MS = HOUR_TO_MS * 24;
  private static final Set<String> KAFKA_ASSIGNER_GOALS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(KafkaAssignerEvenRackAwareGoal.class.getSimpleName(),
                                                              KafkaAssignerDiskUsageDistributionGoal.class.getSimpleName())));
  public static final String OPERATION_LOGGER = "operationLogger";

  private KafkaCruiseControlUtils() {

  }

  public static String currentUtcDate() {
    return utcDateFor(System.currentTimeMillis());
  }

  public static String utcDateFor(long timeMs) {
    Date date = new Date(timeMs);
    DateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
    formatter.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
    return formatter.format(date);
  }

  /**
   * Format the timestamp from long to a human readable string.
   */
  public static String toDateString(long time) {
    return toDateString(time, DATE_FORMAT2, "");
  }

  /**
   * Format the timestamp from long to human readable string. Allow customization of date format and time zone.
   * @param time time in milliseconds
   * @param dateFormat see formats above
   * @param timeZone will use default if timeZone is set to empty string
   * @return string representation of date
   */
  public static String toDateString(long time, String dateFormat, String timeZone) {
    if (time < 0) {
      throw new IllegalArgumentException(String.format("Attempt to convert negative time %d to date.", time));
    }
    DateFormat formatter = new SimpleDateFormat(dateFormat);
    if (!timeZone.isEmpty()) {
      formatter.setTimeZone(TimeZone.getTimeZone(timeZone));
    }
    return formatter.format(new Date(time));
  }

  /**
   * Format the duration from double to human readable string.
   * @param durationMs Duration in milliseconds
   * @return String representation of duration
   */
  public static String toPrettyDuration(double durationMs) {
    if (durationMs < 0) {
      throw new IllegalArgumentException(String.format("Duration cannot be negative value, get %f", durationMs));
    }

    // If the duration is less than one second, represent in milliseconds.
    if (durationMs < SEC_TO_MS) {
      return String.format("%.2f milliseconds", durationMs);
    }

    // If the duration is less than one minute, represent in seconds.
    if (durationMs < MIN_TO_MS) {
      return String.format("%.2f seconds", durationMs / SEC_TO_MS);
    }

    // If the duration is less than one hour, represent in minutes.
    if (durationMs < HOUR_TO_MS) {
      return String.format("%.2f minutes", durationMs / MIN_TO_MS);
    }

    // If the duration is less than one day, represent in hours.
    if (durationMs < DAY_TO_MS) {
      return String.format("%.2f hours", durationMs / HOUR_TO_MS);
    }

    // Represent in days.
    return String.format("%.2f days", durationMs / DAY_TO_MS);
  }

  /**
   * Get a configuration and throw exception if the configuration was not provided.
   * @param configs the config map.
   * @param configName the config to get.
   * @return the configuration string.
   */
  public static String getRequiredConfig(Map<String, ?> configs, String configName) {
    String value = (String) configs.get(configName);
    if (value == null || value.isEmpty()) {
      throw new ConfigException(String.format("Configuration %s must be provided.", configName));
    }
    return value;
  }

  public static ZkUtils createZkUtils(String zkConnect, boolean zkSecurityEnabled) {
    return ZkUtils.apply(zkConnect, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, zkSecurityEnabled);
  }

  public static void closeZkUtilsWithTimeout(ZkUtils zkUtils, long timeoutMs) {
    Thread t = new Thread() {
      @Override
      public void run() {
        zkUtils.close();
      }
    };
    t.setDaemon(true);
    t.start();
    try {
      t.join(timeoutMs);
    } catch (InterruptedException e) {
      // let it go
    }
    if (t.isAlive()) {
      t.interrupt();
    }
  }

  /**
   * Check if set a contains any element in set b.
   * @param a the first set.
   * @param b the second set.
   * @return true if a contains at least one of the element in b. false otherwise;
   */
  public static boolean containsAny(Set<Integer> a, Set<Integer> b) {
    return b.stream().mapToInt(i -> i).anyMatch(a::contains);
  }

  /**
   * Check if the ClusterAndGeneration needs to be refreshed to retrieve the requested substates.
   *
   * @param substates Substates for which the need for refreshing the ClusterAndGeneration will be evaluated.
   * @return True if substates contain {@link CruiseControlState.SubState#ANALYZER} or
   * {@link CruiseControlState.SubState#MONITOR}, false otherwise.
   */
  public static boolean shouldRefreshClusterAndGeneration(Set<CruiseControlState.SubState> substates) {
    return substates.stream()
                    .anyMatch(substate -> substate == CruiseControlState.SubState.ANALYZER
                                          || substate == CruiseControlState.SubState.MONITOR);
  }

  /**
   * Check if the partition is currently under replicated.
   * @param cluster The current cluster state.
   * @param tp The topic partition to check.
   * @return True if the partition is currently under replicated.
   */
  public static boolean isPartitionUnderReplicated(Cluster cluster, TopicPartition tp) {
    PartitionInfo partitionInfo = cluster.partition(tp);
    return partitionInfo.inSyncReplicas().length != partitionInfo.replicas().length;
  }

  /**
   * Sanity check there is no offline replica in the cluster.
   * @param cluster The current cluster state.
   */
  public static void sanityCheckNoOfflineReplica(Cluster cluster) {
    List<Node> aliveNodes = cluster.nodes();
    for (String topic : cluster.topics()) {
      for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
        Set<Integer> offlineReplicas = Arrays.stream(partitionInfo.replicas())
                                             .filter(node -> !aliveNodes.contains(node))
                                             .mapToInt(Node::id)
                                             .boxed()
                                             .collect(Collectors.toSet());
        if (offlineReplicas.size() > 0) {
          throw new IllegalStateException(String.format("Topic partition %s-%d has offline replicas on brokers %s",
                                                        partitionInfo.topic(), partitionInfo.partition(), offlineReplicas));
        }
      }
    }
  }

  /**
   * Sanity check whether the given goals exist in the given supported goals.
   * @param goals A list of goals.
   * @param supportedGoals Supported goals.
   */
  public static void sanityCheckNonExistingGoal(List<String> goals, Map<String, Goal> supportedGoals) {
    Set<String> nonExistingGoals = new HashSet<>();
    goals.stream().filter(goalName -> supportedGoals.get(goalName) == null).forEach(nonExistingGoals::add);

    if (!nonExistingGoals.isEmpty()) {
      throw new IllegalArgumentException("Goals " + nonExistingGoals + " are not supported. Supported: " + supportedGoals.keySet());
    }
  }

  /**
   * Check whether any of the given goals contain a Kafka Assigner goal.
   *
   * @param goals The goals to check
   * @return True if the given goals contain a Kafka Assigner goal, false otherwise.
   */
  public static boolean isKafkaAssignerMode(Collection<String> goals) {
    return goals.stream().anyMatch(KAFKA_ASSIGNER_GOALS::contains);
  }

  /**
   * Check that only one target replication factor is set for each topic.
   *
   * @param topicsToChangeByReplicationFactor Topics to change replication factor by target replication factor.
   */
  private static void sanityCheckTargetReplicationFactorForTopic(Map<Short, Set<String>> topicsToChangeByReplicationFactor) {
    Set<String> topicsToChange = new HashSet<>();
    Set<String> topicsHavingMultipleTargetReplicationFactors = new HashSet<>();
    for (Set<String> topics : topicsToChangeByReplicationFactor.values()) {
      for (String topic : topics) {
        if (!topicsToChange.add(topic)) {
          topicsHavingMultipleTargetReplicationFactors.add(topic);
        }
      }
    }
    if (!topicsHavingMultipleTargetReplicationFactors.isEmpty()) {
      throw new IllegalStateException(String.format("Topics %s are requested with more than one target replication factor.",
                                                    topicsHavingMultipleTargetReplicationFactors));
    }
  }

  /**
   * Populate topics to change replication factor based on the request and current cluster state.
   * @param topicPatternByReplicationFactor Requested topic patterns to change replication factor by target replication factor.
   * @param cluster Current cluster state.
   * @return Topics to change replication factor by target replication factor.
   */
  public static Map<Short, Set<String>> topicsForReplicationFactorChange(Map<Short, Pattern> topicPatternByReplicationFactor,
                                                                         Cluster cluster) {
    Map<Short, Set<String>> topicsToChangeByReplicationFactor = new HashMap<>(topicPatternByReplicationFactor.size());
    for (Map.Entry<Short, Pattern> entry : topicPatternByReplicationFactor.entrySet()) {
      short replicationFactor = entry.getKey();
      Pattern topicPattern = entry.getValue();
      Set<String> topics = cluster.topics().stream().filter(t -> topicPattern.matcher(t).matches()).collect(Collectors.toSet());
      // Ensure there are topics matching the requested topic pattern.
      if (topics.isEmpty()) {
        throw new IllegalStateException(String.format("There is no topic in cluster matching pattern '%s'.", topicPattern));
      }
      Set<String> topicsToChange = topics.stream()
                                         .filter(t -> cluster.partitionsForTopic(t).stream().anyMatch(p -> p.replicas().length != replicationFactor))
                                         .collect(Collectors.toSet());
      if (!topicsToChange.isEmpty()) {
        topicsToChangeByReplicationFactor.put(replicationFactor, topicsToChange);
      }
    }

    if (topicsToChangeByReplicationFactor.isEmpty()) {
      throw new IllegalStateException(String.format("All topics matching given pattern already have target replication factor. Requested "
                                                    + "topic pattern by replication factor: %s.", topicPatternByReplicationFactor));
    }
    // Sanity check that no topic is set with more than one target replication factor.
    sanityCheckTargetReplicationFactorForTopic(topicsToChangeByReplicationFactor);
    return topicsToChangeByReplicationFactor;
  }

  /**
   * Populate cluster rack information for topics to change replication factor. In the process this method also conducts a sanity
   * check to ensure that there are enough racks in the cluster to allocate new replicas to racks which do not host replica
   * of the same partition.
   *
   * @param topicsByReplicationFactor Topics to change replication factor by target replication factor.
   * @param cluster Current cluster state.
   * @param excludedBrokersForReplicaMove Set of brokers which do not host new replicas.
   * @param skipTopicRackAwarenessCheck Whether to skip the rack awareness sanity check or not.
   * @param brokersByRack Mapping from rack to broker.
   * @param rackByBroker Mapping from broker to rack.
   */
  public static void populateRackInfoForReplicationFactorChange(Map<Short, Set<String>> topicsByReplicationFactor,
                                                                Cluster cluster,
                                                                Set<Integer> excludedBrokersForReplicaMove,
                                                                boolean skipTopicRackAwarenessCheck,
                                                                Map<String, List<Integer>> brokersByRack,
                                                                Map<Integer, String> rackByBroker) {
    for (Node node : cluster.nodes()) {
      // New follower replica is not assigned to brokers excluded for replica movement.
      if (excludedBrokersForReplicaMove.contains(node.id())) {
        continue;
      }
      // If the rack is not specified, we use the broker id info as rack info.
      String rack = node.rack() == null || node.rack().isEmpty() ? String.valueOf(node.id()) : node.rack();
      brokersByRack.putIfAbsent(rack, new ArrayList<>());
      brokersByRack.get(rack).add(node.id());
      rackByBroker.put(node.id(), rack);
    }

    topicsByReplicationFactor.forEach((replicationFactor, topics) -> {
      if (replicationFactor > brokersByRack.size()) {
        if (skipTopicRackAwarenessCheck) {
          LOG.info("Target replication factor for topics {} is {}, which is larger than number of racks in cluster. Rack-awareness "
                   + "property will be violated to add new replicas.", topics, replicationFactor);
        } else {
          throw new RuntimeException(String.format("Unable to change replication factor of topics %s to %d since there are only %d "
                                                   + "racks in the cluster, to skip the rack-awareness check, set %s to true in the request.",
                                                   topics, replicationFactor, brokersByRack.size(), ParameterUtils.SKIP_RACK_AWARENESS_CHECK_PARAM));
        }
      }
    });
  }

  /**
   * Get the balancedness cost of violating goals by their name, where the sum of costs is {@link #MAX_BALANCEDNESS_SCORE}.
   *
   * @param goals The goals to be used for balancing (sorted by priority).
   * @param priorityWeight The impact of having one level higher goal priority on the relative balancedness score.
   * @param strictnessWeight The impact of strictness on the relative balancedness score.
   * @return the balancedness cost of violating goals by their name.
   */
  public static Map<String, Double> balancednessCostByGoal(List<Goal> goals, double priorityWeight, double strictnessWeight) {
    if (goals.isEmpty()) {
      throw new IllegalArgumentException("At least one goal must be provided to get the balancedness cost.");
    } else if (priorityWeight <= 0 || strictnessWeight <= 0) {
      throw new IllegalArgumentException(String.format("Balancedness weights must be positive (priority:%f, strictness:%f).",
                                                       priorityWeight, strictnessWeight));
    }
    Map<String, Double> balancednessCostByGoal = new HashMap<>(goals.size());
    // Step-1: Get weights.
    double weightSum = 0.0;
    double previousGoalPriorityWeight = (1 / priorityWeight);
    for (int i = goals.size() - 1; i >= 0; i--) {
      Goal goal = goals.get(i);
      double currentGoalPriorityWeight = priorityWeight * previousGoalPriorityWeight;
      double cost = currentGoalPriorityWeight * (goal.isHardGoal() ? strictnessWeight : 1);
      weightSum += cost;
      balancednessCostByGoal.put(goal.name(), cost);
      previousGoalPriorityWeight = currentGoalPriorityWeight;
    }

    // Step-2: Set costs.
    for (Map.Entry<String, Double> entry : balancednessCostByGoal.entrySet()) {
      entry.setValue(MAX_BALANCEDNESS_SCORE * entry.getValue() / weightSum);
    }

    return balancednessCostByGoal;
  }
}
