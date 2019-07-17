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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;


/**
 * Util class for convenience.
 */
public class KafkaCruiseControlUtils {
  public static final int ZK_SESSION_TIMEOUT = 30000;
  public static final int ZK_CONNECTION_TIMEOUT = 30000;
  public static final String DATE_FORMAT = "YYYY-MM-dd_HH:mm:ss z";
  public static final String DATE_FORMAT2 = "dd/MM/yyyy HH:mm:ss";
  public static final String TIME_ZONE = "UTC";
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
  public static String toDurationString(double durationMs) {
    // If the duration is less than one second, represent in milliseconds.
    if (durationMs < 1000) {
      return String.format("%.2f milliseconds", durationMs);
    }

    durationMs = durationMs / 1000;
    // If the duration is less than one minute, represent in seconds.
    if (durationMs < 60) {
      return String.format("%.2f seconds", durationMs);
    }

    durationMs = durationMs / 60;
    // If the duration is less than one hour, represent in minutes.
    if (durationMs < 60) {
      return String.format("%.2f minutes", durationMs);
    }

    durationMs = durationMs / 60;
    // If the duration is less than one day, represent in hours.
    if (durationMs < 24) {
      return String.format("%.2f hours", durationMs);
    }

    // Represent in days.
    durationMs = durationMs / 24;
    return String.format("%.2f days", durationMs);
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
}
