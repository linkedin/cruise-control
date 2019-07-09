/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SystemTime;


/**
 * Util class for convenience.
 */
public class KafkaCruiseControlUtils {
  public static final int ZK_SESSION_TIMEOUT = 30000;
  public static final int ZK_CONNECTION_TIMEOUT = 30000;
  public static final long KAFKA_ZK_CLIENT_CLOSE_TIMEOUT_MS = 10000;
  public static final long ADMIN_CLIENT_CLOSE_TIMEOUT_MS = 10000;
  public static final long LOGDIR_RESPONSE_TIMEOUT_MS = 10000;
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
    Date date = new Date(System.currentTimeMillis());
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

  private static void closeClientWithTimeout(Runnable clientCloseTask, long timeoutMs) {
    Thread t = new Thread(clientCloseTask);
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
   * Close the given KafkaZkClient with the default timeout of {@link #KAFKA_ZK_CLIENT_CLOSE_TIMEOUT_MS}.
   *
   * @param kafkaZkClient KafkaZkClient to be closed
   */
  public static void closeKafkaZkClientWithTimeout(KafkaZkClient kafkaZkClient) {
    closeKafkaZkClientWithTimeout(kafkaZkClient, KAFKA_ZK_CLIENT_CLOSE_TIMEOUT_MS);
  }

  public static void closeKafkaZkClientWithTimeout(KafkaZkClient kafkaZkClient, long timeoutMs) {
    closeClientWithTimeout(kafkaZkClient::close, timeoutMs);
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
   * Create an instance of KafkaZkClient with security disabled.
   *
   * @param connectString Comma separated host:port pairs, each corresponding to a zk server
   * @param metricGroup Metric group
   * @param metricType Metric type
   * @param zkSecurityEnabled True if zkSecurityEnabled, false otherwise.
   * @return A new instance of KafkaZkClient
   */
  public static KafkaZkClient createKafkaZkClient(String connectString, String metricGroup, String metricType, boolean zkSecurityEnabled) {
    return KafkaZkClient.apply(connectString, zkSecurityEnabled, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, Integer.MAX_VALUE,
        new SystemTime(), metricGroup, metricType);
  }

  /**
   * Describe LogDirs using the given bootstrap servers for the given brokers.
   *
   * @param brokers Brokers for which the logDirs will be described.
   * @param adminClientConfigs Configurations used for the AdminClient.
   * @return DescribeLogDirsResult using the given bootstrap servers for the given brokers.
   */
  public static DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, Map<String, Object> adminClientConfigs) {
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(adminClientConfigs);
    try {
      return adminClient.describeLogDirs(brokers);
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }
  }

  /**
   * Create an instance of AdminClient using the given configurations.
   *
   * @param adminClientConfigs Configurations used for the AdminClient.
   * @return A new instance of AdminClient.
   */
  public static AdminClient createAdminClient(Map<String, Object> adminClientConfigs) {
    return AdminClient.create(adminClientConfigs);
  }

  /**
   * Close the given AdminClient with the default timeout of {@link #ADMIN_CLIENT_CLOSE_TIMEOUT_MS}.
   *
   * @param adminClient AdminClient to be closed
   */
  public static void closeAdminClientWithTimeout(AdminClient adminClient) {
    closeAdminClientWithTimeout(adminClient, ADMIN_CLIENT_CLOSE_TIMEOUT_MS);
  }

  public static void closeAdminClientWithTimeout(AdminClient adminClient, long timeoutMs) {
    closeClientWithTimeout(adminClient::close, timeoutMs);
  }

  /**
   * Parse AdminClient configs based on the given {@link KafkaCruiseControlConfig configs}.
   *
   * @param configs Configs to be used for parsing AdminClient configs.
   * @return AdminClient configs.
   */
  public static Map<String, Object> parseAdminClientConfigs(KafkaCruiseControlConfig configs) {
    Map<String, Object> adminClientConfigs = new HashMap<>();
    // Add bootstrap server.
    List<String> bootstrapServers = configs.getList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
    String bootstrapServersString = bootstrapServers.toString()
        .replace(" ", "")
        .replace("[", "")
        .replace("]", "");
    adminClientConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersString);

    // Add security protocol (if specified).
    try {
      String securityProtocol = configs.getString(AdminClientConfig.SECURITY_PROTOCOL_CONFIG);
      adminClientConfigs.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
      setStringConfigIfExists(configs, adminClientConfigs, SaslConfigs.SASL_MECHANISM);
      setPasswordConfigIfExists(configs, adminClientConfigs, SaslConfigs.SASL_JAAS_CONFIG);

      // Configure SSL configs (if security protocol is SSL or SASL_SSL)
      if (securityProtocol.equals(SecurityProtocol.SSL.name) || securityProtocol.equals(SecurityProtocol.SASL_SSL.name)) {
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        setPasswordConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        setPasswordConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        setPasswordConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
      }
    } catch (ConfigException ce) {
      // let it go.
    }

    return adminClientConfigs;
  }

  private static void setPasswordConfigIfExists(KafkaCruiseControlConfig configs, Map<String, Object> props, String name) {
    try {
      props.put(name, configs.getPassword(name));
    } catch (ConfigException ce) {
      // let it go.
    }
  }

  private static void setStringConfigIfExists(KafkaCruiseControlConfig configs, Map<String, Object> props, String name) {
    try {
      props.put(name, configs.getString(name));
    } catch (ConfigException ce) {
      // let it go.
    }
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
   * Compare and ensure two sets are disjoint.
   * @param set1 The first set to compare.
   * @param set2 The second set to compare.
   * @param message The exception's detailed message if two sets are not disjoint.
   * @param <E> The type of elements maintained by the sets.
   */
  public static <E> void ensureDisJoint(Set<E> set1, Set<E> set2, String message) {
    Set<E> interSection = new HashSet<>(set1);
    interSection.retainAll(set2);
    if (!interSection.isEmpty()) {
      throw new IllegalStateException(message);
    }
  }


  /**
   * Sanity check there is no offline replica in the cluster.
   * @param cluster The current cluster state.
   */
  public static void sanityCheckNoOfflineReplica(Cluster cluster) {
    for (String topic : cluster.topics()) {
      for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
        if (partitionInfo.offlineReplicas().length > 0) {
          throw new IllegalStateException(String.format("Topic partition %s-%d has offline replicas on brokers %s",
                                                        partitionInfo.topic(), partitionInfo.partition(),
                                                        Arrays.stream(partitionInfo.offlineReplicas()).mapToInt(Node::id)
                                                              .boxed().collect(Collectors.toSet())));
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
   * Sanity check to ensure that the given cluster model contains brokers with offline replicas.
   * @param clusterModel Cluster model for which the existence of an offline replica will be verified.
   */
  public static void sanityCheckOfflineReplicaPresence(ClusterModel clusterModel) {
    if (clusterModel.brokersHavingOfflineReplicasOnBadDisks().isEmpty()) {
      for (Broker deadBroker : clusterModel.deadBrokers()) {
        if (!deadBroker.replicas().isEmpty()) {
          // Has offline replica(s) on a dead broker.
          return;
        }
      }
      throw new IllegalStateException("Cluster has no offline replica on brokers " + clusterModel.brokers() + " to fix.");
    }
    // Has offline replica(s) on a broken disk.
  }

  /**
   * Sanity check to ensure that the given cluster model has no offline replicas on bad disks in Kafka Assigner mode.
   * @param goals Goals to check whether it is Kafka Assigner mode or not.
   * @param clusterModel Cluster model for which the existence of an offline replicas on bad disks will be verified.
   */
  public static void sanityCheckBrokersHavingOfflineReplicasOnBadDisks(List<String> goals, ClusterModel clusterModel) {
    if (isKafkaAssignerMode(goals) && !clusterModel.brokersHavingOfflineReplicasOnBadDisks().isEmpty()) {
      throw new IllegalStateException("Kafka Assigner mode is not supported when there are offline replicas on bad disks."
                                      + " Please run fix_offline_replicas before using Kafka Assigner mode.");
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
