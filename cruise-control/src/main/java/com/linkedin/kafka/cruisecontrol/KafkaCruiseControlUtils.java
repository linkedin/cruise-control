/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *//*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import java.util.TimeZone;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
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
  public static final String DATA_FORMAT = "YYYY-MM-dd_HH:mm:ss z";
  public static final String TIME_ZONE = "UTC";

  private KafkaCruiseControlUtils() {

  }

  public static String currentUtcDate() {
    Date date = new Date(System.currentTimeMillis());
    DateFormat formatter = new SimpleDateFormat(DATA_FORMAT);
    formatter.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
    return formatter.format(date);
  }

  /**
   * Format the timestamp from long to a human readable string.
   */
  public static String toDateString(long time) {
    DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    return format.format(new Date(time));
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
    for (int i : b) {
      if (a.contains(i)) {
        return true;
      }
    }
    return false;
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
   * @return A new instance of KafkaZkClient
   */
  public static KafkaZkClient createKafkaZkClient(String connectString, String metricGroup, String metricType) {
    return KafkaZkClient.apply(connectString, false, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, Integer.MAX_VALUE,
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

      // Configure SSL configs (if security protocol is SSL)
      if (securityProtocol.equals(SecurityProtocol.SSL.name)) {
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
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
}
