/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;


/**
 * Util class for convenience.
 */
public class KafkaCruiseControlUtils {
  public static final int ZK_SESSION_TIMEOUT = 30000;
  public static final int ZK_CONNECTION_TIMEOUT = 30000;
  public static final long KAFKA_ZK_CLIENT_CLOSE_TIMEOUT_MS = 10000;

  private KafkaCruiseControlUtils() {

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

  /**
   * Close the given KafkaZkClient with the default timeout of {@link #KAFKA_ZK_CLIENT_CLOSE_TIMEOUT_MS}.
   *
   * @param kafkaZkClient KafkaZkClient to be closed
   */
  public static void closeKafkaZkClientWithTimeout(KafkaZkClient kafkaZkClient) {
    closeKafkaZkClientWithTimeout(kafkaZkClient, KAFKA_ZK_CLIENT_CLOSE_TIMEOUT_MS);
  }

  public static void closeKafkaZkClientWithTimeout(KafkaZkClient kafkaZkClient, long timeoutMs) {
    Thread t = new Thread() {
      @Override
      public void run() {
        kafkaZkClient.close();
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
   * @return True if substates contain {@link KafkaCruiseControlState.SubState#ANALYZER} or
   * {@link KafkaCruiseControlState.SubState#MONITOR}, false otherwise.
   */
  public static boolean shouldRefreshClusterAndGeneration(Set<KafkaCruiseControlState.SubState> substates) {
    return substates.stream()
                    .anyMatch(substate -> substate == KafkaCruiseControlState.SubState.ANALYZER
                                          || substate == KafkaCruiseControlState.SubState.MONITOR);
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
   * @param bootstrapServers Bootstrap servers that the underlying AdminClient will use.
   * @param brokers Brokers for which the logDirs will be described.
   * @return DescribeLogDirsResult using the given bootstrap servers for the given brokers.
   */
  public static DescribeLogDirsResult describeLogDirs(List<String> bootstrapServers, Collection<Integer> brokers) {
    try (AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(
        bootstrapServers.toString().replace(" ", "").replace("[", "").replace("]", ""))) {
      return adminClient.describeLogDirs(brokers);
    }
  }

  /**
   * Create an instance of AdminClient using the given bootstrap servers.
   *
   * @param bootstrapServers Bootstrap servers that the AdminClient will use.
   * @return A new instance of AdminClient.
   */
  public static AdminClient createAdminClient(String bootstrapServers) {
    Properties props = new Properties();
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    return AdminClient.create(props);
  }
}
