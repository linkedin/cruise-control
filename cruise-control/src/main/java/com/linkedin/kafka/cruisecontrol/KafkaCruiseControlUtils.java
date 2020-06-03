/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
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
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SystemTime;
import scala.Option;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.SKIP_HARD_GOAL_CHECK_PARAM;


/**
 * Util class for convenience.
 */
public class KafkaCruiseControlUtils {
  public static final double MAX_BALANCEDNESS_SCORE = 100.0;
  public static final int ZK_SESSION_TIMEOUT = 30000;
  public static final int ZK_CONNECTION_TIMEOUT = 30000;
  public static final long KAFKA_ZK_CLIENT_CLOSE_TIMEOUT_MS = 10000;
  public static final long ADMIN_CLIENT_CLOSE_TIMEOUT_MS = 10000;
  public static final String DATE_FORMAT = "YYYY-MM-dd_HH:mm:ss z";
  public static final String DATE_FORMAT2 = "dd/MM/yyyy HH:mm:ss";
  public static final String TIME_ZONE = "UTC";
  public static final int SEC_TO_MS = 1000;
  private static final int MIN_TO_MS = SEC_TO_MS * 60;
  private static final int HOUR_TO_MS = MIN_TO_MS * 60;
  private static final int DAY_TO_MS = HOUR_TO_MS * 24;
  public static final String OPERATION_LOGGER = "operationLogger";
  // This will make MetaData.update() trigger a real metadata fetch.
  public static final int REQUEST_VERSION_UPDATE = -1;

  private KafkaCruiseControlUtils() {

  }

  /**
   * @return The current UTC date.
   */
  public static String currentUtcDate() {
    return utcDateFor(System.currentTimeMillis());
  }

  /**
   * @param timeMs Time in milliseconds.
   * @return The date for the given time in {@link #TIME_ZONE}.
   */
  public static String utcDateFor(long timeMs) {
    Date date = new Date(timeMs);
    DateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
    formatter.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
    return formatter.format(date);
  }

  /**
   * @return Formatted timestamp from long to a human readable string.
   */
  public static String toDateString(long time) {
    return toDateString(time, DATE_FORMAT2, "");
  }

  /**
   * Format the timestamp from long to human readable string. Allow customization of date format and time zone.
   * @param time time in milliseconds
   * @param dateFormat see formats above
   * @param timeZone will use default if timeZone is set to empty string
   * @return String representation of date
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
   * @return The configuration string.
   */
  public static String getRequiredConfig(Map<String, ?> configs, String configName) {
    String value = (String) configs.get(configName);
    if (value == null || value.isEmpty()) {
      throw new ConfigException(String.format("Configuration %s must be provided.", configName));
    }
    return value;
  }

  /**
   * Sanity check whether
   * <ul>
   *   <li>all hard goals (based on the given config) are included in the provided goal list, and</li>
   *   <li>{@link #sanityCheckNonExistingGoal} is not violated.</li>
   * </ul>
   *
   * There are two scenarios where this check is skipped.
   * <ul>
   *   <li> {@code goals} is {@code null} or empty -- i.e. even if hard goals are excluded from the default goals, this
   *   check will pass</li>
   *   <li> {@code goals} only has {@link PreferredLeaderElectionGoal}, denotes it is a PLE request.</li>
   * </ul>
   *
   * @param goals A list of goal names (i.e. each matching {@link Goal#name()}) to check.
   * @param skipHardGoalCheck True if hard goal checking is not needed.
   * @param config The configurations for Cruise Control.
   */
  public static void sanityCheckGoals(List<String> goals, boolean skipHardGoalCheck, KafkaCruiseControlConfig config) {
    if (goals != null && !goals.isEmpty() && !skipHardGoalCheck &&
        !(goals.size() == 1 && goals.get(0).equals(PreferredLeaderElectionGoal.class.getSimpleName()))) {
      sanityCheckNonExistingGoal(goals, AnalyzerUtils.getCaseInsensitiveGoalsByName(config));
      Set<String> hardGoals = hardGoals(config);
      if (!goals.containsAll(hardGoals)) {
        throw new IllegalArgumentException(String.format("Missing hard goals %s in the provided goals: %s. Add %s=true "
                                                         + "parameter to ignore this sanity check.", hardGoals, goals,
                                                         SKIP_HARD_GOAL_CHECK_PARAM));
      }
    }
  }

  /**
   * Get the set of configured hard goals.
   *
   * @param config The configurations for Cruise Control.
   * @return The set of configured hard goals.
   */
  public static Set<String> hardGoals(KafkaCruiseControlConfig config) {
    return config.getList(AnalyzerConfig.HARD_GOALS_CONFIG).stream()
                 .map(goalName -> goalName.substring(goalName.lastIndexOf(".") + 1)).collect(Collectors.toSet());
  }

  /**
   * Check to ensure that if requested cluster model completeness requires non-zero number of valid windows in cluster model,
   * load monitor should have already finished loading all the samples.
   * Note that even if only one valid window is required, we still need to wait load monitor finish loading all samples. This
   * is because sample loading follow time order, i.e. the first window loaded is the oldest window and the load information
   * in that window is probably stale.
   *
   * @param completenessRequirements Requested cluster model completeness requirement.
   * @param loadMonitorTaskRunnerState Current state of load monitor's task runner.
   */
  public static void sanityCheckLoadMonitorReadiness(ModelCompletenessRequirements completenessRequirements,
                                                     LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState) {
    if (completenessRequirements.minRequiredNumWindows() > 0 &&
        loadMonitorTaskRunnerState == LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING) {
      throw new IllegalStateException("Unable to generate proposal since load monitor is in "
                                      + LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING + " state.");
    }
  }

  /**
   * Get a goals by priority based on the goal list.
   *
   * @param goals A list of goals.
   * @param config The configurations for Cruise Control.
   * @return A list of goals sorted by highest to lowest priority.
   */
  public static List<Goal> goalsByPriority(List<String> goals, KafkaCruiseControlConfig config) {
    if (goals == null || goals.isEmpty()) {
      return AnalyzerUtils.getGoalsByPriority(config);
    }
    Map<String, Goal> allGoals = AnalyzerUtils.getCaseInsensitiveGoalsByName(config);
    sanityCheckNonExistingGoal(goals, allGoals);
    return goals.stream().map(allGoals::get).collect(Collectors.toList());
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
   * @param kafkaZkClient KafkaZkClient to be closed.
   */
  public static void closeKafkaZkClientWithTimeout(KafkaZkClient kafkaZkClient) {
    closeKafkaZkClientWithTimeout(kafkaZkClient, KAFKA_ZK_CLIENT_CLOSE_TIMEOUT_MS);
  }

  /**
   * Close the given KafkaZkClient with the given timeout.
   * @param kafkaZkClient KafkaZkClient to be closed
   * @param timeoutMs the timeout.
   */
  public static void closeKafkaZkClientWithTimeout(KafkaZkClient kafkaZkClient, long timeoutMs) {
    closeClientWithTimeout(kafkaZkClient::close, timeoutMs);
  }

  /**
   * Check if set a contains any element in set b.
   * @param a the first set.
   * @param b the second set.
   * @return True if a contains at least one of the element in b. false otherwise;
   */
  public static boolean containsAny(Set<Integer> a, Set<Integer> b) {
    return b.stream().mapToInt(i -> i).anyMatch(a::contains);
  }

  /**
   * Create an instance of KafkaZkClient with security disabled.
   * Name of the underlying {@link kafka.zookeeper.ZooKeeperClient} instance is derived using the combination given
   * metricGroup and metricType with a dash in between.
   *
   * @param connectString Comma separated host:port pairs, each corresponding to a zk server
   * @param metricGroup Metric group
   * @param metricType Metric type
   * @param zkSecurityEnabled True if zkSecurityEnabled, false otherwise.
   * @return A new instance of KafkaZkClient
   */
  public static KafkaZkClient createKafkaZkClient(String connectString, String metricGroup, String metricType, boolean zkSecurityEnabled) {
    String zooKeeperClientName = String.format("%s-%s", metricGroup, metricType);
    return KafkaZkClient.apply(connectString, zkSecurityEnabled, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, Integer.MAX_VALUE,
                               new SystemTime(), metricGroup, metricType, Option.apply(zooKeeperClientName), Option.empty());
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
   * @param adminClient AdminClient to be closed.
   */
  public static void closeAdminClientWithTimeout(AdminClient adminClient) {
    closeAdminClientWithTimeout(adminClient, ADMIN_CLIENT_CLOSE_TIMEOUT_MS);
  }

  /**
   * Close the given AdminClient with the given timeout.
   *
   * @param adminClient AdminClient to be closed.
   * @param timeoutMs the timeout.
   */
  public static void closeAdminClientWithTimeout(AdminClient adminClient, long timeoutMs) {
    closeClientWithTimeout(() -> {
      try {
        ((AutoCloseable) adminClient).close();
      } catch (Exception e) {
        throw new IllegalStateException("Failed to close the Admin Client.", e);
      }
    }, timeoutMs);
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
      setClassConfigIfExists(configs, adminClientConfigs, SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS);
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

  /**
   * Generate a {@link MetadataResponseData} with the given information -- e.g. for creating bootstrap and test response.
   *
   * @param brokers Brokers in the cluster.
   * @param clusterId Cluster Id.
   * @param controllerId Controller Id.
   * @param topicMetadataList Metadata list for the topics in the cluster.
   * @return A {@link MetadataResponseData} with the given information.
   */
  public static MetadataResponse prepareMetadataResponse(List<Node> brokers,
                                                         String clusterId,
                                                         int controllerId,
                                                         List<MetadataResponse.TopicMetadata> topicMetadataList) {
    MetadataResponseData responseData = new MetadataResponseData();
    responseData.setThrottleTimeMs(AbstractResponse.DEFAULT_THROTTLE_TIME);
    brokers.forEach(broker -> responseData.brokers().add(
        new MetadataResponseData.MetadataResponseBroker().setNodeId(broker.id())
                                                         .setHost(broker.host())
                                                         .setPort(broker.port())
                                                         .setRack(broker.rack())));

    responseData.setClusterId(clusterId);
    responseData.setControllerId(controllerId);
    responseData.setClusterAuthorizedOperations(MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);
    topicMetadataList.forEach(topicMetadata -> responseData.topics().add(prepareMetadataResponseTopic(topicMetadata)));

    return new MetadataResponse(responseData);
  }

  private static MetadataResponseData.MetadataResponseTopic prepareMetadataResponseTopic(MetadataResponse.TopicMetadata topicMetadata) {
    MetadataResponseData.MetadataResponseTopic metadataResponseTopic = new MetadataResponseData.MetadataResponseTopic();
    metadataResponseTopic.setErrorCode(topicMetadata.error().code())
                         .setName(topicMetadata.topic())
                         .setIsInternal(topicMetadata.isInternal())
                         .setTopicAuthorizedOperations(topicMetadata.authorizedOperations());

    for (MetadataResponse.PartitionMetadata partitionMetadata : topicMetadata.partitionMetadata()) {
      metadataResponseTopic.partitions().add(
          new MetadataResponseData.MetadataResponsePartition()
              .setErrorCode(partitionMetadata.error.code())
              .setPartitionIndex(partitionMetadata.partition())
              .setLeaderId(partitionMetadata.leaderId.orElse(-1))
              .setLeaderEpoch(partitionMetadata.leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
              .setReplicaNodes(partitionMetadata.replicaIds)
              .setIsrNodes(partitionMetadata.inSyncReplicaIds)
              .setOfflineReplicas(partitionMetadata.offlineReplicaIds));
    }

    return metadataResponseTopic;
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

  private static void setClassConfigIfExists(KafkaCruiseControlConfig configs, Map<String, Object> props, String name) {
    try {
      props.put(name, configs.getClass(name));
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
  public static <E> void ensureDisjoint(Set<E> set1, Set<E> set2, String message) {
    Set<E> interSection = new HashSet<>(set1);
    interSection.retainAll(set2);
    if (!interSection.isEmpty()) {
      throw new IllegalStateException(message);
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
   * Get the balancedness cost of violating goals by their name, where the sum of costs is {@link #MAX_BALANCEDNESS_SCORE}.
   *
   * @param goals The goals to be used for balancing (sorted by priority).
   * @param priorityWeight The impact of having one level higher goal priority on the relative balancedness score.
   * @param strictnessWeight The impact of strictness on the relative balancedness score.
   * @return The balancedness cost of violating goals by their name.
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
