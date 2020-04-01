/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.MetricSamplingException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerLoad;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ReassignmentInProgressException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static kafka.log.LogConfig.CleanupPolicyProp;
import static kafka.log.LogConfig.RetentionMsProp;
import static com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig.SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;


public class SamplingUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SamplingUtils.class);
  private static final String SKIP_BUILDING_SAMPLE_PREFIX = "Skip generating metric sample for ";
  public static final long CLIENT_REQUEST_TIMEOUT_MS = 10000L;
  public static final String DEFAULT_CLEANUP_POLICY = "delete";
  public static final int UNRECOGNIZED_BROKER_ID = -1;

  private SamplingUtils() {
  }

  /**
   * Get the number of leader partitions for each topic by each broker. It is useful to derive the partition level IO
   * from the topic level IO on a broker.
   * TODO: Create open source KIP to provide per partition IO metrics.
   *
   * @param cluster Kafka cluster
   * @return The number of leader partitions for each topic by each broker.
   */
  static Map<Integer, Map<String, Integer>> leaderDistribution(Cluster cluster) {
    List<Node> clusterNodes = cluster.nodes();
    Map<Integer, Map<String, Integer>> stats = new HashMap<>(clusterNodes.size());
    for (Node node : clusterNodes) {
      Map<String, Integer> numLeadersByTopic = new HashMap<>();
      stats.put(node.id(), numLeadersByTopic);
      cluster.partitionsForNode(node.id()).forEach(partitionInfo -> numLeadersByTopic.merge(partitionInfo.topic(), 1, Integer::sum));
    }
    return stats;
  }

  /**
   * Estimate the leader CPU utilization of the partition using its metric sample based on the static model defined via
   * {@link ModelUtils#estimateLeaderCpuUtilPerCore(double, double, double, double, double, double)} and the given number of
   * CPU cores.
   *
   * @param pms Metric sample of partition.
   * @param brokerLoad Load information for the broker that the leader of the partition resides.
   * @param commonMetricDef Definitions to look up the metric info.
   * @param numCpuCores Number of CPU cores.
   * @return The estimated CPU utilization of the leader for the partition based on the static model, or {@code null}
   * if estimation is not possible.
   */
  private static Double estimateLeaderCpuUtil(PartitionMetricSample pms, BrokerLoad brokerLoad, MetricDef commonMetricDef, short numCpuCores) {
    double partitionBytesInRate = pms.metricValue(commonMetricDef.metricInfo(KafkaMetricDef.LEADER_BYTES_IN.name()).id());
    double partitionBytesOutRate = pms.metricValue(commonMetricDef.metricInfo(KafkaMetricDef.LEADER_BYTES_OUT.name()).id());
    double partitionReplicationBytesOutRate = pms.metricValue(commonMetricDef.metricInfo(KafkaMetricDef.REPLICATION_BYTES_OUT_RATE.name()).id());
    double brokerTotalBytesOut = brokerLoad.brokerMetric(ALL_TOPIC_BYTES_OUT) + brokerLoad.brokerMetric(ALL_TOPIC_REPLICATION_BYTES_OUT);
    Double estimatedLeaderCpuUtilPerCore = ModelUtils.estimateLeaderCpuUtilPerCore(brokerLoad.brokerMetric(BROKER_CPU_UTIL),
                                                                                   brokerLoad.brokerMetric(ALL_TOPIC_BYTES_IN),
                                                                                   brokerTotalBytesOut,
                                                                                   brokerLoad.brokerMetric(ALL_TOPIC_REPLICATION_BYTES_IN),
                                                                                   partitionBytesInRate,
                                                                                   partitionBytesOutRate + partitionReplicationBytesOutRate);
    return estimatedLeaderCpuUtilPerCore != null ? numCpuCores * estimatedLeaderCpuUtilPerCore : null;
  }

  /**
   * Estimate the leader CPU utilization of the partition using its metric sample based on linear regression model
   * defined via {@link ModelUtils#estimateLeaderCpuUtilUsingLinearRegressionModel(double, double)}
   *
   * @param metricSample Metric sample of partition.
   * @return The estimated CPU utilization of the leader for the partition based on linear regression model.
   */
  static double estimateLeaderCpuUtilUsingLinearRegressionModel(PartitionMetricSample metricSample) {
    List<Short> cpuId = KafkaMetricDef.resourceToMetricIds(Resource.CPU);
    List<Short> networkOutId = KafkaMetricDef.resourceToMetricIds(Resource.NW_OUT);
    // TODO: Verify whether the linear regression model intends to use cpu utilization or not (i.e. NW_IN).
    Double cpuUtilization = sumOfMetrics(metricSample, cpuId);
    Double partitionBytesOutRate = sumOfMetrics(metricSample, networkOutId);
    return ModelUtils.estimateLeaderCpuUtilUsingLinearRegressionModel(cpuUtilization, partitionBytesOutRate);
  }

  /**
   * Add all the values of the given metric ids up.
   * TODO: Remove this once we completely move to metric def.
   *
   * @param metricSample Metric sample of partition.
   * @param metricIds Metric Ids.
   * @return Sum of all the values of the given metric ids.
   */
  private static Double sumOfMetrics(PartitionMetricSample metricSample, Collection<Short> metricIds) {
    double result = 0;
    for (short id : metricIds) {
      result += metricSample.metricValue(id);
    }
    return result;
  }

  /**
   * Check whether there are failures in fetching offsets during sampling.
   *
   * @param endOffsets End offsets retrieved by consumer.
   * @param offsetsForTimes Offsets for times retrieved by consumer.
   */
  static void sanityCheckOffsetFetch(Map<TopicPartition, Long> endOffsets,
                                     Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes)
      throws MetricSamplingException {
    Set<TopicPartition> failedToFetchOffsets = new HashSet<>();
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
      if (entry.getValue() == null && endOffsets.get(entry.getKey()) == null) {
        failedToFetchOffsets.add(entry.getKey());
      }
    }

    if (!failedToFetchOffsets.isEmpty()) {
      throw new MetricSamplingException(String.format("Metric consumer failed to fetch offsets for %s. Consider "
                                                      + "decreasing reconnect.backoff.ms to mitigate consumption failures"
                                                      + " due to transient network issues.", failedToFetchOffsets));
    }
  }

  /**
   * Removes any dots that potentially exist in the given parameter.
   *
   * @param tp TopicPartition that may contain dots.
   * @return TopicPartition whose dots have been removed from the given topic name.
   */
  private static TopicPartition partitionHandleDotInTopicName(TopicPartition tp) {
    // In the reported metrics, the "." in the topic name will be replaced by "_".
    return !tp.topic().contains(".") ? tp :
           new TopicPartition(replaceDotsWithUnderscores(tp.topic()), tp.partition());
  }

  /**
   * Removes any dots that potentially exist in the given string. This method useful for making topic names reported by
   * Kafka metadata consistent with the topic names reported by the metrics reporter.
   *
   * Note that the reported metrics implicitly replaces the "." in topic names with "_".
   *
   * @param stringWithDots String that may contain dots.
   * @return String whose dots have been removed from the given string.
   */
  public static String replaceDotsWithUnderscores(String stringWithDots) {
    return !stringWithDots.contains(".") ? stringWithDots : stringWithDots.replace('.', '_');
  }

  /**
   * Create a {@link PartitionMetricSample}, record the relevant metrics for the given partition from the given topic on
   * broker that hosts the given number of leaders, and return the sample.
   *
   * @param cluster Kafka cluster.
   * @param leaderDistribution The leader count per topic/broker
   * @param tpDotNotHandled The original topic name that may contain dots.
   * @param brokerLoadById Load information for brokers by the broker id.
   * @param maxMetricTimestamp Maximum timestamp of the sampled metric during the sampling process.
   * @param cachedNumCoresByBroker Cached number of cores by broker.
   * @param skippedPartitionByBroker Number of skipped partition samples by broker ids.
   * @return Metric sample populated with topic and partition metrics, or {@code null} if sample generation is skipped.
   */
  static PartitionMetricSample buildPartitionMetricSample(Cluster cluster,
                                                          Map<Integer, Map<String, Integer>> leaderDistribution,
                                                          TopicPartition tpDotNotHandled,
                                                          Map<Integer, BrokerLoad> brokerLoadById,
                                                          long maxMetricTimestamp,
                                                          Map<Integer, Short> cachedNumCoresByBroker,
                                                          Map<Integer, Integer> skippedPartitionByBroker) {
    Node leaderNode = cluster.leaderFor(tpDotNotHandled);
    if (leaderNode == null) {
      LOG.trace("Partition {} has no current leader.", tpDotNotHandled);
      skippedPartitionByBroker.merge(UNRECOGNIZED_BROKER_ID, 1, Integer::sum);
      return null;
    }
    int leaderId = leaderNode.id();
    //TODO: switch to linear regression model without computing partition level CPU usage.
    BrokerLoad brokerLoad = brokerLoadById.get(leaderId);
    TopicPartition tpWithDotHandled = partitionHandleDotInTopicName(tpDotNotHandled);
    if (skipBuildingPartitionMetricSample(tpDotNotHandled, tpWithDotHandled, leaderId, brokerLoad, cachedNumCoresByBroker)) {
      skippedPartitionByBroker.merge(leaderId, 1, Integer::sum);
      return null;
    }

    // Fill in all the common metrics.
    MetricDef commonMetricDef = KafkaMetricDef.commonMetricDef();
    PartitionMetricSample pms = new PartitionMetricSample(leaderId, tpDotNotHandled);
    int numLeaders = leaderDistribution.get(leaderId).get(tpDotNotHandled.topic());
    for (RawMetricType rawMetricType : RawMetricType.topicMetricTypes()) {
      double sampleValue = numLeaders == 0 ? 0 : (brokerLoad.topicMetrics(tpWithDotHandled.topic(), rawMetricType)) / numLeaders;
      MetricInfo metricInfo = commonMetricDef.metricInfo(KafkaMetricDef.forRawMetricType(rawMetricType).name());
      pms.record(metricInfo, sampleValue);
    }
    // Fill in disk and CPU utilization, which are not topic metric types.
    Double partitionSize = brokerLoad.partitionMetric(tpWithDotHandled.topic(), tpWithDotHandled.partition(), PARTITION_SIZE);
    if (partitionSize == null) {
      skippedPartitionByBroker.merge(leaderId, 1, Integer::sum);
      return null;
    }
    pms.record(commonMetricDef.metricInfo(KafkaMetricDef.DISK_USAGE.name()), partitionSize);
    Double estimatedLeaderCpuUtil = estimateLeaderCpuUtil(pms, brokerLoad, commonMetricDef, cachedNumCoresByBroker.get(leaderId));
    if (estimatedLeaderCpuUtil == null) {
      skippedPartitionByBroker.merge(leaderId, 1, Integer::sum);
      return null;
    }
    pms.record(commonMetricDef.metricInfo(KafkaMetricDef.CPU_USAGE.name()), estimatedLeaderCpuUtil);
    pms.close(maxMetricTimestamp);
    return pms;
  }

  /**
   * Create a {@link BrokerMetricSample}, record the relevant metrics for the given broker, and return the sample.
   *
   * @param node Node hosting the broker.
   * @param brokerLoadById Load information for brokers by the broker id.
   * @param maxMetricTimestamp Maximum timestamp of the sampled metric during the sampling process.
   * @return Metric sample populated with broker metrics, or {@code null} if sample generation is skipped.
   */
  static BrokerMetricSample buildBrokerMetricSample(Node node,
                                                    Map<Integer, BrokerLoad> brokerLoadById,
                                                    long maxMetricTimestamp) throws UnknownVersionException {
    BrokerLoad brokerLoad = brokerLoadById.get(node.id());
    if (skipBuildingBrokerMetricSample(brokerLoad, node.id())) {
      return null;
    }
    MetricDef brokerMetricDef = KafkaMetricDef.brokerMetricDef();
    BrokerMetricSample bms = new BrokerMetricSample(node.host(), node.id(), brokerLoad.brokerSampleDeserializationVersion());
    for (Map.Entry<Byte, Set<RawMetricType>> entry : RawMetricType.brokerMetricTypesDiffByVersion().entrySet()) {
      for (RawMetricType rawBrokerMetricType : entry.getValue()) {
        // We require the broker to report all the metric types (including nullable values). Otherwise we skip the broker.
        if (!brokerLoad.brokerMetricAvailable(rawBrokerMetricType)) {
          LOG.warn("{}broker {} because it does not have {} metrics (serde version {}) or the metrics are inconsistent.",
                   SKIP_BUILDING_SAMPLE_PREFIX, node.id(), rawBrokerMetricType, entry.getKey());
          return null;
        } else {
          MetricInfo metricInfo = brokerMetricDef.metricInfo(KafkaMetricDef.forRawMetricType(rawBrokerMetricType).name());
          double metricValue = brokerLoad.brokerMetric(rawBrokerMetricType);
          bms.record(metricInfo, metricValue);
        }
      }
    }

    // Disk usage is not one of the broker raw metric type.
    bms.record(brokerMetricDef.metricInfo(KafkaMetricDef.DISK_USAGE.name()), brokerLoad.diskUsage());
    bms.close(maxMetricTimestamp);
    return bms;
  }

  /**
   * Check whether the metric sample generation for the partition with the given information should be skipped.
   *
   * @param tpDotNotHandled The original topic name that may contain dots.
   * @param tpWithDotHandled Topic partition with dot-handled topic name (see {@link SamplingUtils#partitionHandleDotInTopicName}).
   * @param leaderId Leader Id of partition.
   * @param brokerLoad Load information for the broker that the leader of the partition resides.
   * @param cachedNumCoresByBroker Cached number of cores by broker.
   * @return True to skip generating partition metric sample, false otherwise.
   */
  private static boolean skipBuildingPartitionMetricSample(TopicPartition tpDotNotHandled,
                                                           TopicPartition tpWithDotHandled,
                                                           int leaderId,
                                                           BrokerLoad brokerLoad,
                                                           Map<Integer, Short> cachedNumCoresByBroker) {
    if (brokerLoad == null || !brokerLoad.brokerMetricAvailable(BROKER_CPU_UTIL)) {
      // Broker load or its BROKER_CPU_UTIL metric is not available.
      LOG.debug("{}partition {} because {} metric for broker {} is unavailable.", SKIP_BUILDING_SAMPLE_PREFIX,
                tpDotNotHandled, BROKER_CPU_UTIL, leaderId);
      return true;
    } else if (cachedNumCoresByBroker.get(leaderId) == null) {
      // Broker load is available but the corresponding number of cores was not cached.
      LOG.debug("{}partition {} because the number of CPU cores of its leader broker {} is unavailable. Please ensure that "
                + "either the broker capacity config resolver provides the number of CPU cores without estimation or allow "
                + "CPU capacity estimation during sampling (i.e. set {} to true).", SKIP_BUILDING_SAMPLE_PREFIX,
                tpDotNotHandled, leaderId, SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION_CONFIG);
      return true;
    } else if (!brokerLoad.allDotHandledTopicMetricsAvailable(tpWithDotHandled.topic())) {
      // Topic metrics are not available.
      LOG.debug("{}partition {} because broker {} has no metric or topic metrics are not available", SKIP_BUILDING_SAMPLE_PREFIX,
                tpDotNotHandled, leaderId);
      return true;
    }
    return false;
  }

  /**
   * Check whether the metric sample generation for the broker with the given load and id should be skipped.
   *
   * @param brokerLoad Broker load.
   * @param brokerId Broker id
   * @return True to skip generating broker metric sample, false otherwise.
   */
  private static boolean skipBuildingBrokerMetricSample(BrokerLoad brokerLoad, int brokerId) {
    if (brokerLoad == null) {
      LOG.warn("{}broker {} because all broker metrics are missing.", SKIP_BUILDING_SAMPLE_PREFIX, brokerId);
      return true;
    } else if (!brokerLoad.minRequiredBrokerMetricsAvailable()) {
      if (brokerLoad.missingBrokerMetricsInMinSupportedVersion().size() == 0) {
        LOG.warn("{}broker {} because there are not enough topic metrics to generate broker metrics.",
                 SKIP_BUILDING_SAMPLE_PREFIX, brokerId);
      } else {
        LOG.warn("{}broker {} because the following required metrics are missing {}.", SKIP_BUILDING_SAMPLE_PREFIX,
                 brokerId, brokerLoad.missingBrokerMetricsInMinSupportedVersion());
      }
      return true;
    }

    return false;
  }

  /**
   * Build a wrapper around the topic with the given desired properties and {@link #DEFAULT_CLEANUP_POLICY}.
   *
   * @param topic The name of the topic.
   * @param partitionCount Desired partition count.
   * @param replicationFactor Desired replication factor.
   * @param retentionMs Desired retention in milliseconds.
   * @return A wrapper around the topic with the given desired properties.
   */
  public static NewTopic wrapTopic(String topic, int partitionCount, short replicationFactor, long retentionMs) {
    if (partitionCount <= 0 || replicationFactor <= 0 || retentionMs <= 0) {
      throw new IllegalArgumentException(String.format("Partition count (%d), replication factor (%d), and retention ms (%d)"
                                                       + " must be positive for the topic (%s).", partitionCount,
                                                       replicationFactor, retentionMs, topic));
    }

    NewTopic newTopic = new NewTopic(topic, partitionCount, replicationFactor);
    Map<String, String> config = new HashMap<>(2);
    config.put(RetentionMsProp(), Long.toString(retentionMs));
    config.put(CleanupPolicyProp(), DEFAULT_CLEANUP_POLICY);
    newTopic.configs(config);

    return newTopic;
  }

  /**
   * Add config altering operations to the given configs to alter for configs that differ between current and desired.
   *
   * @param configsToAlter A set of config altering operations to be populated.
   * @param desiredConfig Desired config value by name.
   * @param currentConfig Current config.
   */
  private static void maybeUpdateConfig(Set<AlterConfigOp> configsToAlter, Map<String, String> desiredConfig, Config currentConfig) {
    for (Map.Entry<String, String> entry : desiredConfig.entrySet()) {
      String configName = entry.getKey();
      String targetConfigValue = entry.getValue();
      ConfigEntry currentConfigEntry = currentConfig.get(configName);
      if (currentConfigEntry == null || !currentConfigEntry.value().equals(targetConfigValue)) {
        configsToAlter.add(new AlterConfigOp(new ConfigEntry(configName, targetConfigValue), AlterConfigOp.OpType.SET));
      }
    }
  }

  /**
   * Update topic configurations with the desired configs specified in the given topicToUpdateConfigs.
   *
   * @param adminClient The adminClient to send describeConfigs and incrementalAlterConfigs requests.
   * @param topicToUpdateConfigs Existing topic to update selected configs if needed -- cannot be {@code null}.
   * @return {@code true} if the request is completed successfully, {@code false} if there are any exceptions.
   */
  public static boolean maybeUpdateTopicConfig(AdminClient adminClient, NewTopic topicToUpdateConfigs) {
    String topicName = topicToUpdateConfigs.name();
    // Retrieve topic config to check if it needs an update.
    ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singleton(topicResource));
    Config topicConfig;
    try {
      topicConfig = describeConfigsResult.values().get(topicResource).get(CLIENT_REQUEST_TIMEOUT_MS,
                                                                          TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.warn("Config check for topic {} failed due to failure to describe its configs.", topicName, e);
      return false;
    }

    // Update configs if needed.
    Map<String, String> desiredConfig = topicToUpdateConfigs.configs();
    if (desiredConfig != null) {
      Set<AlterConfigOp> alterConfigOps = new HashSet<>(desiredConfig.size());
      maybeUpdateConfig(alterConfigOps, desiredConfig, topicConfig);
      if (!alterConfigOps.isEmpty()) {
        AlterConfigsResult alterConfigsResult
            = adminClient.incrementalAlterConfigs(Collections.singletonMap(topicResource, alterConfigOps));
        try {
          alterConfigsResult.values().get(topicResource).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          LOG.warn("Config change for topic {} failed.", topicName, e);
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Increase the partition count of the given existing topic to the desired partition count (if needed).
   *
   * @param adminClient The adminClient to send describeTopics and createPartitions requests.
   * @param topicToAddPartitions Existing topic to add more partitions if needed -- cannot be {@code null}.
   * @return {@code true} if the request is completed successfully, {@code false} if there are any exceptions.
   */
  public static boolean maybeIncreasePartitionCount(AdminClient adminClient, NewTopic topicToAddPartitions) {
    String topicName = topicToAddPartitions.name();

    // Retrieve partition count of topic to check if it needs a partition count update.
    TopicDescription topicDescription;
    try {
      topicDescription = adminClient.describeTopics(Collections.singletonList(topicName)).values()
                                    .get(topicName).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.warn("Partition count increase check for topic {} failed due to failure to describe cluster.", topicName, e);
      return false;
    }

    // Update partition count of topic if needed.
    if (topicDescription.partitions().size() < topicToAddPartitions.numPartitions()) {
      CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(
          Collections.singletonMap(topicName, NewPartitions.increaseTo(topicToAddPartitions.numPartitions())));

      try {
        createPartitionsResult.values().get(topicName).get(CLIENT_REQUEST_TIMEOUT_MS,
                                                           TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        LOG.warn("Partition count increase to {} for topic {} failed{}.", topicToAddPartitions.numPartitions(), topicName,
                 (e.getCause() instanceof ReassignmentInProgressException) ? " due to ongoing reassignment" : "", e);
        return false;
      }
    }

    return true;
  }
}
