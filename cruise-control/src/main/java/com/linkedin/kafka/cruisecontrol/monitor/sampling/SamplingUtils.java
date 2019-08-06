/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.MetricSamplingException;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerLoad;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;


public class SamplingUtils {
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
  static Map<Integer, Map<String, Integer>> leaderDistributionStats(Cluster cluster) {
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
   * {@link ModelUtils#estimateLeaderCpuUtil(double, double, double, double, double, double)}.
   *
   * @param metricSample Metric sample of partition.
   * @param brokerLoad Load information for the broker that the leader of the partition resides.
   * @param commonMetricDef Definitions to look up the metric info.
   * @return The estimated CPU utilization of the leader for the partition based on the static model.
   */
  static double estimateLeaderCpuUtil(PartitionMetricSample metricSample, BrokerLoad brokerLoad, MetricDef commonMetricDef) {
    double partitionBytesInRate = metricSample.metricValue(commonMetricDef.metricInfo(KafkaMetricDef.LEADER_BYTES_IN.name()).id());
    double partitionBytesOutRate = metricSample.metricValue(commonMetricDef.metricInfo(KafkaMetricDef.LEADER_BYTES_OUT.name()).id());
    double partitionReplicationBytesOutRate = metricSample.metricValue(commonMetricDef.metricInfo(KafkaMetricDef.REPLICATION_BYTES_OUT_RATE.name()).id());
    double brokerTotalBytesOut = brokerLoad.brokerMetric(ALL_TOPIC_BYTES_OUT) + brokerLoad.brokerMetric(ALL_TOPIC_REPLICATION_BYTES_OUT);
    return ModelUtils.estimateLeaderCpuUtil(brokerLoad.brokerMetric(BROKER_CPU_UTIL),
                                            brokerLoad.brokerMetric(ALL_TOPIC_BYTES_IN),
                                            brokerTotalBytesOut,
                                            brokerLoad.brokerMetric(ALL_TOPIC_REPLICATION_BYTES_IN),
                                            partitionBytesInRate,
                                            partitionBytesOutRate + partitionReplicationBytesOutRate);
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
  static TopicPartition partitionHandleDotInTopicName(TopicPartition tp) {
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
}
