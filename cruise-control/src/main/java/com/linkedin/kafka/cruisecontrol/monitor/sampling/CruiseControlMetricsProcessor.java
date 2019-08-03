/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerLoad;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.getRackHandleNull;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils.estimateLeaderCpuUtil;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils.leaderDistributionStats;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils.partitionHandleDotInTopicName;


/**
 * Process the raw metrics collected by {@link CruiseControlMetricsReporterSampler} from the Kafka cluster.
 */
public class CruiseControlMetricsProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(CruiseControlMetricsProcessor.class);
  private final Map<Integer, BrokerLoad> _brokerLoad;
  // TODO: Use the cached number of cores in estimation of partition CPU utilization.
  private final Map<Integer, Short> _cachedNumCoresByBroker;
  private final BrokerCapacityConfigResolver _brokerCapacityConfigResolver;
  private final boolean _allowCpuCapacityEstimation;
  private long _maxMetricTimestamp = -1;

  /**
   * @param brokerCapacityConfigResolver The resolver for retrieving broker capacities.
   * @param allowCpuCapacityEstimation True to allow CPU capacity estimation of brokers used for CPU utilization estimation.
   */
  CruiseControlMetricsProcessor(BrokerCapacityConfigResolver brokerCapacityConfigResolver, boolean allowCpuCapacityEstimation) {
    _brokerLoad = new HashMap<>();
    _cachedNumCoresByBroker = new HashMap<>();
    _brokerCapacityConfigResolver = brokerCapacityConfigResolver;
    _allowCpuCapacityEstimation = allowCpuCapacityEstimation;
  }

  void addMetric(Cluster cluster, CruiseControlMetric metric) {
    int brokerId = metric.brokerId();
    LOG.trace("Adding cruise control metric {}", metric);
    _maxMetricTimestamp = Math.max(metric.time(), _maxMetricTimestamp);
    _brokerLoad.compute(brokerId, (bid, load) -> {
      BrokerLoad brokerLoad = load == null ? new BrokerLoad() : load;
      brokerLoad.recordMetric(metric);
      return brokerLoad;
    });

    // Compute cached number of cores by broker id if they have not been cached already.
    Node node = cluster.nodeById(brokerId);
    _cachedNumCoresByBroker.computeIfAbsent(brokerId, bid -> {
      BrokerCapacityInfo capacity = _brokerCapacityConfigResolver.capacityForBroker(getRackHandleNull(node), node.host(), bid);
      // No mapping shall be recorded if capacity is estimated, but estimation is not allowed.
      return (!_allowCpuCapacityEstimation && capacity.isEstimated()) ? null : capacity.numCpuCores();
    });
  }

  /**
   * Package private for unit tests.
   */
  Map<Integer, Short> cachedNumCoresByBroker() {
    return _cachedNumCoresByBroker;
  }

  /**
   * Process all the added {@link CruiseControlMetric} to get the {@link MetricSampler.Samples}
   *
   * @param cluster Kafka cluster.
   * @param partitionsDotNotHandled Partitions to construct samples for. The topic partition name may have dots.
   * @param samplingMode The sampling mode to indicate which type of samples are needed.
   *
   * @return the constructed metric samples.
   */
  MetricSampler.Samples process(Cluster cluster,
                                Collection<TopicPartition> partitionsDotNotHandled,
                                MetricSampler.SamplingMode samplingMode) throws UnknownVersionException {
    Map<Integer, Map<String, Integer>> leaderDistributionStats = leaderDistributionStats(cluster);
    // Theoretically we should not move forward at all if a broker reported a different all topic bytes in from all
    // its resident replicas. However, it is not clear how often this would happen yet. At this point we still
    // continue process the other brokers. Later on if in practice all topic bytes in and the aggregation value is
    // rarely inconsistent we can just stop the sample generation when the this happens.
    _brokerLoad.forEach((broker, load) -> load.prepareBrokerMetrics(cluster, broker, _maxMetricTimestamp));

    Set<PartitionMetricSample> partitionMetricSamples = new HashSet<>();
    Set<BrokerMetricSample> brokerMetricSamples = new HashSet<>();

    // Get partition metric samples.
    int skippedPartition = 0;
    if (samplingMode == MetricSampler.SamplingMode.ALL
        || samplingMode == MetricSampler.SamplingMode.PARTITION_METRICS_ONLY) {
      skippedPartition = addPartitionMetricSamples(cluster, partitionsDotNotHandled, leaderDistributionStats, partitionMetricSamples);
    }

    // Get broker metric samples.
    int skippedBroker = 0;
    if (samplingMode == MetricSampler.SamplingMode.ALL
        || samplingMode == MetricSampler.SamplingMode.BROKER_METRICS_ONLY) {
      skippedBroker = addBrokerMetricSamples(cluster, brokerMetricSamples);
    }

    LOG.info("Generated {}{} partition metric samples and {}{} broker metric samples for timestamp {}",
             partitionMetricSamples.size(), skippedPartition > 0 ? "(" + skippedPartition + " skipped)" : "",
             brokerMetricSamples.size(), skippedBroker > 0 ? "(" + skippedBroker + " skipped)" : "",
             _maxMetricTimestamp);
    return new MetricSampler.Samples(partitionMetricSamples, brokerMetricSamples);
  }

  void clear() {
    _brokerLoad.clear();
    _maxMetricTimestamp = -1L;
  }

  /**
   * Add the partition metric samples to the provided set.
   *
   * @param cluster Kafka cluster
   * @param partitionsDotNotHandled The partitions to get samples. The topic partition name may have dots.
   * @param leaderDistributionStats The leader count per topic/broker
   * @param partitionMetricSamples The set to add the partition samples to.
   * @return The number of skipped partitions.
   */
  private int addPartitionMetricSamples(Cluster cluster,
                                        Collection<TopicPartition> partitionsDotNotHandled,
                                        Map<Integer, Map<String, Integer>> leaderDistributionStats,
                                        Set<PartitionMetricSample> partitionMetricSamples) {
    int skippedPartition = 0;

      for (TopicPartition tpDotNotHandled : partitionsDotNotHandled) {
        try {
          PartitionMetricSample sample = buildPartitionMetricSample(cluster, tpDotNotHandled, leaderDistributionStats);
          if (sample != null) {
            LOG.trace("Added partition metrics sample for {}", tpDotNotHandled);
            partitionMetricSamples.add(sample);
          } else {
            skippedPartition++;
          }
        } catch (Exception e) {
          LOG.error("Error building partition metric sample for " + tpDotNotHandled, e);
          skippedPartition++;
        }
      }
    return skippedPartition;
  }

  /**
   * Add the broker metric samples to the provided set.
   *
   * @param cluster The Kafka cluster
   * @param brokerMetricSamples The set to add the broker samples to.
   * @return The number of skipped brokers.
   */
  private int addBrokerMetricSamples(Cluster cluster,
                                     Set<BrokerMetricSample> brokerMetricSamples) throws UnknownVersionException {
    int skippedBroker = 0;
    MetricDef brokerMetricDef = KafkaMetricDef.brokerMetricDef();
    for (Node node : cluster.nodes()) {
      BrokerLoad brokerLoad = _brokerLoad.get(node.id());
      if (brokerLoad == null) {
        LOG.warn("Skip generating broker metric sample for broker {} because all broker metrics are missing.", node.id());
        skippedBroker++;
        continue;
      } else if (!brokerLoad.minRequiredBrokerMetricsAvailable()) {
        if (brokerLoad.missingBrokerMetricsInMinSupportedVersion().size() == 0) {
          LOG.warn("Skip generating broker metric sample for broker {} because there are not enough topic metrics to "
                  + "generate broker metrics.", node.id());
        } else {
          LOG.warn("Skip generating broker metric sample for broker {} because the following required metrics are missing {}.",
              node.id(), brokerLoad.missingBrokerMetricsInMinSupportedVersion());
        }
        skippedBroker++;
        continue;
      }

      boolean validSample = true;
      BrokerMetricSample brokerMetricSample = new BrokerMetricSample(node.host(), node.id(), brokerLoad.brokerSampleDeserializationVersion());
      for (Map.Entry<Byte, Set<RawMetricType>> entry : RawMetricType.brokerMetricTypesDiffByVersion().entrySet()) {
        for (RawMetricType rawBrokerMetricType : entry.getValue()) {
          // We require the broker to report all the metric types (including nullable values). Otherwise we skip the broker.
          if (!brokerLoad.brokerMetricAvailable(rawBrokerMetricType)) {
            skippedBroker++;
            validSample = false;
            LOG.warn("Skip generating broker metric sample for broker {} because it does not have {} metrics (serde "
                     + "version {}) or the metrics are inconsistent.", node.id(), rawBrokerMetricType, entry.getKey());
            break;
          } else {
            MetricInfo metricInfo = brokerMetricDef.metricInfo(KafkaMetricDef.forRawMetricType(rawBrokerMetricType).name());
            double metricValue = brokerLoad.brokerMetric(rawBrokerMetricType);
            brokerMetricSample.record(metricInfo, metricValue);
          }
        }
      }

      if (validSample) {
        // Disk usage is not one of the broker raw metric type.
        brokerMetricSample.record(brokerMetricDef.metricInfo(KafkaMetricDef.DISK_USAGE.name()),
                                  brokerLoad.diskUsage());
        brokerMetricSample.close(_maxMetricTimestamp);

        LOG.trace("Added broker metric sample for broker {}", node.id());
        brokerMetricSamples.add(brokerMetricSample);
      }
    }
    return skippedBroker;
  }

  private static boolean skipBuildingPartitionMetricSample(TopicPartition tpDotNotHandled,
                                                           TopicPartition tpWithDotHandled,
                                                           int leaderId,
                                                           BrokerLoad brokerLoad) {
    // Ensure broker load is available.
    if (brokerLoad == null || !brokerLoad.brokerMetricAvailable(BROKER_CPU_UTIL)) {
      LOG.debug("Skip generating metric sample for partition {} because broker metric for broker {} is unavailable.",
                tpDotNotHandled, leaderId);
      return true;
    }
    // Ensure the topic load is available.
    if (!brokerLoad.allDotHandledTopicMetricsAvailable(tpWithDotHandled.topic())) {
      LOG.debug("Skip generating metric samples for partition {} because broker {} has no metric or topic metrics "
                + "are not available", tpDotNotHandled, leaderId);
      return true;
    }
    if (!brokerLoad.partitionMetricAvailable(tpWithDotHandled, PARTITION_SIZE)) {
      // This broker is no longer the leader.
      LOG.debug("Skip generating metric sample for partition {} because broker {} no long host the partition.", tpDotNotHandled, leaderId);
      return true;
    }
    return false;
  }

  private PartitionMetricSample buildPartitionMetricSample(Cluster cluster,
                                                           TopicPartition tpDotNotHandled,
                                                           Map<Integer, Map<String, Integer>> leaderDistributionStats) {
    Node leaderNode = cluster.leaderFor(tpDotNotHandled);
    if (leaderNode == null) {
      return null;
    }
    int leaderId = leaderNode.id();
    //TODO: switch to linear regression model without computing partition level CPU usage.
    BrokerLoad brokerLoad = _brokerLoad.get(leaderId);
    TopicPartition tpWithDotHandled = partitionHandleDotInTopicName(tpDotNotHandled);
    if (skipBuildingPartitionMetricSample(tpDotNotHandled, tpWithDotHandled, leaderId, brokerLoad)) {
      return null;
    }

    double partitionSize = brokerLoad.partitionMetric(tpWithDotHandled.topic(), tpWithDotHandled.partition(), PARTITION_SIZE);
    // Fill in all the common metrics.
    MetricDef commonMetricDef = KafkaMetricDef.commonMetricDef();
    PartitionMetricSample pms = new PartitionMetricSample(leaderId, tpDotNotHandled);
    int numLeaderPartitionsOnBroker = leaderDistributionStats.get(leaderId).get(tpDotNotHandled.topic());
    for (RawMetricType rawTopicMetricType : RawMetricType.topicMetricTypes()) {
      MetricInfo metricInfo = commonMetricDef.metricInfo(KafkaMetricDef.forRawMetricType(rawTopicMetricType).name());
      double metricValue = brokerLoad.topicMetrics(tpWithDotHandled.topic(), rawTopicMetricType);
      pms.record(metricInfo, numLeaderPartitionsOnBroker == 0 ? 0 : metricValue / numLeaderPartitionsOnBroker);
    }
    // Fill in disk and CPU utilization, which are not topic metric types.
    pms.record(commonMetricDef.metricInfo(KafkaMetricDef.DISK_USAGE.name()), partitionSize);
    pms.record(commonMetricDef.metricInfo(KafkaMetricDef.CPU_USAGE.name()), estimateLeaderCpuUtil(pms, brokerLoad, commonMetricDef));
    pms.close(_maxMetricTimestamp);
    return pms;
  }
}
