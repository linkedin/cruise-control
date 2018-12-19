/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.PartitionMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.TopicMetric;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.*;


/**
 * This class is to help process the raw metrics collected by {@link CruiseControlMetricsReporterSampler} from the
 * Kafka cluster.
 */
public class CruiseControlMetricsProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(CruiseControlMetricsProcessor.class);
  private static final double MAX_ALLOWED_MISSING_PARTITION_METRIC_PERCENT = 0.01;
  private static final double MAX_ALLOWED_MISSING_TOPIC_METRIC_PERCENT = 0.01;
  private static final int BYTES_IN_KB = 1024;
  private static final int BYTES_IN_MB = 1024 * 1024;
  private final Map<Integer, BrokerLoad> _brokerLoad;
  private long _maxMetricTimestamp = -1;

  CruiseControlMetricsProcessor() {
    _brokerLoad = new HashMap<>();
  }

  void addMetric(CruiseControlMetric metric) {
    int brokerId = metric.brokerId();
    LOG.trace("Adding cruise control metric {}", metric);
    _maxMetricTimestamp = Math.max(metric.time(), _maxMetricTimestamp);
    _brokerLoad.compute(brokerId, (bid, load) -> {
      BrokerLoad brokerLoad = load == null ? new BrokerLoad() : load;
      brokerLoad.recordMetric(metric);
      return brokerLoad;
    });
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
        continue;
      } else if (!brokerLoad.minRequiredBrokerMetricsAvailable()) {
        if (brokerLoad.missingBrokerMetricsInMinSupportedVersion().size() == 0) {
          LOG.warn("Skip generating broker metric sample for broker {} because there are not enough topic metrics to "
                  + "generate broker metrics.", node.id());
        } else {
          LOG.warn("Skip generating broker metric sample for broker {} because the following required metrics are missing {}.",
              node.id(), brokerLoad.missingBrokerMetricsInMinSupportedVersion());
        }
        continue;
      }

      boolean validSample = true;
      BrokerMetricSample brokerMetricSample = new BrokerMetricSample(node.host(), node.id(), brokerLoad._brokerSampleDeserializationVersion);
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

  /**
   * A helper function to get the number of leader partitions for each topic on each broker. It is useful to
   * derive the partition level IO from the topic level IO on a broker.
   * TODO: create open source KIP to provide per partition IO metrics.
   */
  private Map<Integer, Map<String, Integer>> leaderDistributionStats(Cluster cluster) {
    Map<Integer, Map<String, Integer>> stats = new HashMap<>();
    for (Node node : cluster.nodes()) {
      Map<String, Integer> numLeadersByTopic = new HashMap<>();
      stats.put(node.id(), numLeadersByTopic);
      for (PartitionInfo partitionInfo : cluster.partitionsForNode(node.id())) {
        numLeadersByTopic.merge(partitionInfo.topic(), 1, (v0, v1) -> v0 + v1);
      }
    }
    return stats;
  }

  private PartitionMetricSample buildPartitionMetricSample(Cluster cluster,
                                                           TopicPartition tpDotNotHandled,
                                                           Map<Integer, Map<String, Integer>> leaderDistributionStats) {
    TopicPartition tpWithDotHandled = ModelUtils.partitionHandleDotInTopicName(tpDotNotHandled);
    Node leaderNode = cluster.leaderFor(tpDotNotHandled);
    if (leaderNode == null) {
      return null;
    }
    int leaderId = leaderNode.id();
    //TODO: switch to linear regression model without computing partition level CPU usage.
    BrokerLoad brokerLoad = _brokerLoad.get(leaderId);
    // Ensure broker load is available.
    if (brokerLoad == null || !brokerLoad.brokerMetricAvailable(BROKER_CPU_UTIL)) {
      LOG.debug("Skip generating metric sample for partition {} because broker metric for broker {} is unavailable.",
                tpDotNotHandled, leaderId);
      return null;
    }
    // Ensure the topic load is available.
    if (!brokerLoad.allDotHandledTopicMetricsAvailable(tpWithDotHandled.topic())) {
      LOG.debug("Skip generating metric samples for partition {} because broker {} has no metric or topic metrics "
                    + "are not available", tpDotNotHandled, leaderId);
      return null;
    }
    if (!brokerLoad.partitionMetricAvailable(tpWithDotHandled, PARTITION_SIZE)) {
      // This broker is no longer the leader.
      LOG.debug("Skip generating metric sample for partition {} because broker {} no long host the partition.", tpDotNotHandled, leaderId);
      return null;
    }
    // Ensure there is a partition size.
    double partSize = brokerLoad.partitionMetric(tpWithDotHandled.topic(), tpWithDotHandled.partition(), PARTITION_SIZE);
    // Fill in all the common metrics.
    PartitionMetricSample pms = new PartitionMetricSample(leaderId, tpDotNotHandled);
    MetricDef commonMetricDef = KafkaMetricDef.commonMetricDef();
    int numLeaderPartitionsOnBroker = leaderDistributionStats.get(leaderId).get(tpDotNotHandled.topic());
    for (RawMetricType rawTopicMetricType : RawMetricType.topicMetricTypes()) {
      MetricInfo metricInfo = commonMetricDef.metricInfo(KafkaMetricDef.forRawMetricType(rawTopicMetricType).name());
      double metricValue = brokerLoad.topicMetrics(tpWithDotHandled.topic(), rawTopicMetricType);
      pms.record(metricInfo, numLeaderPartitionsOnBroker == 0 ? 0 : metricValue / numLeaderPartitionsOnBroker);
    }
    // Fill in disk usage are not a topic metric type
    pms.record(commonMetricDef.metricInfo(KafkaMetricDef.DISK_USAGE.name()), partSize);
    // fill in CPU usage, which is not a topic metric type
    double partitionBytesInRate = pms.metricValue(commonMetricDef.metricInfo(KafkaMetricDef.LEADER_BYTES_IN.name()).id());
    double partitionBytesOutRate = pms.metricValue(commonMetricDef.metricInfo(KafkaMetricDef.LEADER_BYTES_OUT.name()).id());
    double partitionReplicationBytesOutRate =
        pms.metricValue(commonMetricDef.metricInfo(KafkaMetricDef.REPLICATION_BYTES_OUT_RATE.name()).id());
    double brokerTotalBytesOut = brokerLoad.brokerMetric(ALL_TOPIC_BYTES_OUT) + brokerLoad.brokerMetric(ALL_TOPIC_REPLICATION_BYTES_OUT);
    double cpuUsage = ModelUtils.estimateLeaderCpuUtil(brokerLoad.brokerMetric(BROKER_CPU_UTIL),
                                                       brokerLoad.brokerMetric(ALL_TOPIC_BYTES_IN),
                                                       brokerTotalBytesOut,
                                                       brokerLoad.brokerMetric(ALL_TOPIC_REPLICATION_BYTES_IN),
                                                       partitionBytesInRate,
                                                       partitionBytesOutRate + partitionReplicationBytesOutRate);
    pms.record(commonMetricDef.metricInfo(KafkaMetricDef.CPU_USAGE.name()), cpuUsage);
    pms.close(_maxMetricTimestamp);
    return pms;
  }

  private static double convertUnit(double value, RawMetricType rawMetricType) {
    switch (rawMetricType) {
      case ALL_TOPIC_BYTES_IN:
      case ALL_TOPIC_BYTES_OUT:
      case ALL_TOPIC_REPLICATION_BYTES_IN:
      case ALL_TOPIC_REPLICATION_BYTES_OUT:
      case TOPIC_BYTES_IN:
      case TOPIC_BYTES_OUT:
      case TOPIC_REPLICATION_BYTES_IN:
      case TOPIC_REPLICATION_BYTES_OUT:
        return value / BYTES_IN_KB;

      case PARTITION_SIZE:
        return value / BYTES_IN_MB;

      default:
        return value;
    }
  }

  /**
   * Some helper classes.
   */
  private static class BrokerLoad {
    private final RawMetricsHolder _brokerMetrics = new RawMetricsHolder();
    private final Map<String, RawMetricsHolder> _dotHandledTopicMetrics = new HashMap<>();
    private final Map<TopicPartition, RawMetricsHolder> _dotHandledPartitionMetrics = new HashMap<>();
    // Remember which topic has partition size reported. Because the topic level IO metrics are only created when
    // there is IO, the topic level IO metrics may be missing if there was no traffic to the topic on the broker.
    // However, because the partition size will always be reported, when we see partition size was reported for
    // a topic but the topic level IO metrics are not reported, we assume there was no traffic to the topic.
    private final Set<String> _dotHandledTopicsWithPartitionSizeReported = new HashSet<>();
    private final Set<RawMetricType> _missingBrokerMetricsInMinSupportedVersion = new HashSet<>();

    private static final Map<RawMetricType, RawMetricType> METRIC_TYPES_TO_SUM = new HashMap<>();
    static {
      METRIC_TYPES_TO_SUM.put(TOPIC_PRODUCE_REQUEST_RATE, ALL_TOPIC_PRODUCE_REQUEST_RATE);
      METRIC_TYPES_TO_SUM.put(TOPIC_FETCH_REQUEST_RATE, ALL_TOPIC_FETCH_REQUEST_RATE);
      METRIC_TYPES_TO_SUM.put(TOPIC_BYTES_IN, ALL_TOPIC_BYTES_IN);
      METRIC_TYPES_TO_SUM.put(TOPIC_BYTES_OUT, ALL_TOPIC_BYTES_OUT);
      METRIC_TYPES_TO_SUM.put(TOPIC_REPLICATION_BYTES_IN, ALL_TOPIC_REPLICATION_BYTES_IN);
      METRIC_TYPES_TO_SUM.put(TOPIC_REPLICATION_BYTES_OUT, ALL_TOPIC_REPLICATION_BYTES_OUT);
      METRIC_TYPES_TO_SUM.put(TOPIC_MESSAGES_IN_PER_SEC, ALL_TOPIC_MESSAGES_IN_PER_SEC);
    }

    private boolean _minRequiredBrokerMetricsAvailable = false;
    // Set to the latest possible deserialization version based on the sampled data.
    private byte _brokerSampleDeserializationVersion = -1;

    private void recordMetric(CruiseControlMetric ccm) {
      RawMetricType rawMetricType = ccm.rawMetricType();
      switch (rawMetricType.metricScope()) {
        case BROKER:
          _brokerMetrics.recordCruiseControlMetric(ccm);
          break;
        case TOPIC:
          TopicMetric tm = (TopicMetric) ccm;
          _dotHandledTopicMetrics.computeIfAbsent(tm.topic(), t -> new RawMetricsHolder())
                                 .recordCruiseControlMetric(ccm);
          break;
        case PARTITION:
          PartitionMetric pm = (PartitionMetric) ccm;
          _dotHandledPartitionMetrics.computeIfAbsent(new TopicPartition(pm.topic(), pm.partition()), tp -> new RawMetricsHolder())
                                     .recordCruiseControlMetric(ccm);
          _dotHandledTopicsWithPartitionSizeReported.add(pm.topic());
          break;
        default:
          throw new IllegalStateException(String.format("Should never be here. Unrecognized metric scope %s",
                                                        rawMetricType.metricScope()));
      }
    }

    private boolean allDotHandledTopicMetricsAvailable(String dotHandledTopic) {
      // We rely on the partition size metric to determine whether a topic metric is available or not.
      // The topic names in this set are dot handled -- i.e. dots (".") in topic name is replaced with underscores ("_").
      // Note that metrics reporter implicitly does this conversion, but the metadata topic names keep the original name.
      return _dotHandledTopicsWithPartitionSizeReported.contains(dotHandledTopic);
    }

    private boolean minRequiredBrokerMetricsAvailable() {
      return _minRequiredBrokerMetricsAvailable;
    }

    private boolean brokerMetricAvailable(RawMetricType rawMetricType) {
      return _brokerMetrics.metricValue(rawMetricType) != null;
    }

    private boolean partitionMetricAvailable(TopicPartition tpWithDotHandled, RawMetricType rawMetricType) {
      RawMetricsHolder rawMetricsHolder = _dotHandledPartitionMetrics.get(tpWithDotHandled);
      return rawMetricsHolder != null && rawMetricsHolder.metricValue(rawMetricType) != null;
    }

    private Set<RawMetricType> missingBrokerMetricsInMinSupportedVersion() {
      return _missingBrokerMetricsInMinSupportedVersion;
    }

    private double brokerMetric(RawMetricType rawMetricType) {
      checkMetricScope(rawMetricType, BROKER);
      ValueHolder valueHolder = _brokerMetrics.metricValue(rawMetricType);
      if (valueHolder == null) {
        throw new IllegalArgumentException(String.format("Broker metric %s does not exist.", rawMetricType));
      } else {
        return convertUnit(valueHolder.value(), rawMetricType);
      }
    }

    private double topicMetrics(String dotHandledTopic, RawMetricType rawMetricType) {
      return topicMetrics(dotHandledTopic, rawMetricType, true);
    }

    private double topicMetrics(String dotHandledTopic, RawMetricType rawMetricType, boolean convertUnit) {
      checkMetricScope(rawMetricType, TOPIC);
      if (!allDotHandledTopicMetricsAvailable(dotHandledTopic)) {
        throw new IllegalArgumentException(String.format("Topic metric %s does not exist.", rawMetricType));
      }
      RawMetricsHolder rawMetricsHolder = _dotHandledTopicMetrics.get(dotHandledTopic);
      if (rawMetricsHolder == null || rawMetricsHolder.metricValue(rawMetricType) == null) {
          return 0.0;
      }
      double rawMetricValue = rawMetricsHolder.metricValue(rawMetricType).value();
      return convertUnit ? convertUnit(rawMetricValue, rawMetricType) : rawMetricValue;
    }

    private double partitionMetric(String dotHandledTopic, int partition, RawMetricType rawMetricType) {
      checkMetricScope(rawMetricType, PARTITION);
      RawMetricsHolder metricsHolder = _dotHandledPartitionMetrics.get(new TopicPartition(dotHandledTopic, partition));
      if (metricsHolder == null || metricsHolder.metricValue(rawMetricType) == null) {
        throw new IllegalArgumentException(String.format("Partition metric %s does not exist.", rawMetricType));
      } else {
        return convertUnit(metricsHolder.metricValue(rawMetricType).value(), rawMetricType);
      }
    }

    private void checkMetricScope(RawMetricType rawMetricType, MetricScope expectedMetricScope) {
      if (rawMetricType.metricScope() != expectedMetricScope) {
        throw new IllegalArgumentException(String.format("Metric scope %s does not match the expected metric scope %s",
                                                         rawMetricType.metricScope(), expectedMetricScope));
      }
    }

    /**
     * Due to the yammer metric exponential decaying mechanism, the broker metric and the sum of the partition metrics
     * on the same broker may differ by a lot. Our experience shows that in that case, the sum of the topic/partition
     * level metrics are more accurate. So we will just replace the following metrics with the sum of topic/partition
     * level metrics:
     * <ul>
     *   <li>BrokerProduceRate</li>
     *   <li>BrokerFetchRate</li>
     *   <li>BrokerLeaderBytesInRate</li>
     *   <li>BrokerLeaderBytesOutRate</li>
     *   <li>BrokerReplicationBytesInRate</li>
     *   <li>BrokerReplicationBytesOutRate</li>
     *   <li>BrokerMessagesInRate</li>
     * </ul>
     *
     * We use the cluster metadata to check if the reported topic level metrics are complete. If the reported topic
     * level metrics are not complete, we ignore the broker metric sample by setting the _minRequiredBrokerMetricsAvailable
     * flag to false.
     *
     * @param cluster The Kafka cluster.
     * @param brokerId The broker id to prepare metrics for.
     * @param time The last sample time.
     */
    private void prepareBrokerMetrics(Cluster cluster, int brokerId, long time) {
      boolean enoughTopicPartitionMetrics = enoughTopicPartitionMetrics(cluster, brokerId);
      // Ensure there are enough topic level metrics.
      if (enoughTopicPartitionMetrics) {
        Map<RawMetricType, Double> sumOfTopicMetrics = new HashMap<>();
        for (String dotHandledTopic : _dotHandledTopicsWithPartitionSizeReported) {
          METRIC_TYPES_TO_SUM.keySet().forEach(type -> {
            double value = topicMetrics(dotHandledTopic, type, false);
            sumOfTopicMetrics.compute(type, (t, v) -> (v == null ? 0 : v) + value);
          });
        }
        for (Map.Entry<RawMetricType, Double> entry : sumOfTopicMetrics.entrySet()) {
          RawMetricType rawTopicMetricType = entry.getKey();
          double value = entry.getValue();
          _brokerMetrics.setRawMetricValue(METRIC_TYPES_TO_SUM.get(rawTopicMetricType), value, time);
        }
      }
      // Check if all the broker raw metrics are available.
      maybeSetBrokerRawMetrics(cluster, brokerId, time);

      // A broker metric is only available if it has enough valid topic metrics and it has reported
      // replication bytes in/out metrics.
      _minRequiredBrokerMetricsAvailable = enoughTopicPartitionMetrics && _missingBrokerMetricsInMinSupportedVersion.isEmpty();
    }

    /**
     * Use the latest supported version for which the raw broker metrics are available.
     *
     * 1) If broker metrics are incomplete for the {@link BrokerMetricSample#MIN_SUPPORTED_VERSION}, then broker metrics
     * are insufficient to create a broker sample.
     * 2) If broker metrics are complete in a version-X but not in (version-X + 1), then use version-X.
     *
     * {@link #_missingBrokerMetricsInMinSupportedVersion} is relevant only for {@link BrokerMetricSample#MIN_SUPPORTED_VERSION}.
     *
     * @param cluster The Kafka cluster.
     * @param brokerId The broker id to prepare metrics for.
     * @param time The last sample time.
     */
    private void maybeSetBrokerRawMetrics(Cluster cluster, int brokerId, long time) {
      for (byte v = BrokerMetricSample.MIN_SUPPORTED_VERSION; v <= BrokerMetricSample.LATEST_SUPPORTED_VERSION; v++) {
        Set<RawMetricType> missingBrokerMetrics = new HashSet<>();
        for (RawMetricType rawBrokerMetricType : RawMetricType.brokerMetricTypesDiffForVersion(v)) {
          if (_brokerMetrics.metricValue(rawBrokerMetricType) == null) {
            if (allowMissingBrokerMetric(cluster, brokerId, rawBrokerMetricType)) {
              // If the metric is allowed to be missing, we simply use 0 as the value.
              _brokerMetrics.setRawMetricValue(rawBrokerMetricType, 0.0, time);
            } else {
              missingBrokerMetrics.add(rawBrokerMetricType);
            }
          }
        }

        if (!missingBrokerMetrics.isEmpty()) {
          if (_brokerSampleDeserializationVersion == -1) {
            _missingBrokerMetricsInMinSupportedVersion.addAll(missingBrokerMetrics);
          }
          break;
        } else {
          // Set supported deserialization version so far.
          _brokerSampleDeserializationVersion = v;
        }
      }
      // Set nullable broker metrics for missing metrics if a valid deserialization exists with an old version.
      setNullableBrokerMetrics();
    }

    public byte brokerSampleDeserializationVersion() {
      return _brokerSampleDeserializationVersion;
    }

    private void setNullableBrokerMetrics() {
      if (_brokerSampleDeserializationVersion != -1) {
        Set<RawMetricType> nullableBrokerMetrics = new HashSet<>();
        for (byte v = (byte) (_brokerSampleDeserializationVersion + 1); v <= BrokerMetricSample.LATEST_SUPPORTED_VERSION; v++) {
          Set<RawMetricType> nullableMetrics = new HashSet<>(RawMetricType.brokerMetricTypesDiffForVersion(v));
          nullableBrokerMetrics.addAll(nullableMetrics);
        }
        nullableBrokerMetrics.forEach(nullableMetric -> _brokerMetrics.setRawMetricValue(nullableMetric, 0.0, 0L));
      }
    }

    /**
     * Check if a broker raw metric is reasonable to be missing. As of now, it looks that only the following metrics
     * might be missing:
     * <ul>
     *   <li>BROKER_FOLLOWER_FETCH_REQUEST_RATE</li>
     *   <li>BROKER_LOG_FLUSH_RATE</li>
     *   <li>BROKER_LOG_FLUSH_TIME_MS_MEAN</li>
     *   <li>BROKER_LOG_FLUSH_TIME_MS_MAX</li>
     * </ul>
     * When these raw metrics are missing, we are going to use 0 as the value.
     *
     * @param cluster the Kafka cluster.
     * @param brokerId the id of the broker whose raw metric is missing
     * @param rawMetricType the raw metric type that is missing.
     * @return true if the missing is allowed, false otherwise.
     */
    private boolean allowMissingBrokerMetric(Cluster cluster, int brokerId, RawMetricType rawMetricType) {
      switch (rawMetricType) {
        case BROKER_FOLLOWER_FETCH_REQUEST_RATE:
          for (PartitionInfo partitionInfo : cluster.partitionsForNode(brokerId)) {
            // If there is at least one leader partition on the broker that meets the following condition:
            // 1. replication factor is greater than 1,
            // 2. there are more than one alive replicas.
            // Then the broker must report BrokerFollowerFetchRequestRate.
            if (partitionInfo.replicas().length > 1
                && partitionInfo.leader() != null
                && partitionInfo.leader().id() == brokerId) {
              return false;
            }
          }
          return true;
        case BROKER_LOG_FLUSH_RATE:
        case BROKER_LOG_FLUSH_TIME_MS_MEAN:
        case BROKER_LOG_FLUSH_TIME_MS_MAX:
        case BROKER_LOG_FLUSH_TIME_MS_50TH:
        case BROKER_LOG_FLUSH_TIME_MS_999TH:
        case BROKER_PRODUCE_REQUEST_RATE:
        case BROKER_CONSUMER_FETCH_REQUEST_RATE:
          return true;
        default:
          return false;
      }
    }

    /**
     * Verify whether we have collected enough metrics to generate the broker metric samples. The broker must have
     * missed less than {@link CruiseControlMetricsProcessor#MAX_ALLOWED_MISSING_TOPIC_METRIC_PERCENT} of the topic level
     * and {@link CruiseControlMetricsProcessor#MAX_ALLOWED_MISSING_PARTITION_METRIC_PERCENT} partition level metrics in the
     * broker to generate broker level metrics.
     *
     * @param cluster The Kafka cluster.
     * @param brokerId The broker id to check.
     * @return True if there are enough topic level metrics, false otherwise.
     */
    private boolean enoughTopicPartitionMetrics(Cluster cluster, int brokerId) {
      Set<String> missingTopics = new HashSet<>();
      Set<String> topicsInBroker = new HashSet<>();
      AtomicInteger missingPartitions = new AtomicInteger(0);
      List<PartitionInfo> leaderPartitionsInNode = cluster.partitionsForNode(brokerId);
      if (leaderPartitionsInNode.isEmpty()) {
        // If the broker does not have any leader partition, return true immediately.
        return true;
      }
      leaderPartitionsInNode.forEach(info -> {
        String topicWithDotHandled = ModelUtils.replaceDotsWithUnderscores(info.topic());
        topicsInBroker.add(topicWithDotHandled);
        if (!_dotHandledTopicsWithPartitionSizeReported.contains(topicWithDotHandled)) {
          missingPartitions.incrementAndGet();
          missingTopics.add(topicWithDotHandled);
        }
      });
      boolean result = ((double) missingTopics.size() / topicsInBroker.size()) <= MAX_ALLOWED_MISSING_TOPIC_METRIC_PERCENT
          && ((double) missingPartitions.get() / cluster.partitionsForNode(brokerId).size() <= MAX_ALLOWED_MISSING_PARTITION_METRIC_PERCENT);
      if (!result) {
        LOG.warn("Broker {} is missing {}/{} topics metrics and {}/{} leader partition metrics. Missing leader topics: {}.", brokerId,
            missingTopics.size(), topicsInBroker.size(), missingPartitions.get(), cluster.partitionsForNode(brokerId).size(), missingTopics);
      }
      return result;
    }

    private double diskUsage() {
      double result = 0.0;
      for (RawMetricsHolder rawMetricsHolder : _dotHandledPartitionMetrics.values()) {
        result += rawMetricsHolder.metricValue(RawMetricType.PARTITION_SIZE).value();
      }
      return convertUnit(result, PARTITION_SIZE);
    }
  }

  /**
   * A class that helps store all the {@link CruiseControlMetric} by their {@link RawMetricType}.
   */
  private static class RawMetricsHolder {
    private final Map<RawMetricType, ValueHolder> _rawMetricsByType = new HashMap<>();

    /**
     * Record a cruise control metric value.
     * @param ccm the {@link CruiseControlMetric} to record.
     */
    void recordCruiseControlMetric(CruiseControlMetric ccm) {
      RawMetricType rawMetricType = ccm.rawMetricType();
      ValueHolder valueHolder = _rawMetricsByType.computeIfAbsent(rawMetricType, mt -> getValueHolderFor(rawMetricType));
      valueHolder.recordValue(ccm.value(), ccm.time());
    }

    /**
     * Directly set a raw metric value. The existing metric value will be discarded.
     * This method is used when we have to modify the raw metric values to unify the meaning of the metrics across
     * different Kafka versions.
     *
     * @param rawMetricType the raw metric type to set value for.
     * @param value the value to set
     * @param time the time to
     */
    void setRawMetricValue(RawMetricType rawMetricType, double value, long time) {
      _rawMetricsByType.compute(rawMetricType, (type, vh) -> {
        ValueHolder valueHolder = vh == null ? getValueHolderFor(rawMetricType) : vh;
        valueHolder.reset();
        valueHolder.recordValue(value, time);
        return valueHolder;
      });
    }

    /**
     * Get the value for the given raw metric type.
     * @param rawMetricType the raw metric type to get value for.
     * @return the value of the given raw metric type.
     */
    ValueHolder metricValue(RawMetricType rawMetricType) {
      return _rawMetricsByType.get(rawMetricType);
    }

    private ValueHolder getValueHolderFor(RawMetricType rawMetricType) {
      KafkaMetricDef kafkaMetricDef = KafkaMetricDef.forRawMetricType(rawMetricType);
      switch (kafkaMetricDef.valueComputingStrategy()) {
        case AVG:
        case MAX:
          return new ValueAndCount();
        case LATEST:
          return new ValueAndCount();
        default:
          throw new IllegalStateException("Should never be here");
      }
    }
  }

  /**
   * A private interface to unify the {@link ValueAndTime} and {@link ValueAndCount}
   */
  private interface ValueHolder {
    void recordValue(double value, long time);
    void reset();
    double value();
    double value(boolean assertNonZeroCount);
  }

  /**
   * A private class to give average of the recorded values.
   */
  private static class ValueAndTime implements ValueHolder {
    private double _value = 0.0;
    private long _time = -1;

    @Override
    public void recordValue(double value, long time) {
      if (time > _time) {
        _value = value;
        _time = time;
      }
    }

    @Override
    public void reset() {
      _value = 0.0;
      _time = -1;
    }

    @Override
    public double value() {
      return _value;
    }

    @Override
    public double value(boolean assertNonZeroCount) {
      return _value;
    }
  }

  /**
   * A private class to give the latest of the recorded values.
   */
  private static class ValueAndCount implements ValueHolder {
    private double _value = 0.0;
    private int _count = 0;

    @Override
    public void recordValue(double value, long time) {
      _value += value;
      _count++;
    }

    @Override
    public void reset() {
      _value = 0.0;
      _count = 0;
    }

    @Override
    public double value() {
      return value(false);
    }

    @Override
    public double value(boolean assertNonZeroCount) {
      return _count == 0 ? (assertNonZeroCount ? -1.0 : 0.0) : _value / _count;
    }
  }
}
