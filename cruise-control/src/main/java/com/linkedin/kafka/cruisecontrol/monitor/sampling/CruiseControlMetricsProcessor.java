/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.PartitionMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.TopicMetric;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;


/**
 * This class is to help process the raw metrics collected by {@link CruiseControlMetricsReporterSampler} from the
 * Kafka cluster.
 */
public class CruiseControlMetricsProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(CruiseControlMetricsProcessor.class);
  private static final int BYTES_IN_KB = 1024;
  private static final int BYTES_IN_MB = 1024 * 1024;
  private final Map<Integer, BrokerLoad> _brokerLoad;
  private final Map<Integer, Double> _brokerReplicationBytesInRate;
  private long _maxMetricTimestamp = -1;

  CruiseControlMetricsProcessor() {
    _brokerLoad = new HashMap<>();
    _brokerReplicationBytesInRate = new HashMap<>();
  }

  void addMetric(CruiseControlMetric metric) {
    int brokerId = metric.brokerId();
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
   * @param cluster the Kafka cluster.
   * @param partitions the partitions to construct samples for.
   * @param samplingMode the sampling mode to indicate which type of samples are needed.
   * @param metricDef the metric definition.
   *                  
   * @return the constructed metric samples.
   */
  MetricSampler.Samples process(Cluster cluster,
                                Collection<TopicPartition> partitions,
                                MetricSampler.SamplingMode samplingMode,
                                MetricDef metricDef) {
    Map<Integer, Map<String, Integer>> leaderDistributionStats = leaderDistributionStats(cluster);
    maybeDeriveMetrics(cluster, leaderDistributionStats);
    // Theoretically we should not move forward at all if a broker reported a different all topic bytes in from all
    // its resident replicas. However, it is not clear how often this would happen yet. At this point we still
    // continue process the other brokers. Later on if in practice all topic bytes in and the aggregation value is
    // rarely inconsistent we can just stop the sample generation when the this happens.
    _brokerLoad.forEach((broker, load) -> load.prepareMetrics(cluster, broker, _maxMetricTimestamp));

    Set<PartitionMetricSample> partitionMetricSamples = new HashSet<>();
    Set<BrokerMetricSample> brokerMetricSamples = new HashSet<>();

    int skippedPartition = 0;
    if (samplingMode == MetricSampler.SamplingMode.ALL
        || samplingMode == MetricSampler.SamplingMode.PARTITION_METRICS_ONLY) {
      for (TopicPartition tp : partitions) {
        try {
          PartitionMetricSample sample = buildPartitionMetricSample(cluster, tp, leaderDistributionStats, metricDef);
          if (sample != null) {
            LOG.debug("Added partition metrics sample for {}", tp);
            partitionMetricSamples.add(sample);
          } else {
            skippedPartition++;
          }
        } catch (Exception e) {
          LOG.error("Error building partition metric sample for " + tp, e);
          skippedPartition++;
        }
      }
    }

    int skippedBroker = 0;
    if (samplingMode == MetricSampler.SamplingMode.ALL
        || samplingMode == MetricSampler.SamplingMode.BROKER_METRICS_ONLY) {
      MetricDef brokerMetricDef = KafkaMetricDef.brokerMetricDef();
      for (Node node : cluster.nodes()) {
        BrokerLoad brokerLoad = _brokerLoad.get(node.id());
        if (brokerLoad == null || !brokerLoad.brokerMetricsAvailable()) {
          // A new broker or broker metrics are not consistent.
          LOG.debug("Skip generating broker metric sample for broker {} because it does not have IO load metrics or "
                        + "the metrics are inconsistent.", node.id());
          skippedBroker++;
          continue;
        }

        boolean validSample = true;
        BrokerMetricSample brokerMetricSample = new BrokerMetricSample(node.rack(), node.id());
        for (RawMetricType rawBrokerMetricType : RawMetricType.brokerMetricTypes()) {
          MetricInfo metricInfo = brokerMetricDef.metricInfo(KafkaMetricDef.forRawMetricType(rawBrokerMetricType).name());
          ValueHolder rawMetricValue = brokerLoad.brokerMetric(rawBrokerMetricType);
          // We require the broker to report all the metric types. Otherwise we skip the broker.
          if (rawMetricValue == null) {
            skippedBroker++;
            validSample = false;
            LOG.debug("Skip generating broker metric sample for broker {} because it does not have %s metrics or "
                          + "the metrics are inconsistent.", node.id(), rawBrokerMetricType);
            break;
          }
          brokerMetricSample.record(metricInfo, brokerLoad.brokerMetric(rawBrokerMetricType).value());
        }
        if (validSample) {
          brokerMetricSample.record(brokerMetricDef.metricInfo(DISK_USAGE.name()), brokerLoad.diskUsage());
          brokerMetricSample.close(_maxMetricTimestamp);

          LOG.debug("Added broker metric sample for broker {}", node.id());
          brokerMetricSamples.add(brokerMetricSample);
        }
      }
    }
    LOG.info("Generated {}{} partition metric samples and {}{} broker metric samples for timestamp {}",
             partitionMetricSamples.size(), skippedPartition > 0 ? "(" + skippedPartition + " skipped)" : "",
             brokerMetricSamples.size(), skippedBroker > 0 ? "(" + skippedBroker + " skipped)" : "",
             _maxMetricTimestamp);
    return new MetricSampler.Samples(partitionMetricSamples, brokerMetricSamples);
  }

  synchronized void clear() {
    _brokerLoad.clear();
    _brokerReplicationBytesInRate.clear();
    _maxMetricTimestamp = -1L;
  }

  private void maybeDeriveMetrics(Cluster cluster, Map<Integer, Map<String, Integer>> leaderDistributionStats) {
    synchronized (this) {
      if (!_brokerReplicationBytesInRate.isEmpty()) {
        return;
      }
      for (Node node : cluster.nodes()) {
        _brokerReplicationBytesInRate.putIfAbsent(node.id(), 0.0);
        BrokerLoad brokerLoad = _brokerLoad.get(node.id());
        if (brokerLoad == null) {
          // new broker?
          continue;
        }
        if (brokerLoad.brokerMetric(ALL_TOPIC_REPLICATION_BYTES_IN) != null
            || brokerLoad.brokerMetric(ALL_TOPIC_BYTES_OUT) != null) {
          // The broker does not need to have derived metrics.
          continue;
        }
        for (PartitionInfo partitionInfo : cluster.partitionsForNode(node.id())) {
          if (!brokerLoad.topicMetricsAvailable(partitionInfo.topic())) {
            // The topic did not report any IO metric and the partition does not exist on the broker.
            LOG.debug("No IO load reported from broker {} for partition {}-{}",
                      node.id(), partitionInfo.topic(), partitionInfo.partition());
            continue;
          }
          int numLeadersOnBroker = leaderDistributionStats.get(node.id()).get(partitionInfo.topic());
          double partitionBytesIn = brokerLoad.topicMetrics(partitionInfo.topic(), TOPIC_BYTES_IN) / numLeadersOnBroker;
          for (Node replica : partitionInfo.replicas()) {
            if (replica.id() != node.id()) {
              _brokerReplicationBytesInRate.merge(replica.id(), partitionBytesIn, (v0, v1) -> v0 + v1);
            }
          }
        }
      }
    }
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
                                                           TopicPartition tp,
                                                           Map<Integer, Map<String, Integer>> leaderDistributionStats) {
    TopicPartition tpWithDotHandled = partitionHandleDotInTopicName(tp);
    int leaderId = cluster.leaderFor(tp).id();
    BrokerLoad brokerLoad = _brokerLoad.get(leaderId);
    if (brokerLoad == null || !brokerLoad.topicMetricsAvailable(tp.topic())) {
      LOG.debug("Skip generating metric samples for partition {} because broker {} has no metric or topic metrics " 
                    + "are not available", tp, leaderId);
      return null;
    }
    //TODO: switch to linear regression model without computing partition level CPU usage.
    double leaderBrokerCpuUtil = brokerLoad.brokerMetric(BROKER_CPU_UTIL).value();
    if (leaderBrokerCpuUtil < 0) {
      LOG.debug("Skip generating metric sample for partition {} because broker {} did not report CPU utilization.", tp, leaderId);
      return null;
    }
    RawMetricsHolder partitionMetrics = brokerLoad.partitionMetrics(tpWithDotHandled.topic(), tpWithDotHandled.partition());
    if (partitionMetrics == null || partitionMetrics.metricValue(PARTITION_SIZE) == null) {
      // This broker is no longer the leader.
      LOG.debug("Skip generating metric sample for partition {} because broker {} is no longer the leader.", tp, leaderId);
      return null;
    }

    PartitionMetricSample pms = new PartitionMetricSample(leaderId, tp);
    MetricDef commonMetricDef = KafkaMetricDef.commonMetricDef();
    int numLeaderPartitionsOnBroker = leaderDistributionStats.get(leaderId).get(tp.topic());
    for (RawMetricType rawTopicMetricType : RawMetricType.topicMetricTypes()) {
      MetricInfo metricInfo = commonMetricDef.metricInfo(KafkaMetricDef.forRawMetricType(rawTopicMetricType).name());
      double metricValue = brokerLoad.topicMetrics(tpWithDotHandled.topic(), rawTopicMetricType);
      if (rawTopicMetricType == TOPIC_BYTES_IN || rawTopicMetricType == TOPIC_BYTES_OUT) {
        metricValue = metricValue / BYTES_IN_KB;
      }
      pms.record(metricInfo, metricValue / numLeaderPartitionsOnBroker);
    }
    // CPU and Disk usage are not in topic metric types
    double partSize = partitionMetrics.metricValue(PARTITION_SIZE).value();
    pms.record(commonMetricDef.metricInfo(KafkaMetricDef.DISK_USAGE.name()), partSize / BYTES_IN_MB);
    
    double partitionBytesInRate = pms.metricValue(commonMetricDef.metricInfo(KafkaMetricDef.LEADER_BYTES_IN.name()).id());
    double partitionBytesOutRate = pms.metricValue(commonMetricDef.metricInfo(KafkaMetricDef.LEADER_BYTES_OUT.name()).id());
    double cpuUsage = ModelUtils.estimateLeaderCpuUtil(leaderBrokerCpuUtil, 
                                                       brokerLoad.brokerMetric(ALL_TOPIC_BYTES_IN).value() / BYTES_IN_KB, 
                                                       brokerLoad.brokerMetric(ALL_TOPIC_BYTES_OUT).value() / BYTES_IN_KB, 
                                                       _brokerReplicationBytesInRate.get(leaderId), 
                                                       partitionBytesInRate, 
                                                       partitionBytesOutRate);
    pms.record(commonMetricDef.metricInfo(KafkaMetricDef.DISK_USAGE.name()), cpuUsage);
    pms.close(_maxMetricTimestamp);
    return pms;
  }

  private TopicPartition partitionHandleDotInTopicName(TopicPartition tp) {
    // In the reported metrics, the "." in the topic name will be replaced by "_".
    return !tp.topic().contains(".") ? tp :
      new TopicPartition(tp.topic().replace('.', '_'), tp.partition());
  }

  /**
   * Some helper classes.
   */
  private static class BrokerLoad {
    private final RawMetricsHolder _brokerMetrics = new RawMetricsHolder();
    private final Map<String, RawMetricsHolder> _topicMetrics = new HashMap<>();
    private final Map<TopicPartition, RawMetricsHolder> _partitionMetrics = new HashMap<>();
    // Remember which topic has partition size reported. Because the topic level IO metrics are only created when
    // there is IO, the topic level IO metrics may be missing if there was no traffic to the topic on the broker.
    // However, because the partition size will always be reported, when we see partition size was reported for
    // a topic but the topic level IO metrics are not reported, we assume there was no traffic to the topic.
    private final Set<String> _topicsWithPartitionSizeReported = new HashSet<>(); 

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
    
    private boolean _valid = true;

    private void recordMetric(CruiseControlMetric ccm) {
      RawMetricType rawMetricType = ccm.rawMetricType();
      switch (rawMetricType.metricScope()) {
        case BROKER:
          _brokerMetrics.recordCruiseControlMetric(ccm);
          break;
        case TOPIC:
          TopicMetric tm = (TopicMetric) ccm;
          _topicMetrics.computeIfAbsent(tm.topic(), t -> new RawMetricsHolder())
                       .recordCruiseControlMetric(ccm);
          break;
        case PARTITION:
          PartitionMetric pm = (PartitionMetric) ccm;
          _partitionMetrics.computeIfAbsent(new TopicPartition(pm.topic(), pm.partition()), tp -> new RawMetricsHolder())
                           .recordCruiseControlMetric(ccm);
          _topicsWithPartitionSizeReported.add(pm.topic());
          break;
        default:
          throw new IllegalStateException(String.format("Should never be here. Unrecognized metric scope %s", 
                                                        rawMetricType.metricScope()));
      }
    }
    
    private boolean hasReplicationMetrics() {
      // If the broker does not have replication bytes IO reported at all, do not generate the broker metrics
      // samples.
      return _brokerMetrics.metricValue(ALL_TOPIC_REPLICATION_BYTES_IN) != null
          || _brokerMetrics.metricValue(ALL_TOPIC_REPLICATION_BYTES_OUT) != null;
    }
    
    private boolean topicMetricsAvailable(String topic) {
      return _topicsWithPartitionSizeReported.contains(topic);
    }
    
    private boolean brokerMetricsAvailable() {
      return hasReplicationMetrics() && _valid;
    }
    
    private ValueHolder brokerMetric(RawMetricType rawMetricType) {
       return _brokerMetrics.metricValue(rawMetricType);
    }
    
    private double topicMetrics(String topic, RawMetricType rawMetricType) {
      RawMetricsHolder rawMetricsHolder = _topicMetrics.get(topic);
      if (rawMetricsHolder == null || rawMetricsHolder.metricValue(rawMetricType) == null) {
          return 0.0;
      }
      return rawMetricsHolder.metricValue(rawMetricType).value();
    }
    
    private RawMetricsHolder partitionMetrics(String topic, int partition) {
      return _partitionMetrics.get(new TopicPartition(topic, partition));
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
     * level metrics are not complete, we ignore the broker metric sample by setting the valid flat to false.
     * 
     * @param cluster The Kafka cluster.
     * @param brokerId the broker id to prepare metrics for.
     * @param time the last sample time.
     */
    private void prepareMetrics(Cluster cluster, int brokerId, long time) {
      _valid = validateMetrics(cluster, brokerId);
      if (_valid && hasReplicationMetrics()) {
        Map<RawMetricType, Double> sumOfTopicMetrics = new HashMap<>();
        for (String topic : _topicsWithPartitionSizeReported) {
          METRIC_TYPES_TO_SUM.keySet().forEach(type -> {
            sumOfTopicMetrics.compute(type, (t, v) -> (v == null ? 0 : v) + topicMetrics(topic, type));
          });
        }
        for (Map.Entry<RawMetricType, Double> entry : sumOfTopicMetrics.entrySet()) {
          RawMetricType rawTopicMetricType = entry.getKey();
          double value = entry.getValue();
          _brokerMetrics.setRawMetricValue(METRIC_TYPES_TO_SUM.get(rawTopicMetricType), value, time);
        }
      }
    }

    /**
     * Verify whether we have collected enough metrics to generate the broker metric samples. The broker must have
     * collected more than 99% of the topic level metrics in the broker to generate broker level metrics.
     * 
     * @param cluster the Kafka cluster.
     * @param brokerId the broker id to check.
     * @return true if there are enough topic level metrics, false otherwise.
     */
    private boolean validateMetrics(Cluster cluster, int brokerId) {
      Set<String> missingTopics = new HashSet<>();
      Set<String> topicsInBroker = new HashSet<>();
      cluster.partitionsForNode(brokerId).forEach(info -> {
        topicsInBroker.add(info.topic());
        if (!_topicsWithPartitionSizeReported.contains(info.topic())) {
          missingTopics.add(info.topic());
        }
      });
      return ((double) missingTopics.size() / topicsInBroker.size()) <= 0.01;
    }
    
    private double diskUsage() {
      double result = 0.0;
      for (RawMetricsHolder rawMetricsHolder : _partitionMetrics.values()) {
        result += rawMetricsHolder.metricValue(RawMetricType.PARTITION_SIZE).value();
      }
      return result;
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
