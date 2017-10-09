/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.PartitionMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.TopicMetric;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
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


/**
 * This class is to help process the raw metrics collected by {@link CruiseControlMetricsReporterSampler} from the
 * Kafka cluster.
 */
public class CruiseControlMetricsProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(CruiseControlMetricsProcessor.class);
  private static final int BYTES_IN_KB = 1024;
  private static final int BYTES_IN_MB = 1024 * 1024;
  private final Map<Integer, BrokerLoad> _brokerLoad;
  private final Map<Integer, Double> _brokerFollowerLoad;
  private long _maxMetricTimestamp = -1;

  CruiseControlMetricsProcessor() {
    _brokerLoad = new HashMap<>();
    _brokerFollowerLoad = new HashMap<>();
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

  MetricSampler.Samples process(Cluster cluster,
                                Collection<TopicPartition> partitions,
                                MetricSampler.SamplingMode samplingMode) {
    Map<Integer, Map<String, Integer>> leaderDistributionStats = leaderDistributionStats(cluster);
    fillInFollowerBytesInRate(cluster, leaderDistributionStats);
    //TODO: maybe need to skip the entire processing logic if broker load is not consistent.
    // Theoretically we should not move forward at all if a broker reported a different all topic bytes in from all
    // its resident replicas. However, it is not clear how often this would happen yet. At this point we still
    // continue process the other brokers. Later on if in practice all topic bytes in and the aggregation value is
    // rarely inconsistent we can just stop the sample generation when the this happens.
    _brokerLoad.values().forEach(BrokerLoad::validate);

    Set<PartitionMetricSample> partitionMetricSamples = new HashSet<>();
    Set<BrokerMetricSample> brokerMetricSamples = new HashSet<>();

    int skippedPartition = 0;
    if (samplingMode == MetricSampler.SamplingMode.ALL
        || samplingMode == MetricSampler.SamplingMode.PARTITION_METRICS_ONLY) {
      for (TopicPartition tp : partitions) {
        try {
          PartitionMetricSample sample = buildPartitionMetricSample(cluster, tp, leaderDistributionStats);
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
      for (Node node : cluster.nodes()) {
        BrokerLoad brokerLoad = _brokerLoad.get(node.id());
        if (brokerLoad == null || !brokerLoad.isValid()) {
          // A new broker or broker metrics are not consistent.
          LOG.debug("Skip generating broker metric sample for broker {} because it does not have IO load metrics or "
                        + "the metrics are inconsistent.", node.id());
          skippedBroker++;
          continue;
        }
        double leaderCpuUtil = brokerLoad.cpuUtil();
        if (leaderCpuUtil > 0) {
          BrokerMetricSample brokerMetricSample =
              new BrokerMetricSample(node.id(),
                                     leaderCpuUtil,
                                     brokerLoad.bytesIn() / BYTES_IN_KB,
                                     brokerLoad.bytesOut() / BYTES_IN_KB,
                                     // The replication bytes in is only available from Kafka 0.11.0 and above.
                                     (brokerLoad.replicationBytesIn() > 0 ?
                                         brokerLoad.replicationBytesIn() : _brokerFollowerLoad.get(node.id())) / BYTES_IN_KB,
                                     brokerLoad.replicationBytesOut() / BYTES_IN_KB,
                                     brokerLoad.messagesInRate(),
                                     brokerLoad.produceRequestRate(),
                                     brokerLoad.consumerFetchRequestRate(),
                                     brokerLoad.followerFetchRequestRate(),
                                     -1.0,
                                     brokerLoad.allTopicsProduceRequestRate(),
                                     brokerLoad.allTopicsFetchRequestRate(),
                                     _maxMetricTimestamp);
          LOG.debug("Added broker metric sample for broker {}", node.id());
          brokerMetricSamples.add(brokerMetricSample);
        } else {
          skippedBroker++;
        }
      }
    }
    LOG.info("Generated {}{} partition metric samples and {}{} broker metric samples for timestamp {}",
             partitionMetricSamples.size(), skippedPartition > 0 ? "(" + skippedPartition + " skipped)" : "",
             brokerMetricSamples.size(), skippedBroker > 0 ? "(" + skippedBroker + " skipped)" : "",
             _maxMetricTimestamp);
    return new MetricSampler.Samples(partitionMetricSamples, brokerMetricSamples);
  }

  void clear() {
    _brokerLoad.clear();
    _brokerFollowerLoad.clear();
    _maxMetricTimestamp = -1L;
  }

  private void fillInFollowerBytesInRate(Cluster cluster, Map<Integer, Map<String, Integer>> leaderDistributionStats) {
    synchronized (this) {
      if (!_brokerFollowerLoad.isEmpty()) {
        return;
      }
      for (Node node : cluster.nodes()) {
        _brokerFollowerLoad.putIfAbsent(node.id(), 0.0);
        BrokerLoad brokerLoad = _brokerLoad.get(node.id());
        if (brokerLoad == null) {
          // new broker?
          continue;
        }
        for (PartitionInfo partitionInfo : cluster.partitionsForNode(node.id())) {
          IOLoad topicIOLoad = brokerLoad.ioLoad(partitionInfo.topic(), partitionInfo.partition());
          if (topicIOLoad == null) {
            // The topic did not report any IO metric and the partition does not exist on the broker.
            LOG.debug("No IO load reported from broker {} for partition {}-{}",
                      node.id(), partitionInfo.topic(), partitionInfo.partition());
            continue;
          }
          int numLeadersOnBroker = leaderDistributionStats.get(node.id()).get(partitionInfo.topic());
          double partitionBytesIn = topicIOLoad.bytesIn() / numLeadersOnBroker;
          for (Node replica : partitionInfo.replicas()) {
            if (replica.id() != node.id()) {
              _brokerFollowerLoad.merge(replica.id(), partitionBytesIn, (v0, v1) -> v0 + v1);
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
    if (brokerLoad == null || !brokerLoad.isValid()) {
      LOG.debug("Skip generating metric samples for partition {} because broker {} has no metric or has inconsistent IO load.",
                tp, leaderId);
      return null;
    }
    double leaderCpuUtil = brokerLoad.cpuUtil();
    if (leaderCpuUtil < 0) {
      LOG.debug("Skip generating metric sample for partition {} because broker {} did not report CPU utilization.", tp, leaderId);
      return null;
    }
    ValueAndTime partSize = brokerLoad.size(tpWithDotHandled);
    if (partSize == null) {
      // This broker is no longer the leader.
      LOG.debug("Skip generating metric sample for partition {} because broker {} is no longer the leader.", tp, leaderId);
      return null;
    }

    int numLeaderPartitionsOnBroker = leaderDistributionStats.get(leaderId).get(tp.topic());

    IOLoad topicIOLoad = brokerLoad.ioLoad(tpWithDotHandled.topic());
    double topicBytesInInBroker = topicIOLoad.bytesIn();
    double topicBytesOutInBroker = topicIOLoad.bytesOut();
    double partitionBytesInRate = topicBytesInInBroker / numLeaderPartitionsOnBroker;
    double partitionBytesOutRate = topicBytesOutInBroker / numLeaderPartitionsOnBroker;

    PartitionMetricSample pms = new PartitionMetricSample(leaderId, tp);
    pms.record(Resource.NW_IN, partitionBytesInRate / BYTES_IN_KB);
    // TODO: After Kafka 0.11 the bytes out will exclude replication bytes, we need to take replication factor into consideration as well.
    pms.record(Resource.NW_OUT, partitionBytesOutRate / BYTES_IN_KB);
    pms.record(Resource.DISK, partSize.value() / BYTES_IN_MB);
    pms.record(Resource.CPU, ModelUtils.estimateLeaderCpuUtil(brokerLoad.cpuUtil(),
                                                              brokerLoad.bytesIn(),
                                                              brokerLoad.bytesOut(),
                                                              _brokerFollowerLoad.get(leaderId),
                                                              partitionBytesInRate,
                                                              partitionBytesOutRate));
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
  private class BrokerLoad extends IOLoad {
    private Map<String, IOLoad> _topicIOLoad = new HashMap<>();
    private Map<TopicPartition, ValueAndTime> _partitionSize = new HashMap<>();
    private ValueAndCount _cpuUtil = new ValueAndCount();
    private ValueAndCount _allTopicsProduceRequestRate = new ValueAndCount();
    private ValueAndCount _allTopicsFetchRequestRate = new ValueAndCount();
    private ValueAndCount _consumerFetchRequestRate = new ValueAndCount();
    private ValueAndCount _followerFetchRequestRate = new ValueAndCount();
    private boolean _valid = true;

    private void recordMetric(CruiseControlMetric ccm) {
      switch (ccm.metricType()) {
        case TOPIC_BYTES_IN:
          recordTopicBytesIn((TopicMetric) ccm);
          break;
        case TOPIC_BYTES_OUT:
          recordTopicBytesOut((TopicMetric) ccm);
          break;
        case ALL_TOPIC_PRODUCE_REQUEST_RATE:
          _allTopicsProduceRequestRate.addValue(ccm.value());
          break;
        case ALL_TOPIC_FETCH_REQUEST_RATE:
          _allTopicsFetchRequestRate.addValue(ccm.value());
          break;
        case BROKER_CONSUMER_FETCH_REQUEST_RATE:
          _consumerFetchRequestRate.addValue(ccm.value());
          break;
        case BROKER_FOLLOWER_FETCH_REQUEST_RATE:
          _followerFetchRequestRate.addValue(ccm.value());
          break;
        case BROKER_CPU_UTIL:
          _cpuUtil.addValue(ccm.value());
          break;
        case PARTITION_SIZE:
          recordPartitionSize((PartitionMetric) ccm);
          break;
        default:
          recordCruiseControlMetric(ccm);
          break;
      }
    }

    private void recordTopicBytesIn(TopicMetric tm) {
      _topicIOLoad.compute(tm.topic(), (topic, load) -> {
        IOLoad ioLoad = load == null ? new IOLoad() : load;
        ioLoad.recordCruiseControlMetric(tm);
        return ioLoad;
      });
    }

    private void recordTopicBytesOut(TopicMetric tm) {
      _topicIOLoad.compute(tm.topic(), (topic, load) -> {
        IOLoad ioLoad = load == null ? new IOLoad() : load;
        ioLoad.recordCruiseControlMetric(tm);
        return ioLoad;
      });
    }

    private void recordPartitionSize(PartitionMetric pm) {
      _partitionSize.compute(new TopicPartition(pm.topic(), pm.partition()), (tp, vat) -> {
        ValueAndTime valueAndTime = vat == null ? new ValueAndTime() : vat;
        valueAndTime.recordValue(pm.value(), pm.time());
        return valueAndTime;
      });
    }

    private void validate() {
      double aggAllBytesIn = 0.0;
      double aggAllBytesOut = 0.0;
      for (IOLoad topicLoad : _topicIOLoad.values()) {
        aggAllBytesIn += topicLoad.bytesIn();
        aggAllBytesOut += topicLoad.bytesOut();
      }
      double allBytesIn = bytesIn();
      double allBytesInError = Math.abs((aggAllBytesIn - allBytesIn) / allBytesIn);
      if (allBytesIn > 100 && allBytesInError > 0.05) {
        LOG.warn("Aggregated Topic Bytes In value {} does not match the broker AllByteIn metric value {}, error = {}",
                 aggAllBytesIn, allBytesIn, allBytesInError);
        _valid = false;
      }

      double allBytesOut = bytesOut();
      double allBytesOutError = Math.abs((aggAllBytesOut - allBytesOut) / allBytesOut);
      if (allBytesOut > 100 && allBytesOutError > 0.05) {
        LOG.warn("Aggregated Topic Bytes Out value {} does not match the broker AllByteOut metric value {}, error = {}",
                 aggAllBytesOut, allBytesOut, allBytesOutError);
        _valid = false;
      }
    }

    private boolean isValid() {
      return _valid;
    }

    private double cpuUtil() {
      return _cpuUtil.value(true);
    }

    private double allTopicsProduceRequestRate() {
      return _allTopicsProduceRequestRate.value();
    }

    private double allTopicsFetchRequestRate() {
      return _allTopicsFetchRequestRate.value();
    }

    private double consumerFetchRequestRate() {
      return _consumerFetchRequestRate.value();
    }

    private double followerFetchRequestRate() {
      return _followerFetchRequestRate.value();
    }

    private IOLoad ioLoad(String topic) {
      return _topicIOLoad.get(topic.replace('.', '_'));
    }

    private IOLoad ioLoad(String topic, int partition) {
      // The metric will replace . with _
      String topicWithDotHandled = topic.replace('.', '_');
      return _topicIOLoad.compute(topicWithDotHandled, (t, l) -> {
        if (l != null) {
          return l;
        }
        // If partition size has been reported on this partition, we create topic IO load for this topic.
        // This is because for topics that does not have an IO, the broker will not create the sensors for IO.
        return (_partitionSize.containsKey(new TopicPartition(topicWithDotHandled, partition))) ? new IOLoad() : null;
      });
    }

    private ValueAndTime size(TopicPartition tp) {
      return _partitionSize.get(partitionHandleDotInTopicName(tp));
    }
  }

  private static class IOLoad {
    private ValueAndCount _bytesIn = new ValueAndCount();
    private ValueAndCount _bytesOut = new ValueAndCount();
    private ValueAndCount _replicationBytesIn = new ValueAndCount();
    private ValueAndCount _replicationBytesOut = new ValueAndCount();
    private ValueAndCount _produceRequestRate = new ValueAndCount();
    private ValueAndCount _fetchRequestRate = new ValueAndCount();
    private ValueAndCount _messagesInRate = new ValueAndCount();

    void recordCruiseControlMetric(CruiseControlMetric ccm) {
      double value = ccm.value();
      switch (ccm.metricType()) {
        case ALL_TOPIC_BYTES_IN:
        case TOPIC_BYTES_IN:
          _bytesIn.addValue(value);
          break;
        case ALL_TOPIC_BYTES_OUT:
        case TOPIC_BYTES_OUT:
          _bytesOut.addValue(value);
          break;
        case ALL_TOPIC_REPLICATION_BYTES_IN:
        case TOPIC_REPLICATION_BYTES_IN:
          _replicationBytesIn.addValue(value);
          break;
        case ALL_TOPIC_REPLICATION_BYTES_OUT:
        case TOPIC_REPLICATION_BYTES_OUT:
          _replicationBytesOut.addValue(value);
          break;
        case BROKER_PRODUCE_REQUEST_RATE:
        case TOPIC_PRODUCE_REQUEST_RATE:
          _produceRequestRate.addValue(value);
          break;
        case TOPIC_FETCH_REQUEST_RATE:
          _fetchRequestRate.addValue(value);
          break;
        case ALL_TOPIC_MESSAGES_IN_PER_SEC:
        case TOPIC_MESSAGES_IN_PER_SEC:
          _messagesInRate.addValue(value);
          break;
        default:
          LOG.warn("CruiseControlMetric type {} should not be added to this IOLoad. This may happen when a new "
                       + "metric type is added. The current known metric type is from 0 to {}", ccm.metricType(),
                   MetricType.values().length - 1);
          break;
      }
    }

    double replicationBytesIn() {
      return _replicationBytesIn.value();
    }

    double replicationBytesOut() {
      return _replicationBytesOut.value();
    }

    double produceRequestRate() {
      return _produceRequestRate.value();
    }

    double fetchRequestRate() {
      return _fetchRequestRate.value();
    }

    double messagesInRate() {
      return _messagesInRate.value();
    }

    double bytesIn() {
      return _bytesIn.value();
    }

    double bytesOut() {
      return _bytesOut.value();
    }
  }

  private static class ValueAndTime {
    private double _value = 0.0;
    private long _time = -1;

    private void recordValue(double value, long time) {
      if (time > _time) {
        _value = value;
        _time = time;
      }
    }

    private double value() {
      return _value;
    }
  }

  private static class ValueAndCount {
    private double _value = 0.0;
    private int _count = 0;

    void addValue(double value) {
      _value += value;
      _count++;
    }

    int count() {
      return _count;
    }

    double value() {
      return value(false);
    }

    double value(boolean assertNonZeroCount) {
      return _count == 0 ? (assertNonZeroCount ? -1.0 : 0.0) : _value / _count;
    }
  }
}
