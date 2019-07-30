/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.MetricSamplingException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricSerde;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CruiseControlMetricsReporterSampler implements MetricSampler {
  private static final Logger LOG = LoggerFactory.getLogger(CruiseControlMetricsReporterSampler.class);
  // Configurations
  public static final String METRIC_REPORTER_SAMPLER_BOOTSTRAP_SERVERS = "metric.reporter.sampler.bootstrap.servers";
  public static final String METRIC_REPORTER_TOPIC = "metric.reporter.topic";
  // TODO: Remove the deprecated config.
  public static final String METRIC_REPORTER_TOPIC_PATTERN = "metric.reporter.topic.pattern";
  public static final String METRIC_REPORTER_SAMPLER_GROUP_ID = "metric.reporter.sampler.group.id";
  private static final long METRIC_REPORTER_CONSUMER_POLL_TIMEOUT = 5000L;
  // Default configs
  private static final String DEFAULT_METRIC_REPORTER_SAMPLER_GROUP_ID = "CruiseControlMetricsReporterSampler";
  private static final long DEFAULT_RECONNECT_BACKOFF_MS = 50L;
  private static final long ACCEPTABLE_NETWORK_DELAY_MS = 100L;
  // static metric processor for metrics aggregation.
  private static final CruiseControlMetricsProcessor METRICS_PROCESSOR = new CruiseControlMetricsProcessor();
  // static random token to avoid group conflict.
  private static final Random RANDOM = new Random();

  private Consumer<String, CruiseControlMetric> _metricConsumer;
  private String _metricReporterTopic;
  private Set<TopicPartition> _currentPartitionAssignment;
  // Due to delay introduced by KafkaProducer and network, the metric record's event time is smaller than append
  // time at broker side, sampler should take this delay into consideration when collecting metric records into samples.
  // _acceptableMetricRecordProduceDelayMs is a conservative estimate of this delay, if one record's event time not earlier
  // than starting_time_of_sampling_period minus _acceptableMetricRecordProduceDelayMs, it is included in the sample;
  // otherwise it is discarded.
  private long _acceptableMetricRecordProduceDelayMs;

  @Override
  public Samples getSamples(Cluster cluster,
                            Set<TopicPartition> assignedPartitions,
                            long startTimeMs,
                            long endTimeMs,
                            SamplingMode mode,
                            MetricDef metricDef,
                            long timeout) throws MetricSamplingException {
    if (refreshPartitionAssignment()) {
      return new Samples(Collections.emptySet(), Collections.emptySet());
    }
    // Now seek to the startTimeMs.
    Map<TopicPartition, Long> timestampToSeek = new HashMap<>(_currentPartitionAssignment.size());
    for (TopicPartition tp : _currentPartitionAssignment) {
      timestampToSeek.put(tp, startTimeMs);
    }
    Set<TopicPartition> assignment = new HashSet<>(_currentPartitionAssignment);
    Map<TopicPartition, Long> endOffsets = _metricConsumer.endOffsets(assignment);
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = _metricConsumer.offsetsForTimes(timestampToSeek);
    sanityCheckOffsetFetch(endOffsets, offsetsForTimes);
    // If some partitions do not have data, we simply seek to the end offset. To avoid losing metrics, we use the end
    // offsets before the timestamp query.
    assignment.removeAll(offsetsForTimes.keySet());
    assignment.forEach(tp -> _metricConsumer.seek(tp, endOffsets.get(tp)));
    // For the partition that returned an offset, seek to the returned offsets.
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
      TopicPartition tp = entry.getKey();
      OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
      _metricConsumer.seek(tp, offsetAndTimestamp != null ? offsetAndTimestamp.offset() : endOffsets.get(tp));
    }
    LOG.debug("Starting consuming from metrics reporter topic partitions {}.", _currentPartitionAssignment);
    _metricConsumer.resume(_metricConsumer.paused());
    int totalMetricsAdded = 0;
    Set<TopicPartition> partitionsToPause = new HashSet<>();
    do {
      ConsumerRecords<String, CruiseControlMetric> records = _metricConsumer.poll(METRIC_REPORTER_CONSUMER_POLL_TIMEOUT);
      for (ConsumerRecord<String, CruiseControlMetric> record : records) {
        if (record == null) {
          // This means we cannot parse the metrics. It might happen when a newer type of metrics has been added and
          // the current code is still old. We simply ignore that metric in this case.
          LOG.warn("Cannot parse record, please update your Cruise Control version.");
          continue;
        }
        long recordTime = record.value().time();
        if (recordTime + _acceptableMetricRecordProduceDelayMs < startTimeMs) {
          LOG.debug("Discarding metric {} because its timestamp is more than {} ms earlier than the start time of sampling period {}.",
                    record.value(), _acceptableMetricRecordProduceDelayMs, startTimeMs);
        } else if (recordTime >= endTimeMs) {
          TopicPartition tp = new TopicPartition(record.topic(), record.partition());
          LOG.debug("Saw metric {} whose timestamp is larger than the end time of sampling period {}. Pausing "
                    + "partition {} at offset {}.", record.value(), endTimeMs, tp, record.offset());
          partitionsToPause.add(tp);
        } else {
          METRICS_PROCESSOR.addMetric(record.value());
          totalMetricsAdded++;
        }
      }
      if (!partitionsToPause.isEmpty()) {
        _metricConsumer.pause(partitionsToPause);
        partitionsToPause.clear();
      }
    } while (!consumptionDone(endOffsets) && System.currentTimeMillis() < timeout);
    LOG.info("Finished sampling for topic partitions {} in time range [{},{}]. Collected {} metrics.",
             _currentPartitionAssignment, startTimeMs, endTimeMs, totalMetricsAdded);

    try {
      if (totalMetricsAdded > 0) {
        return METRICS_PROCESSOR.process(cluster, assignedPartitions, mode);
      } else {
        return new Samples(Collections.emptySet(), Collections.emptySet());
      }
    } catch (UnknownVersionException e) {
      LOG.error("Unrecognized serde version detected during metric sampling.", e);
      return new Samples(Collections.emptySet(), Collections.emptySet());
    } finally {
      METRICS_PROCESSOR.clear();
    }
  }

  /**
   * The check if the consumption is done or not. The consumption is done if the consumer has caught up with the
   * log end or all the partitions are paused.
   * @param endOffsets the log end for each partition.
   * @return True if the consumption is done, false otherwise.
   */
  private boolean consumptionDone(Map<TopicPartition, Long> endOffsets) {
    Set<TopicPartition> partitionsNotPaused = new HashSet<>(_metricConsumer.assignment());
    partitionsNotPaused.removeAll(_metricConsumer.paused());
    for (TopicPartition tp : partitionsNotPaused) {
      if (_metricConsumer.position(tp) < endOffsets.get(tp)) {
        return false;
      }
    }
    return true;
  }

  private void sanityCheckOffsetFetch(Map<TopicPartition, Long> endOffsets,
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
   * Ensure that the {@link #_metricConsumer} is assigned to the latest partitions of the {@link #_metricReporterTopic}.
   * This enables metrics reporter sampler to handle dynamic partition size increases in {@link #_metricReporterTopic}.
   *
   * @return True if the set of partitions currently assigned to this consumer is empty, false otherwise.
   */
  private boolean refreshPartitionAssignment() {
    List<PartitionInfo> remotePartitionInfo = _metricConsumer.partitionsFor(_metricReporterTopic);
    if (remotePartitionInfo == null) {
      LOG.error("_metricConsumer returned null for _metricReporterTopic {}", _metricReporterTopic);
      return true;
    }
    if (remotePartitionInfo.isEmpty()) {
      _currentPartitionAssignment = Collections.emptySet();
      LOG.error("The set of partitions currently assigned to the metric consumer is empty.");
      return true;
    }

    // Ensure that reassignment overhead is avoided if partition set of the topic has not changed.
    if (remotePartitionInfo.size() == _currentPartitionAssignment.size()) {
      return false;
    }

    _currentPartitionAssignment = new HashSet<>(remotePartitionInfo.size());
    for (PartitionInfo partitionInfo : remotePartitionInfo) {
      _currentPartitionAssignment.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
    }

    _metricConsumer.assign(_currentPartitionAssignment);
    return false;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    int numSamplers = (Integer) configs.get(KafkaCruiseControlConfig.NUM_METRIC_FETCHERS_CONFIG);
    if (numSamplers != 1) {
      throw new ConfigException("CruiseControlMetricsReporterSampler is not thread safe. Please change " +
                                    KafkaCruiseControlConfig.NUM_METRIC_FETCHERS_CONFIG + " to 1");
    }

    String bootstrapServers = (String) configs.get(METRIC_REPORTER_SAMPLER_BOOTSTRAP_SERVERS);
    if (bootstrapServers == null) {
      bootstrapServers = configs.get(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG).toString();
      // Trim the brackets in List's String representation.
      if (bootstrapServers.length() > 2) {
        bootstrapServers = bootstrapServers.substring(1, bootstrapServers.length() - 1);
      }
    }
    _metricReporterTopic = (String) configs.get(METRIC_REPORTER_TOPIC);
    if (_metricReporterTopic == null) {
      _metricReporterTopic = (String) configs.get(METRIC_REPORTER_TOPIC_PATTERN);
      if (_metricReporterTopic == null) {
        _metricReporterTopic = CruiseControlMetricsReporterConfig.DEFAULT_CRUISE_CONTROL_METRICS_TOPIC;
      }
    }
    String groupId = (String) configs.get(METRIC_REPORTER_SAMPLER_GROUP_ID);
    if (groupId == null) {
      groupId = DEFAULT_METRIC_REPORTER_SAMPLER_GROUP_ID + "-" + RANDOM.nextLong();
    }
    String reconnectBackoffMs = configs.get(KafkaCruiseControlConfig.RECONNECT_BACKOFF_MS_CONFIG).toString();

    CruiseControlMetricsReporterConfig reporterConfig = new CruiseControlMetricsReporterConfig(configs, false);
    _acceptableMetricRecordProduceDelayMs = ACCEPTABLE_NETWORK_DELAY_MS +
        Math.max(reporterConfig.getLong(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_MAX_BLOCK_MS_CONFIG),
                 reporterConfig.getLong(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS_CONFIG));

    Properties consumerProps = new Properties();
    consumerProps.putAll(configs);
    Random random = new Random();
    consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, groupId + "-consumer-" + random.nextInt());
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(Integer.MAX_VALUE));
    consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MetricSerde.class.getName());
    consumerProps.setProperty(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, reconnectBackoffMs);
    _metricConsumer = new KafkaConsumer<>(consumerProps);
    _currentPartitionAssignment = Collections.emptySet();
    if (refreshPartitionAssignment()) {
      throw new IllegalStateException("Cruise Control cannot find partitions for the metrics reporter that topic matches "
                                      + _metricReporterTopic + " in the target cluster.");
    }
  }

  @Override
  public void close() {
    _metricConsumer.close();
  }
}
