/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.MetricSamplingException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricSerde;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CruiseControlMetricsReporterSampler implements MetricSampler {
  private static final Logger LOG = LoggerFactory.getLogger(CruiseControlMetricsReporterSampler.class);
  // Configurations
  public static final String METRIC_REPORTER_SAMPLER_BOOTSTRAP_SERVERS = "metric.reporter.sampler.bootstrap.servers";
  public static final String METRIC_REPORTER_TOPIC_PATTERN = "metric.reporter.topic.pattern";
  public static final String METRIC_REPORTER_SAMPLER_GROUP_ID = "metric.reporter.sampler.group.id";
  // Default configs
  private static final String DEFAULT_METRIC_REPORTER_SAMPLER_GROUP_ID = "CruiseControlMetricsReporterSampler";
  // static metric processor for metrics aggregation.
  private static final CruiseControlMetricsProcessor METRICS_PROCESSOR = new CruiseControlMetricsProcessor();
  // static random token to avoid group conflict.
  private static final Random RANDOM = new Random();

  private Consumer<String, CruiseControlMetric> _metricConsumer;
  @Override
  public Samples getSamples(Cluster cluster,
                            Set<TopicPartition> assignedPartitions,
                            long startTimeMs,
                            long endTimeMs,
                            SamplingMode mode) throws MetricSamplingException {
    // Ensure we have an assignment.
    while (_metricConsumer.assignment().isEmpty()) {
      _metricConsumer.poll(10);
    }
    // Now seek to the startTimeMs.
    Map<TopicPartition, Long> timestampToSeek = new HashMap<>();
    for (TopicPartition tp : _metricConsumer.assignment()) {
      timestampToSeek.put(tp, startTimeMs);
    }
    Set<TopicPartition> assignment = new HashSet<>(_metricConsumer.assignment());
    Map<TopicPartition, Long> endOffsets = _metricConsumer.endOffsets(assignment);
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = _metricConsumer.offsetsForTimes(timestampToSeek);
    // If some of the partitions does not have data, we simply seek to the end offset. To avoid losing metrics, we use
    // the end offsets before the timestamp query.
    assignment.removeAll(offsetsForTimes.keySet());
    for (TopicPartition tp : assignment) {
      _metricConsumer.seek(tp, endOffsets.get(tp));
    }
    // For the partition that returned an offset, seek to the returned offsets.
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
      TopicPartition tp = entry.getKey();
      OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
      if (offsetAndTimestamp != null) {
        _metricConsumer.seek(tp, offsetAndTimestamp.offset());
      } else {
        _metricConsumer.seek(tp, endOffsets.get(tp));
      }
    }
    LOG.debug("Starting consuming from metrics reporter topic.");
    _metricConsumer.resume(_metricConsumer.paused());
    int numMetricsAdded;
    int totalMetricsAdded = 0;
    long maxTimeStamp = -1L;
    do {
      numMetricsAdded = 0;
      ConsumerRecords<String, CruiseControlMetric> records = _metricConsumer.poll(5000L);
      for (ConsumerRecord<String, CruiseControlMetric> record : records) {
        if (record == null) {
          // This means we cannot parse the metrics. It might happen when a newer type of metrics has been added and
          // the current code is still old. We simply ignore that metric in this case.
          continue;
        }
        if (startTimeMs <= record.value().time() && record.value().time() < endTimeMs) {
          METRICS_PROCESSOR.addMetric(record.value());
          maxTimeStamp = Math.max(maxTimeStamp, record.value().time());
          numMetricsAdded++;
          totalMetricsAdded++;
        } else if (record.value().time() >= endTimeMs) {
          TopicPartition tp = new TopicPartition(record.topic(), record.partition());
          _metricConsumer.pause(Collections.singleton(tp));
        }
      }
    } while (numMetricsAdded != 0 || System.currentTimeMillis() < endTimeMs);
    LOG.debug("Finished sampling for time range [{},{}]. Collected {} metrics.", startTimeMs, endTimeMs, totalMetricsAdded);

    try {
      if (totalMetricsAdded > 0) {
        return METRICS_PROCESSOR.process(cluster, assignedPartitions, mode);
      } else {
        return new Samples(Collections.emptySet(), Collections.emptySet());
      }
    } finally {
      METRICS_PROCESSOR.clear();
    }
  }

  @Override
  public void configure(Map<String, ?> configs) {
    String numSamplersString = (String) configs.get(KafkaCruiseControlConfig.NUM_METRIC_FETCHERS_CONFIG);
    if (numSamplersString != null && Integer.parseInt(numSamplersString) != 1) {
      throw new ConfigException("CruiseControlMetricsReporterSampler is not thread safe. Please change " +
                                    KafkaCruiseControlConfig.NUM_METRIC_FETCHERS_CONFIG + " to 1");
    }

    String bootstrapServers = (String) configs.get(METRIC_REPORTER_SAMPLER_BOOTSTRAP_SERVERS);
    if (bootstrapServers == null) {
      bootstrapServers = (String) configs.get(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG);
    }
    String metricReporterTopic = (String) configs.get(METRIC_REPORTER_TOPIC_PATTERN);
    if (metricReporterTopic == null) {
      metricReporterTopic = CruiseControlMetricsReporterConfig.DEFAULT_CRUISE_CONTROL_METRICS_TOPIC;
    }
    String groupId = (String) configs.get(METRIC_REPORTER_SAMPLER_GROUP_ID);
    if (groupId == null) {
      groupId = DEFAULT_METRIC_REPORTER_SAMPLER_GROUP_ID + "-" + RANDOM.nextLong();
    }

    Properties consumerProps = new Properties();
    consumerProps.putAll(configs);
    Random random = new Random();
    consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG,
                              DEFAULT_METRIC_REPORTER_SAMPLER_GROUP_ID + "Consumer-" + random.nextInt());
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(Integer.MAX_VALUE));
    consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MetricSerde.class.getName());
    consumerProps.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
    _metricConsumer = new KafkaConsumer<>(consumerProps);
    _metricConsumer.subscribe(Pattern.compile(metricReporterTopic), new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        _metricConsumer.commitSync();
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        // Do nothing
      }
    });
    Pattern topicPattern = Pattern.compile(metricReporterTopic);
    for (String topic : _metricConsumer.listTopics().keySet()) {
      if (topicPattern.matcher(topic).matches()) {
        return;
      }
    }
    throw new IllegalStateException("Cruise Control cannot find sampling topic matches " + metricReporterTopic
        + " in the target cluster.");
  }

  @Override
  public void close() throws Exception {
    _metricConsumer.close();
  }
}
