/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.BROKER;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.TOPIC;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.PARTITION;


/**
 * The metric type helps the metric sampler to distinguish what metric a value is representing. These metrics are
 * called raw metrics because they are the most basic information reported by the Kafka brokers without any processing.
 * Each metric type has an id for serde purpose.
 */
public enum RawMetricType {
  ALL_TOPIC_BYTES_IN(BROKER, (byte) 0),
  ALL_TOPIC_BYTES_OUT(BROKER, (byte) 1),
  TOPIC_BYTES_IN(TOPIC, (byte) 2),
  TOPIC_BYTES_OUT(TOPIC, (byte) 3),
  PARTITION_SIZE(PARTITION, (byte) 4),
  BROKER_CPU_UTIL(BROKER, (byte) 5),
  ALL_TOPIC_REPLICATION_BYTES_IN(BROKER, (byte) 6),
  ALL_TOPIC_REPLICATION_BYTES_OUT(BROKER, (byte) 7),
  // Note that this is different from broker produce request rate. If one ProduceRequest produces to 3 partitions,
  // it would be counted as one ProduceRequest on the broker, but ALL_TOPIC_PRODUCE_REQUEST would increment by 3.
  // The multiplier is the number of the partitions in the produce request.
  ALL_TOPIC_PRODUCE_REQUEST_RATE(BROKER, (byte) 8),
  // Note that this is different from broker fetch request rate. If one FetchRequest fetches from 3 partitions,
  // it would be counted as one FetchRequest on the broker, but ALL_TOPIC_FETCH_REQUEST would increment by 3.
  // The multiplier is the number of the partitions in the fetch request.
  ALL_TOPIC_FETCH_REQUEST_RATE(BROKER, (byte) 9),
  ALL_TOPIC_MESSAGES_IN_PER_SEC(BROKER, (byte) 10),
  TOPIC_REPLICATION_BYTES_IN(TOPIC, (byte) 11),
  TOPIC_REPLICATION_BYTES_OUT(TOPIC, (byte) 12),
  TOPIC_PRODUCE_REQUEST_RATE(TOPIC, (byte) 13),
  TOPIC_FETCH_REQUEST_RATE(TOPIC, (byte) 14),
  TOPIC_MESSAGES_IN_PER_SEC(TOPIC, (byte) 15),
  BROKER_PRODUCE_REQUEST_RATE(BROKER, (byte) 16),
  BROKER_CONSUMER_FETCH_REQUEST_RATE(BROKER, (byte) 17),
  BROKER_FOLLOWER_FETCH_REQUEST_RATE(BROKER, (byte) 18),
  BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT(BROKER, (byte) 19),
  BROKER_REQUEST_QUEUE_SIZE(BROKER, (byte) 20),
  BROKER_RESPONSE_QUEUE_SIZE(BROKER, (byte) 21),
  BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX(BROKER, (byte) 22),
  BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN(BROKER, (byte) 23),
  BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX(BROKER, (byte) 24),
  BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN(BROKER, (byte) 25),
  BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX(BROKER, (byte) 26),
  BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN(BROKER, (byte) 27),
  BROKER_PRODUCE_TOTAL_TIME_MS_MAX(BROKER, (byte) 28),
  BROKER_PRODUCE_TOTAL_TIME_MS_MEAN(BROKER, (byte) 29),
  BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX(BROKER, (byte) 30),
  BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN(BROKER, (byte) 31),
  BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX(BROKER, (byte) 32),
  BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN(BROKER, (byte) 33),
  BROKER_PRODUCE_LOCAL_TIME_MS_MAX(BROKER, (byte) 34),
  BROKER_PRODUCE_LOCAL_TIME_MS_MEAN(BROKER, (byte) 35),
  BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX(BROKER, (byte) 36),
  BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN(BROKER, (byte) 37),
  BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX(BROKER, (byte) 38),
  BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN(BROKER, (byte) 39),
  BROKER_LOG_FLUSH_RATE(BROKER, (byte) 40),
  BROKER_LOG_FLUSH_TIME_MS_MAX(BROKER, (byte) 41),
  BROKER_LOG_FLUSH_TIME_MS_MEAN(BROKER, (byte) 42);

  private static final List<RawMetricType> CACHED_VALUES = Arrays.asList(RawMetricType.values());
  private static final List<RawMetricType> BROKER_METRIC_TYPES = buildMetricTypeList(BROKER);
  private static final List<RawMetricType> TOPIC_METRIC_TYPES = buildMetricTypeList(TOPIC);
  private static final List<RawMetricType> PARTITION_METRIC_TYPES = buildMetricTypeList(PARTITION);
  private final byte _id;
  private final MetricScope _metricScope;

  RawMetricType(MetricScope scope, byte id) {
    _id = id;
    _metricScope = scope;
  }

  public byte id() {
    return _id;
  }

  public MetricScope metricScope() {
    return _metricScope;
  }

  public static List<RawMetricType> allMetricTypes() {
    return CACHED_VALUES;
  }

  public static List<RawMetricType> brokerMetricTypes() {
    return BROKER_METRIC_TYPES;
  }

  public static List<RawMetricType> topicMetricTypes() {
    return TOPIC_METRIC_TYPES;
  }

  public static List<RawMetricType> partitionMetricTypes() {
    return PARTITION_METRIC_TYPES;
  }

  public static RawMetricType forId(byte id) {
    if (id < values().length) {
      return values()[id];
    } else {
      throw new IllegalArgumentException("CruiseControlMetric type " + id + " does not exist.");
    }
  }

  public enum MetricScope {
    BROKER, TOPIC, PARTITION
  }

  private static List<RawMetricType> buildMetricTypeList(MetricScope metricScope) {
    List<RawMetricType> brokerMetricTypes = new ArrayList<>();
    for (RawMetricType type : RawMetricType.values()) {
      if (type.metricScope() == metricScope) {
        brokerMetricTypes.add(type);
      }
    }
    return brokerMetricTypes;
  }
}
