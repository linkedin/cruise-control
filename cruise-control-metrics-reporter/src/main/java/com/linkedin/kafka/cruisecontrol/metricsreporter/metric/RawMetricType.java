/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.BROKER;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.TOPIC;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.PARTITION;


/**
 * The metric type helps the metric sampler to distinguish what metric a value is representing. These metrics are
 * called raw metrics because they are the most basic information reported by the Kafka brokers without any processing.
 * Each metric type has an id for serde purpose.
 */
public enum RawMetricType {
  ALL_TOPIC_BYTES_IN(BROKER, (byte) 0, (byte) 4),
  ALL_TOPIC_BYTES_OUT(BROKER, (byte) 1, (byte) 4),
  TOPIC_BYTES_IN(TOPIC, (byte) 2),
  TOPIC_BYTES_OUT(TOPIC, (byte) 3),
  PARTITION_SIZE(PARTITION, (byte) 4),
  BROKER_CPU_UTIL(BROKER, (byte) 5, (byte) 4),
  ALL_TOPIC_REPLICATION_BYTES_IN(BROKER, (byte) 6, (byte) 4),
  ALL_TOPIC_REPLICATION_BYTES_OUT(BROKER, (byte) 7, (byte) 4),
  // Note that this is different from broker produce request rate. If one ProduceRequest produces to 3 partitions,
  // it would be counted as one ProduceRequest on the broker, but ALL_TOPIC_PRODUCE_REQUEST would increment by 3.
  // The multiplier is the number of the partitions in the produce request.
  ALL_TOPIC_PRODUCE_REQUEST_RATE(BROKER, (byte) 8, (byte) 4),
  // Note that this is different from broker fetch request rate. If one FetchRequest fetches from 3 partitions,
  // it would be counted as one FetchRequest on the broker, but ALL_TOPIC_FETCH_REQUEST would increment by 3.
  // The multiplier is the number of the partitions in the fetch request.
  ALL_TOPIC_FETCH_REQUEST_RATE(BROKER, (byte) 9, (byte) 4),
  ALL_TOPIC_MESSAGES_IN_PER_SEC(BROKER, (byte) 10, (byte) 4),
  TOPIC_REPLICATION_BYTES_IN(TOPIC, (byte) 11),
  TOPIC_REPLICATION_BYTES_OUT(TOPIC, (byte) 12),
  TOPIC_PRODUCE_REQUEST_RATE(TOPIC, (byte) 13),
  TOPIC_FETCH_REQUEST_RATE(TOPIC, (byte) 14),
  TOPIC_MESSAGES_IN_PER_SEC(TOPIC, (byte) 15),
  BROKER_PRODUCE_REQUEST_RATE(BROKER, (byte) 16, (byte) 4),
  BROKER_CONSUMER_FETCH_REQUEST_RATE(BROKER, (byte) 17, (byte) 4),
  BROKER_FOLLOWER_FETCH_REQUEST_RATE(BROKER, (byte) 18, (byte) 4),
  BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT(BROKER, (byte) 19, (byte) 4),
  BROKER_REQUEST_QUEUE_SIZE(BROKER, (byte) 20, (byte) 4),
  BROKER_RESPONSE_QUEUE_SIZE(BROKER, (byte) 21, (byte) 4),
  BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX(BROKER, (byte) 22, (byte) 4),
  BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN(BROKER, (byte) 23, (byte) 4),
  BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX(BROKER, (byte) 24, (byte) 4),
  BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN(BROKER, (byte) 25, (byte) 4),
  BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX(BROKER, (byte) 26, (byte) 4),
  BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN(BROKER, (byte) 27, (byte) 4),
  BROKER_PRODUCE_TOTAL_TIME_MS_MAX(BROKER, (byte) 28, (byte) 4),
  BROKER_PRODUCE_TOTAL_TIME_MS_MEAN(BROKER, (byte) 29, (byte) 4),
  BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX(BROKER, (byte) 30, (byte) 4),
  BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN(BROKER, (byte) 31, (byte) 4),
  BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX(BROKER, (byte) 32, (byte) 4),
  BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN(BROKER, (byte) 33, (byte) 4),
  BROKER_PRODUCE_LOCAL_TIME_MS_MAX(BROKER, (byte) 34, (byte) 4),
  BROKER_PRODUCE_LOCAL_TIME_MS_MEAN(BROKER, (byte) 35, (byte) 4),
  BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX(BROKER, (byte) 36, (byte) 4),
  BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN(BROKER, (byte) 37, (byte) 4),
  BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX(BROKER, (byte) 38, (byte) 4),
  BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN(BROKER, (byte) 39, (byte) 4),
  BROKER_LOG_FLUSH_RATE(BROKER, (byte) 40, (byte) 4),
  BROKER_LOG_FLUSH_TIME_MS_MAX(BROKER, (byte) 41, (byte) 4),
  BROKER_LOG_FLUSH_TIME_MS_MEAN(BROKER, (byte) 42, (byte) 4),
  BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH(BROKER, (byte) 43, (byte) 5),
  BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH(BROKER, (byte) 44, (byte) 5),
  BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH(BROKER, (byte) 45, (byte) 5),
  BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH(BROKER, (byte) 46, (byte) 5),
  BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH(BROKER, (byte) 47, (byte) 5),
  BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH(BROKER, (byte) 48, (byte) 5),
  BROKER_PRODUCE_TOTAL_TIME_MS_50TH(BROKER, (byte) 49, (byte) 5),
  BROKER_PRODUCE_TOTAL_TIME_MS_999TH(BROKER, (byte) 50, (byte) 5),
  BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH(BROKER, (byte) 51, (byte) 5),
  BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH(BROKER, (byte) 52, (byte) 5),
  BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH(BROKER, (byte) 53, (byte) 5),
  BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH(BROKER, (byte) 54, (byte) 5),
  BROKER_PRODUCE_LOCAL_TIME_MS_50TH(BROKER, (byte) 55, (byte) 5),
  BROKER_PRODUCE_LOCAL_TIME_MS_999TH(BROKER, (byte) 56, (byte) 5),
  BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH(BROKER, (byte) 57, (byte) 5),
  BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH(BROKER, (byte) 58, (byte) 5),
  BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH(BROKER, (byte) 59, (byte) 5),
  BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH(BROKER, (byte) 60, (byte) 5),
  BROKER_LOG_FLUSH_TIME_MS_50TH(BROKER, (byte) 61, (byte) 5),
  BROKER_LOG_FLUSH_TIME_MS_999TH(BROKER, (byte) 62, (byte) 5);

  private static final List<RawMetricType> CACHED_VALUES = Arrays.asList(RawMetricType.values());
  private static final SortedMap<Byte, Set<RawMetricType>> BROKER_METRIC_TYPES_DIFF_BY_VERSION = buildBrokerMetricTypesDiffByVersion();
  private static final List<RawMetricType> TOPIC_METRIC_TYPES = buildMetricTypeList(TOPIC);
  private static final List<RawMetricType> PARTITION_METRIC_TYPES = buildMetricTypeList(PARTITION);
  private final byte _id;
  private final MetricScope _metricScope;
  private final byte _supportedVersionSince;

  RawMetricType(MetricScope scope, byte id) {
    this(scope, id, (byte) -1);
  }

  RawMetricType(MetricScope scope, byte id, byte supportedVersionSince) {
    _id = id;
    _metricScope = scope;
    _supportedVersionSince = supportedVersionSince;
  }

  public byte id() {
    return _id;
  }

  public MetricScope metricScope() {
    return _metricScope;
  }

  public byte supportedVersionSince() {
    return _supportedVersionSince;
  }

  public static List<RawMetricType> allMetricTypes() {
    return CACHED_VALUES;
  }

  public static Map<Byte, Set<RawMetricType>> brokerMetricTypesDiffByVersion() {
    return BROKER_METRIC_TYPES_DIFF_BY_VERSION;
  }

  public static Set<RawMetricType> brokerMetricTypesDiffForVersion(byte version) {
    return BROKER_METRIC_TYPES_DIFF_BY_VERSION.get(version);
  }

  public static List<RawMetricType> topicMetricTypes() {
    return TOPIC_METRIC_TYPES;
  }

  public static List<RawMetricType> partitionMetricTypes() {
    return PARTITION_METRIC_TYPES;
  }

  /**
   * @param id Cruise Control Metric type.
   * @return Raw metric type.
   */
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

  private static SortedMap<Byte, Set<RawMetricType>> buildBrokerMetricTypesDiffByVersion() {
    SortedMap<Byte, Set<RawMetricType>> buildBrokerMetricTypesDiffByVersion = new TreeMap<>();
    for (RawMetricType type : RawMetricType.values()) {
      if (type.metricScope() == BROKER) {
        buildBrokerMetricTypesDiffByVersion.computeIfAbsent(type.supportedVersionSince(), t -> new HashSet<>()).add(type);
      }
    }

    return buildBrokerMetricTypesDiffByVersion;
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
