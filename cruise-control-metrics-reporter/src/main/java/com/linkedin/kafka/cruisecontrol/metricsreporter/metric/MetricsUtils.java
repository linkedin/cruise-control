/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.metrics.KafkaMetric;


public class MetricsUtils {
  // Names
  private static final String BYTES_IN_PER_SEC = "BytesInPerSec";
  private static final String BYTES_OUT_PER_SEC = "BytesOutPerSec";
  private static final String REPLICATION_BYTES_IN_PER_SEC = "ReplicationBytesInPerSec";
  private static final String REPLICATION_BYTES_OUT_PER_SEC = "ReplicationBytesOutPerSec";
  private static final String REQUEST_PER_SEC = "RequestsPerSec";
  private static final String TOTAL_FETCH_REQUEST_PER_SEC = "TotalFetchRequestsPerSec";
  private static final String TOTAL_PRODUCE_REQUEST_PER_SEC = "TotalProduceRequestsPerSec";
  private static final String MESSAGES_IN_PER_SEC = "MessagesInPerSec";
  private static final String SIZE = "Size";
  private static final String REQUEST_HANDLER_AVG_IDLE_PERCENT = "RequestHandlerAvgIdlePercent";
  // Groups
  private static final String KAFKA_SERVER = "kafka.server";
  private static final String KAFKA_LOG = "kafka.log";
  private static final String KAFKA_NETWORK = "kafka.network";
  // Type Keys
  private static final String TYPE_KEY = "type";
  private static final String TOPIC_KEY = "topic";
  private static final String PARTITION_KEY = "partition";
  private static final String REQUEST_TYPE_KEY = "request";
  // Type
  private static final String BROKER_TOPIC_METRICS_GROUP = "BrokerTopicMetrics";
  private static final String LOG_GROUP = "Log";
  private static final String REQUEST_METRICS_GROUP = "RequestMetrics";
  private static final String REQUEST_KAFKA_HANDLER_POOL_GROUP = "KafkaRequestHandlerPool";

  // Tag Value
  private static final String CONSUMER_FETCH_REQUEST_TYPE = "FetchConsumer";
  private static final String FOLLOWER_FETCH_REQUEST_TYPE = "FetchFollower";
  private static final String PRODUCE_REQUEST_TYPE = "Produce";

  // Name Set.
  private static final Set<String> INTERESTED_NETWORK_METRIC_NAMES =
      Collections.unmodifiableSet(Collections.singleton(REQUEST_PER_SEC));

  private static final Set<String> INTERESTED_TOPIC_METRIC_NAMES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(BYTES_IN_PER_SEC,
                                                              BYTES_OUT_PER_SEC,
                                                              REPLICATION_BYTES_IN_PER_SEC,
                                                              REPLICATION_BYTES_OUT_PER_SEC,
                                                              TOTAL_FETCH_REQUEST_PER_SEC,
                                                              TOTAL_PRODUCE_REQUEST_PER_SEC,
                                                              MESSAGES_IN_PER_SEC)));

  private static final Set<String> INTERESTED_LOG_METRIC_NAMES =
      Collections.unmodifiableSet(Collections.singleton(SIZE));

  private static final Set<String> INTERESTED_SERVER_METRIC_NAMES =
      Collections.unmodifiableSet(Collections.singleton(REQUEST_HANDLER_AVG_IDLE_PERCENT));

  // Request type set
  private static final Set<String> INTERESTED_REQUEST_TYPE =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(CONSUMER_FETCH_REQUEST_TYPE,
                                                              FOLLOWER_FETCH_REQUEST_TYPE,
                                                              PRODUCE_REQUEST_TYPE)));

  private MetricsUtils() {

  }

  /**
   * Convert a KafkaMetric to a CruiseControlMetric
   */
  public static CruiseControlMetric toCruiseControlMetric(KafkaMetric kafkaMetric, long now, int brokerId) {
    org.apache.kafka.common.MetricName metricName = kafkaMetric.metricName();
    CruiseControlMetric ccm = toCruiseControlMetric(now, brokerId, metricName.name(), metricName.tags(), kafkaMetric.value());
    if (ccm == null) {
      throw new IllegalArgumentException(String.format("Cannot convert KafkaMetric %s to a Cruise Control metric for "
                                                           + "broker %d at time %d", kafkaMetric.metricName(), brokerId, now));
    }
    return ccm;
  }

  /**
   * Convert a Yammer metric to a CruiseControlMetric
   */
  public static CruiseControlMetric toCruiseControlMetric(long now,
                                                          int brokerId,
                                                          com.yammer.metrics.core.MetricName metricName,
                                                          double value) {

    CruiseControlMetric ccm =
        toCruiseControlMetric(now, brokerId, metricName.getName(), yammerMetricScopeToTags(metricName.getScope()), value);
    if (ccm == null) {
      throw new IllegalArgumentException(String.format("Cannot convert yammer metric %s to a Cruise Control metric for "
                                                           + "broker %d at time %d", metricName, brokerId, now));
    }
    return ccm;
  }

  public static BrokerMetric getCpuMetric(long now, int brokerId) {
    double cpuUtil = ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuLoad();
    return new BrokerMetric(MetricType.BROKER_CPU_UTIL, now, brokerId, cpuUtil);
  }

  /**
   * Check if a kafkaMetric is an interested metric.
   */
  public static boolean isInterested(org.apache.kafka.common.MetricName metricName) {
    String group = metricName.group();
    String name = metricName.name();
    String type = metricName.tags().get(TYPE_KEY);
    return isInterested(group, name, type, metricName.tags());
  }

  /**
   * Check if a yammer metric name is an interested metric
   */
  public static boolean isInterested(com.yammer.metrics.core.MetricName metricName) {
    return isInterested(metricName.getGroup(), metricName.getName(), metricName.getType(),
                        yammerMetricScopeToTags(metricName.getScope()));
  }

  /**
   * Convert a yammer metrics scope to a tags map.
   */
  private static Map<String, String> yammerMetricScopeToTags(String scope) {
    if (scope != null) {
      String[] kv = scope.split("\\.");
      assert kv.length % 2 == 0;
      Map<String, String> tags = new HashMap<>();
      for (int i = 0; i < kv.length; i += 2) {
        tags.put(kv[i], kv[i + 1]);
      }
      return tags;
    } else {
      return Collections.emptyMap();
    }
  }

  /**
   * Check if a metric is an interested metric.
   */
  private static boolean isInterested(String group, String name, String type, Map<String, String> tags) {
    if (group.equals(KAFKA_SERVER)) {
      if ((INTERESTED_TOPIC_METRIC_NAMES.contains(name) && BROKER_TOPIC_METRICS_GROUP.equals(type))
          || (INTERESTED_SERVER_METRIC_NAMES.contains(name) && REQUEST_KAFKA_HANDLER_POOL_GROUP.equals(type))) {
        return true;
      }
    } else if (group.equals(KAFKA_NETWORK)
        && INTERESTED_NETWORK_METRIC_NAMES.contains(name)
        && REQUEST_METRICS_GROUP.equals(type)
        && INTERESTED_REQUEST_TYPE.contains(tags.get(REQUEST_TYPE_KEY))) {
      return true;
    } else if (group.equals(KAFKA_LOG)
        && LOG_GROUP.equals(type)
        && INTERESTED_LOG_METRIC_NAMES.contains(name)) {
      return true;
    }
    return false;
  }

  /**
   * build a CruiseControlMetric object.
   */
  private static CruiseControlMetric toCruiseControlMetric(long now,
                                                           int brokerId,
                                                           String name,
                                                           Map<String, String> tags,
                                                           double value) {
    String topic = tags.get(TOPIC_KEY);
    switch (name) {
      case BYTES_IN_PER_SEC:
        if (topic != null) {
          return new TopicMetric(MetricType.TOPIC_BYTES_IN, now, brokerId, topic, value);
        } else {
          return new BrokerMetric(MetricType.ALL_TOPIC_BYTES_IN, now, brokerId, value);
        }
      case BYTES_OUT_PER_SEC:
        if (topic != null) {
          return new TopicMetric(MetricType.TOPIC_BYTES_OUT, now, brokerId, topic, value);
        } else {
          return new BrokerMetric(MetricType.ALL_TOPIC_BYTES_OUT, now, brokerId, value);
        }
      case REPLICATION_BYTES_IN_PER_SEC:
        if (topic != null) {
          return new TopicMetric(MetricType.TOPIC_REPLICATION_BYTES_IN, now, brokerId, topic, value);
        } else {
          return new BrokerMetric(MetricType.ALL_TOPIC_REPLICATION_BYTES_IN, now, brokerId, value);
        }
      case REPLICATION_BYTES_OUT_PER_SEC:
        if (topic != null) {
          return new TopicMetric(MetricType.TOPIC_REPLICATION_BYTES_OUT, now, brokerId, topic, value);
        } else {
          return new BrokerMetric(MetricType.ALL_TOPIC_REPLICATION_BYTES_OUT, now, brokerId, value);
        }
      case TOTAL_FETCH_REQUEST_PER_SEC:
        if (topic != null) {
          return new TopicMetric(MetricType.TOPIC_FETCH_REQUEST_RATE, now, brokerId, topic, value);
        } else {
          return new BrokerMetric(MetricType.ALL_TOPIC_FETCH_REQUEST_RATE, now, brokerId, value);
        }
      case TOTAL_PRODUCE_REQUEST_PER_SEC:
        if (topic != null) {
          return new TopicMetric(MetricType.TOPIC_PRODUCE_REQUEST_RATE, now, brokerId, topic, value);
        } else {
          return new BrokerMetric(MetricType.ALL_TOPIC_PRODUCE_REQUEST_RATE, now, brokerId, value);
        }
      case MESSAGES_IN_PER_SEC:
        if (topic != null) {
          return new TopicMetric(MetricType.TOPIC_MESSAGES_IN_PER_SEC, now, brokerId, topic, value);
        } else {
          return new BrokerMetric(MetricType.ALL_TOPIC_MESSAGES_IN_PER_SEC, now, brokerId, value);
        }
      case REQUEST_PER_SEC:
        String requestType = tags.get(REQUEST_TYPE_KEY);
        switch (requestType) {
          case PRODUCE_REQUEST_TYPE:
            return new BrokerMetric(MetricType.BROKER_PRODUCE_REQUEST_RATE, now, brokerId, value);
          case CONSUMER_FETCH_REQUEST_TYPE:
            return new BrokerMetric(MetricType.BROKER_CONSUMER_FETCH_REQUEST_RATE, now, brokerId, value);
          case FOLLOWER_FETCH_REQUEST_TYPE:
            return new BrokerMetric(MetricType.BROKER_FOLLOWER_FETCH_REQUEST_RATE, now, brokerId, value);
          default:
            return null;
        }
      case SIZE:
        int partition = Integer.parseInt(tags.get(PARTITION_KEY));
        return new PartitionMetric(MetricType.PARTITION_SIZE, now, brokerId, topic, partition, value);
      case REQUEST_HANDLER_AVG_IDLE_PERCENT:
        return new BrokerMetric(MetricType.BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT, now, brokerId, value);
      default:
        return null;
    }
  }
}
