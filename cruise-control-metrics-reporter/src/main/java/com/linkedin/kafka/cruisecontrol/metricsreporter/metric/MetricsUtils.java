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
  private static final String REQUESTS_PER_SEC = "RequestsPerSec";
  private static final String REQUEST_QUEUE_SIZE = "RequestQueueSize";
  private static final String RESPONSE_QUEUE_SIZE = "ResponseQueueSize";
  private static final String REQUEST_QUEUE_TIME_MS = "RequestQueueTimeMs";
  private static final String LOCAL_TIME_MS =   "LocalTimeMs";
  private static final String TOTAL_TIME_MS = "TotalTimeMs";
  private static final String TOTAL_FETCH_REQUEST_PER_SEC = "TotalFetchRequestsPerSec";
  private static final String TOTAL_PRODUCE_REQUEST_PER_SEC = "TotalProduceRequestsPerSec";
  private static final String MESSAGES_IN_PER_SEC = "MessagesInPerSec";
  private static final String SIZE = "Size";
  private static final String REQUEST_HANDLER_AVG_IDLE_PERCENT = "RequestHandlerAvgIdlePercent";
  private static final String LOG_FLUSH_RATE_AND_TIME_MS = "LogFlushRateAndTimeMs";
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
  private static final String LOG_FLUSH_STATS_GROUP = "LogFlushStats";
  private static final String REQUEST_METRICS_GROUP = "RequestMetrics";
  private static final String REQUEST_CHANNEL_GROUP = "RequestChannel";
  private static final String REQUEST_KAFKA_HANDLER_POOL_GROUP = "KafkaRequestHandlerPool";

  // Tag Value
  private static final String CONSUMER_FETCH_REQUEST_TYPE = "FetchConsumer";
  private static final String FOLLOWER_FETCH_REQUEST_TYPE = "FetchFollower";
  private static final String PRODUCE_REQUEST_TYPE = "Produce";

  // Attribute
  static final String ATTRIBUTE_MEAN = "Mean";
  static final String ATTRIBUTE_MAX = "Max";
  static final String ATTRIBUTE_50TH_PERCENTILE = "50thPercentile";
  static final String ATTRIBUTE_999TH_PERCENTILE = "999thPercentile";

  // Name Set.
  private static final Set<String> INTERESTED_NETWORK_METRIC_NAMES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REQUESTS_PER_SEC,
                                                              REQUEST_QUEUE_SIZE,
                                                              RESPONSE_QUEUE_SIZE,
                                                              REQUEST_QUEUE_TIME_MS,
                                                              LOCAL_TIME_MS,
                                                              TOTAL_TIME_MS)));

  private static final Set<String> INTERESTED_TOPIC_METRIC_NAMES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(BYTES_IN_PER_SEC,
                                                              BYTES_OUT_PER_SEC,
                                                              REPLICATION_BYTES_IN_PER_SEC,
                                                              REPLICATION_BYTES_OUT_PER_SEC,
                                                              TOTAL_FETCH_REQUEST_PER_SEC,
                                                              TOTAL_PRODUCE_REQUEST_PER_SEC,
                                                              MESSAGES_IN_PER_SEC)));
    private static final Set<String> INTERESTED_LOG_METRIC_NAMES =
        Collections.unmodifiableSet(new HashSet<>(Arrays.asList(SIZE, LOG_FLUSH_RATE_AND_TIME_MS)));

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
   * @return KafkaMetric converted as a CruiseControlMetric.
   */
  public static CruiseControlMetric toCruiseControlMetric(KafkaMetric kafkaMetric, long now, int brokerId) {
    org.apache.kafka.common.MetricName metricName = kafkaMetric.metricName();
    if (!(kafkaMetric.metricValue() instanceof Double)) {
      throw new IllegalArgumentException(String.format("Cannot convert non-double (%s) KafkaMetric %s to a Cruise Control"
                                                       + " metric for broker %d", kafkaMetric.metricValue().getClass(),
                                                       kafkaMetric.metricName(), brokerId));
    }

    CruiseControlMetric ccm = toCruiseControlMetric(now, brokerId, metricName.name(), metricName.tags(), (double) kafkaMetric.metricValue());
    if (ccm == null) {
      throw new IllegalArgumentException(String.format("Cannot convert KafkaMetric %s to a Cruise Control metric for "
                                                           + "broker %d at time %d", kafkaMetric.metricName(), brokerId, now));
    }
    return ccm;
  }

  /**
   * @return A Yammer metric coverted as a CruiseControlMetric.
   */
  public static CruiseControlMetric toCruiseControlMetric(long now,
                                                          int brokerId,
                                                          com.yammer.metrics.core.MetricName metricName,
                                                          double value) {
    return toCruiseControlMetric(now, brokerId, metricName, value, null);
  }



  /**
   * @return A Yammer metric converted as a CruiseControlMetric.
   */
  public static CruiseControlMetric toCruiseControlMetric(long now,
                                                          int brokerId,
                                                          com.yammer.metrics.core.MetricName metricName,
                                                          double value,
                                                          String attribute) {
    CruiseControlMetric ccm =
        toCruiseControlMetric(now, brokerId, metricName.getName(), yammerMetricScopeToTags(metricName.getScope()), value, attribute);
    if (ccm == null) {
      throw new IllegalArgumentException(String.format("Cannot convert yammer metric %s to a Cruise Control metric for "
                                                       + "broker %d at time %d for tag %s", metricName, brokerId, now, attribute));
    }
    return ccm;
  }

  /**
   * @return the "recent CPU usage" for the JVM process as a double in [0.0,1.0].
   */
  public static BrokerMetric getCpuMetric(long now, int brokerId) {
    double cpuUtil = ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuLoad();
    if (cpuUtil < 0) {
      throw new IllegalStateException("Java Virtual Machine recent CPU usage is not available.");
    }
    return new BrokerMetric(RawMetricType.BROKER_CPU_UTIL, now, brokerId, cpuUtil);
  }

  /**
   * @return True if a kafkaMetric is an interested metric, false otherwise.
   */
  public static boolean isInterested(org.apache.kafka.common.MetricName metricName) {
    String group = metricName.group();
    String name = metricName.name();
    String type = metricName.tags().get(TYPE_KEY);
    return isInterested(group, name, type, metricName.tags());
  }

  /**
   * @return True if the yammer metric name is an interested metric, false otherwise.
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
      return (INTERESTED_TOPIC_METRIC_NAMES.contains(name) && BROKER_TOPIC_METRICS_GROUP.equals(type)) || (
          INTERESTED_SERVER_METRIC_NAMES.contains(name) && REQUEST_KAFKA_HANDLER_POOL_GROUP.equals(type));
    } else if (group.equals(KAFKA_NETWORK) && INTERESTED_NETWORK_METRIC_NAMES.contains(name)) {
      return REQUEST_CHANNEL_GROUP.equals(type)
                    || (REQUEST_METRICS_GROUP.equals(type) && INTERESTED_REQUEST_TYPE.contains(tags.get(REQUEST_TYPE_KEY)));
    } else if (group.equals(KAFKA_LOG) && INTERESTED_LOG_METRIC_NAMES.contains(name)) {
      return LOG_GROUP.equals(type) || LOG_FLUSH_STATS_GROUP.equals(type);
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
    return toCruiseControlMetric(now, brokerId, name, tags, value, null);
  }

  /**
   * build a CruiseControlMetric object.
   */
  private static CruiseControlMetric toCruiseControlMetric(long now,
                                                           int brokerId,
                                                           String name,
                                                           Map<String, String> tags,
                                                           double value,
                                                           String attribute) {
    String topic = tags.get(TOPIC_KEY);
    switch (name) {
      case BYTES_IN_PER_SEC:
        if (topic != null) {
          return new TopicMetric(RawMetricType.TOPIC_BYTES_IN, now, brokerId, topic, value);
        } else {
          return new BrokerMetric(RawMetricType.ALL_TOPIC_BYTES_IN, now, brokerId, value);
        }
      case BYTES_OUT_PER_SEC:
        if (topic != null) {
          return new TopicMetric(RawMetricType.TOPIC_BYTES_OUT, now, brokerId, topic, value);
        } else {
          return new BrokerMetric(RawMetricType.ALL_TOPIC_BYTES_OUT, now, brokerId, value);
        }
      case REPLICATION_BYTES_IN_PER_SEC:
        if (topic != null) {
          return new TopicMetric(RawMetricType.TOPIC_REPLICATION_BYTES_IN, now, brokerId, topic, value);
        } else {
          return new BrokerMetric(RawMetricType.ALL_TOPIC_REPLICATION_BYTES_IN, now, brokerId, value);
        }
      case REPLICATION_BYTES_OUT_PER_SEC:
        if (topic != null) {
          return new TopicMetric(RawMetricType.TOPIC_REPLICATION_BYTES_OUT, now, brokerId, topic, value);
        } else {
          return new BrokerMetric(RawMetricType.ALL_TOPIC_REPLICATION_BYTES_OUT, now, brokerId, value);
        }
      case TOTAL_FETCH_REQUEST_PER_SEC:
        if (topic != null) {
          return new TopicMetric(RawMetricType.TOPIC_FETCH_REQUEST_RATE, now, brokerId, topic, value);
        } else {
          return new BrokerMetric(RawMetricType.ALL_TOPIC_FETCH_REQUEST_RATE, now, brokerId, value);
        }
      case TOTAL_PRODUCE_REQUEST_PER_SEC:
        if (topic != null) {
          return new TopicMetric(RawMetricType.TOPIC_PRODUCE_REQUEST_RATE, now, brokerId, topic, value);
        } else {
          return new BrokerMetric(RawMetricType.ALL_TOPIC_PRODUCE_REQUEST_RATE, now, brokerId, value);
        }
      case MESSAGES_IN_PER_SEC:
        if (topic != null) {
          return new TopicMetric(RawMetricType.TOPIC_MESSAGES_IN_PER_SEC, now, brokerId, topic, value);
        } else {
          return new BrokerMetric(RawMetricType.ALL_TOPIC_MESSAGES_IN_PER_SEC, now, brokerId, value);
        }
      case REQUESTS_PER_SEC:
        switch (tags.get(REQUEST_TYPE_KEY)) {
          case PRODUCE_REQUEST_TYPE:
            return new BrokerMetric(RawMetricType.BROKER_PRODUCE_REQUEST_RATE, now, brokerId, value);
          case CONSUMER_FETCH_REQUEST_TYPE:
            return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_RATE, now, brokerId, value);
          case FOLLOWER_FETCH_REQUEST_TYPE:
            return new BrokerMetric(RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_RATE, now, brokerId, value);
          default:
            return null;
        }
      case REQUEST_QUEUE_SIZE:
        return new BrokerMetric(RawMetricType.BROKER_REQUEST_QUEUE_SIZE, now, brokerId, value);
      case RESPONSE_QUEUE_SIZE:
        return new BrokerMetric(RawMetricType.BROKER_RESPONSE_QUEUE_SIZE, now, brokerId, value);
      case REQUEST_QUEUE_TIME_MS:
        switch (tags.get(REQUEST_TYPE_KEY)) {
          case PRODUCE_REQUEST_TYPE:
            switch (attribute) {
              case ATTRIBUTE_MAX:
                return new BrokerMetric(RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX, now, brokerId, value);
              case ATTRIBUTE_MEAN:
                return new BrokerMetric(RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN, now, brokerId, value);
              case ATTRIBUTE_50TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH, now, brokerId, value);
              case ATTRIBUTE_999TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH, now, brokerId, value);
              default:
                return null;
            }
          case CONSUMER_FETCH_REQUEST_TYPE:
            switch (attribute) {
              case ATTRIBUTE_MAX:
                return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX, now, brokerId, value);
              case ATTRIBUTE_MEAN:
                return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN, now, brokerId, value);
              case ATTRIBUTE_50TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH, now, brokerId, value);
              case ATTRIBUTE_999TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH, now, brokerId, value);
              default:
                return null;
            }
          case FOLLOWER_FETCH_REQUEST_TYPE:
            switch (attribute) {
              case ATTRIBUTE_MAX:
                return new BrokerMetric(RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX, now, brokerId, value);
              case ATTRIBUTE_MEAN:
                return new BrokerMetric(RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN, now, brokerId, value);
              case ATTRIBUTE_50TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH, now, brokerId, value);
              case ATTRIBUTE_999TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH, now, brokerId, value);
              default:
                return null;
            }
          default:
            return null;
        }
      case LOCAL_TIME_MS:
        switch (tags.get(REQUEST_TYPE_KEY)) {
          case PRODUCE_REQUEST_TYPE:
            switch (attribute) {
              case ATTRIBUTE_MAX:
                return new BrokerMetric(RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_MAX, now, brokerId, value);
              case ATTRIBUTE_MEAN:
                return new BrokerMetric(RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_MEAN, now, brokerId, value);
              case ATTRIBUTE_50TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_50TH, now, brokerId, value);
              case ATTRIBUTE_999TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_999TH, now, brokerId, value);
              default:
                return null;
            }
          case CONSUMER_FETCH_REQUEST_TYPE:
            switch (attribute) {
              case ATTRIBUTE_MAX:
                return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX, now, brokerId, value);
              case ATTRIBUTE_MEAN:
                return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN, now, brokerId, value);
              case ATTRIBUTE_50TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH, now, brokerId, value);
              case ATTRIBUTE_999TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH, now, brokerId, value);
              default:
                return null;
            }
          case FOLLOWER_FETCH_REQUEST_TYPE:
            switch (attribute) {
              case ATTRIBUTE_MAX:
                return new BrokerMetric(RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX, now, brokerId, value);
              case ATTRIBUTE_MEAN:
                return new BrokerMetric(RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN, now, brokerId, value);
              case ATTRIBUTE_50TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH, now, brokerId, value);
              case ATTRIBUTE_999TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH, now, brokerId, value);
              default:
                return null;
            }
          default:
            return null;
        }
      case TOTAL_TIME_MS:
        switch (tags.get(REQUEST_TYPE_KEY)) {
          case PRODUCE_REQUEST_TYPE:
            switch (attribute) {
              case ATTRIBUTE_MAX:
                return new BrokerMetric(RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_MAX, now, brokerId, value);
              case ATTRIBUTE_MEAN:
                return new BrokerMetric(RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_MEAN, now, brokerId, value);
              case ATTRIBUTE_50TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_50TH, now, brokerId, value);
              case ATTRIBUTE_999TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_999TH, now, brokerId, value);
              default:
                return null;
            }
          case CONSUMER_FETCH_REQUEST_TYPE:
            switch (attribute) {
              case ATTRIBUTE_MAX:
                return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX, now, brokerId, value);
              case ATTRIBUTE_MEAN:
                return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN, now, brokerId, value);
              case ATTRIBUTE_50TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH, now, brokerId, value);
              case ATTRIBUTE_999TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH, now, brokerId, value);
              default:
                return null;
            }
          case FOLLOWER_FETCH_REQUEST_TYPE:
            switch (attribute) {
              case ATTRIBUTE_MAX:
                return new BrokerMetric(RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX, now, brokerId, value);
              case ATTRIBUTE_MEAN:
                return new BrokerMetric(RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN, now, brokerId, value);
              case ATTRIBUTE_50TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH, now, brokerId, value);
              case ATTRIBUTE_999TH_PERCENTILE:
                return new BrokerMetric(RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH, now, brokerId, value);
              default:
                return null;
            }
          default:
            return null;
        }
      case SIZE:
        int partition = Integer.parseInt(tags.get(PARTITION_KEY));
        return new PartitionMetric(RawMetricType.PARTITION_SIZE, now, brokerId, topic, partition, value);
      case LOG_FLUSH_RATE_AND_TIME_MS:
        if (attribute == null) {
          return new BrokerMetric(RawMetricType.BROKER_LOG_FLUSH_RATE, now, brokerId, value);
        } else {
          switch (attribute) {
            case ATTRIBUTE_MAX:
              return new BrokerMetric(RawMetricType.BROKER_LOG_FLUSH_TIME_MS_MAX, now, brokerId, value);
            case ATTRIBUTE_MEAN:
              return new BrokerMetric(RawMetricType.BROKER_LOG_FLUSH_TIME_MS_MEAN, now, brokerId, value);
            case ATTRIBUTE_50TH_PERCENTILE:
              return new BrokerMetric(RawMetricType.BROKER_LOG_FLUSH_TIME_MS_50TH, now, brokerId, value);
            case ATTRIBUTE_999TH_PERCENTILE:
              return new BrokerMetric(RawMetricType.BROKER_LOG_FLUSH_TIME_MS_999TH, now, brokerId, value);
            default:
              return null;
          }
        }
      case REQUEST_HANDLER_AVG_IDLE_PERCENT:
        return new BrokerMetric(RawMetricType.BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT, now, brokerId, value);
      default:
        return null;
    }
  }
}
