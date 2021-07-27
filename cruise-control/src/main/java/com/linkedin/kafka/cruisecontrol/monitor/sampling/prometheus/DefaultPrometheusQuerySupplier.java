/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;

/**
 * Default prometheus query supplier. This can be used if the Prometheus JMX and node exporters on the
 * Kafka brokers are running with standard vanilla configuration, and do not have any special rules for
 * transformation of metric names.
 *
 * See {@link PrometheusQuerySupplier}
 */
public class DefaultPrometheusQuerySupplier implements PrometheusQuerySupplier {
    private static final Map<RawMetricType, String> TYPE_TO_QUERY = new HashMap<>();
    static {
        // broker metrics
        TYPE_TO_QUERY.put(BROKER_CPU_UTIL,
            "1 - avg by (instance) (irate(node_cpu_seconds_total{mode=\"idle\"}[1m]))");
        TYPE_TO_QUERY.put(ALL_TOPIC_BYTES_IN,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesInPerSec\",topic=\"\"}");
        TYPE_TO_QUERY.put(ALL_TOPIC_BYTES_OUT,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}");
        TYPE_TO_QUERY.put(ALL_TOPIC_REPLICATION_BYTES_IN,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"ReplicationBytesInPerSec\",topic=\"\"}");
        TYPE_TO_QUERY.put(ALL_TOPIC_REPLICATION_BYTES_OUT,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"ReplicationBytesOutPerSec\",topic=\"\"}");
        TYPE_TO_QUERY.put(ALL_TOPIC_FETCH_REQUEST_RATE,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"TotalFetchRequestsPerSec\",topic=\"\"}");
        TYPE_TO_QUERY.put(ALL_TOPIC_PRODUCE_REQUEST_RATE,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"TotalProduceRequestsPerSec\",topic=\"\"}");
        TYPE_TO_QUERY.put(ALL_TOPIC_MESSAGES_IN_PER_SEC,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"MessagesInPerSec\",topic=\"\"}");
        TYPE_TO_QUERY.put(BROKER_PRODUCE_REQUEST_RATE, "sum by (instance) "
                + "(kafka_network_RequestMetrics_OneMinuteRate{name=\"RequestsPerSec\",request=\"Produce\"})");
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_REQUEST_RATE, "sum by (instance) "
                + "(kafka_network_RequestMetrics_OneMinuteRate{name=\"RequestsPerSec\",request=\"FetchConsumer\"})");
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_REQUEST_RATE, "sum by (instance) "
                + "(kafka_network_RequestMetrics_OneMinuteRate{name=\"RequestsPerSec\",request=\"FetchFollower\"})");
        TYPE_TO_QUERY.put(BROKER_REQUEST_QUEUE_SIZE,
            "kafka_network_RequestChannel_Value{name=\"RequestQueueSize\"}");
        TYPE_TO_QUERY.put(BROKER_RESPONSE_QUEUE_SIZE,
            "kafka_network_RequestChannel_Value{name=\"ResponseQueueSize\"}");
        TYPE_TO_QUERY.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"RequestQueueTimeMs\",request=\"Produce\"}");
        TYPE_TO_QUERY.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"RequestQueueTimeMs\",request=\"Produce\"}");
        TYPE_TO_QUERY.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"RequestQueueTimeMs\",request=\"Produce\"}");
        TYPE_TO_QUERY.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"RequestQueueTimeMs\",request=\"Produce\"}");
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"RequestQueueTimeMs\",request=\"FetchConsumer\"}");
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"RequestQueueTimeMs\",request=\"FetchConsumer\"}");
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"RequestQueueTimeMs\",request=\"FetchConsumer\"}");
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"RequestQueueTimeMs\",request=\"FetchConsumer\"}");
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"RequestQueueTimeMs\",request=\"FetchFollower\"}");
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"RequestQueueTimeMs\",request=\"FetchFollower\"}");
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"RequestQueueTimeMs\",request=\"FetchFollower\"}");
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"RequestQueueTimeMs\",request=\"FetchFollower\"}");
        TYPE_TO_QUERY.put(BROKER_PRODUCE_LOCAL_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"LocalTimeMs\",request=\"Produce\"}");
        TYPE_TO_QUERY.put(BROKER_PRODUCE_LOCAL_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"LocalTimeMs\",request=\"Produce\"}");
        TYPE_TO_QUERY.put(BROKER_PRODUCE_LOCAL_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"LocalTimeMs\",request=\"Produce\"}");
        TYPE_TO_QUERY.put(BROKER_PRODUCE_LOCAL_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"LocalTimeMs\",request=\"Produce\"}");
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"LocalTimeMs\",request=\"FetchConsumer\"}");
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"LocalTimeMs\",request=\"FetchConsumer\"}");
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"LocalTimeMs\",request=\"FetchConsumer\"}");
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"LocalTimeMs\",request=\"FetchConsumer\"}");
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"LocalTimeMs\",request=\"FetchFollower\"}");
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"LocalTimeMs\",request=\"FetchFollower\"}");
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"LocalTimeMs\",request=\"FetchFollower\"}");
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"LocalTimeMs\",request=\"FetchFollower\"}");
        TYPE_TO_QUERY.put(BROKER_PRODUCE_TOTAL_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"TotalTimeMs\",request=\"Produce\"}");
        TYPE_TO_QUERY.put(BROKER_PRODUCE_TOTAL_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"TotalTimeMs\",request=\"Produce\"}");
        TYPE_TO_QUERY.put(BROKER_PRODUCE_TOTAL_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"TotalTimeMs\",request=\"Produce\"}");
        TYPE_TO_QUERY.put(BROKER_PRODUCE_TOTAL_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"TotalTimeMs\",request=\"Produce\"}");
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"TotalTimeMs\",request=\"FetchConsumer\"}");
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"TotalTimeMs\",request=\"FetchConsumer\"}");
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"TotalTimeMs\",request=\"FetchConsumer\"}");
        TYPE_TO_QUERY.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"TotalTimeMs\",request=\"FetchConsumer\"}");
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"TotalTimeMs\",request=\"FetchFollower\"}");
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"TotalTimeMs\",request=\"FetchFollower\"}");
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"TotalTimeMs\",request=\"FetchFollower\"}");
        TYPE_TO_QUERY.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"TotalTimeMs\",request=\"FetchFollower\"}");
        TYPE_TO_QUERY.put(BROKER_LOG_FLUSH_RATE,
            "kafka_log_LogFlushStats_OneMinuteRate{name=\"LogFlushRateAndTimeMs\"}");
        TYPE_TO_QUERY.put(BROKER_LOG_FLUSH_TIME_MS_MAX,
            "kafka_log_LogFlushStats_Max{name=\"LogFlushRateAndTimeMs\"}");
        TYPE_TO_QUERY.put(BROKER_LOG_FLUSH_TIME_MS_MEAN,
            "kafka_log_LogFlushStats_Mean{name=\"LogFlushRateAndTimeMs\"}");
        TYPE_TO_QUERY.put(BROKER_LOG_FLUSH_TIME_MS_50TH,
            "kafka_log_LogFlushStats_50thPercentile{name=\"LogFlushRateAndTimeMs\"}");
        TYPE_TO_QUERY.put(BROKER_LOG_FLUSH_TIME_MS_999TH,
            "kafka_log_LogFlushStats_999thPercentile{name=\"LogFlushRateAndTimeMs\"}");
        TYPE_TO_QUERY.put(BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT,
            "kafka_server_KafkaRequestHandlerPool_OneMinuteRate{name=\"RequestHandlerAvgIdlePercent\"}");

        // topic metrics
        TYPE_TO_QUERY.put(TOPIC_BYTES_IN,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesInPerSec\",topic!=\"\"}");
        TYPE_TO_QUERY.put(TOPIC_BYTES_OUT,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic!=\"\"}");
        TYPE_TO_QUERY.put(TOPIC_REPLICATION_BYTES_IN,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"ReplicationBytesInPerSec\",topic!=\"\"}");
        TYPE_TO_QUERY.put(TOPIC_REPLICATION_BYTES_OUT,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"ReplicationBytesOutPerSec\",topic!=\"\"}");
        TYPE_TO_QUERY.put(TOPIC_FETCH_REQUEST_RATE,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"TotalFetchRequestsPerSec\",topic!=\"\"}");
        TYPE_TO_QUERY.put(TOPIC_PRODUCE_REQUEST_RATE,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"TotalProduceRequestsPerSec\",topic!=\"\"}");
        TYPE_TO_QUERY.put(TOPIC_MESSAGES_IN_PER_SEC,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"MessagesInPerSec\",topic!=\"\"}");

        // partition metrics
        TYPE_TO_QUERY.put(PARTITION_SIZE,
            "kafka_log_Log_Value{name=\"Size\",topic!=\"\",partition!=\"\"}");
    }

    @Override
    public Map<RawMetricType, String> get() {
        return Collections.unmodifiableMap(TYPE_TO_QUERY);
    }
}
