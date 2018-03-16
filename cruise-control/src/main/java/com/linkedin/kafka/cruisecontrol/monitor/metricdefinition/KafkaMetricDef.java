/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.metricdefinition;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.metricdef.ValueComputingStrategy;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.cruisecontrol.metricdef.ValueComputingStrategy.AVG;
import static com.linkedin.cruisecontrol.metricdef.ValueComputingStrategy.LATEST;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.DefScope.COMMON;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.DefScope.BROKER_ONLY;


/**
 * The metric definitions of Kafka Cruise Control.
 *
 * The class maps the raw metric types to metric definitions to be used by Kafka Cruise Control, i.e. analyzer and
 * linear regression model (in development).
 *
 * The metrics in KafkaCruiseControl fall into two categories.
 * 1. Broker only metrics. e.g. request queue size.
 * 2. Common metrics share by brokers and replicas. E.g. bytes in/out rate.
 *
 * As of now, we do not have replica/partition only metrics.
 */
public enum KafkaMetricDef {
  // Ideally CPU usage should be defined as broker only, for legacy reason we are defining it as common metric.
  CPU_USAGE(AVG, COMMON, true),
  DISK_USAGE(LATEST, COMMON, false),
  LEADER_BYTES_IN(AVG, COMMON, false),
  LEADER_BYTES_OUT(AVG, COMMON, false),
  PRODUCE_RATE(AVG, COMMON, false),
  FETCH_RATE(AVG, COMMON, false),
  MESSAGE_IN_RATE(AVG, COMMON, false),
  REPLICATION_BYTES_IN_RATE(AVG, COMMON, false),
  REPLICATION_BYTES_OUT_RATE(AVG, COMMON, false),
  BROKER_PRODUCE_REQUEST_RATE(AVG, BROKER_ONLY, false),
  BROKER_CONSUMER_FETCH_REQUEST_RATE(AVG, BROKER_ONLY, false),
  BROKER_FOLLOWER_FETCH_REQUEST_RATE(AVG, BROKER_ONLY, false),
  BROKER_REQUEST_HANDLER_POOL_IDLE_PERCENT(AVG, BROKER_ONLY, false),
  BROKER_REQUEST_QUEUE_SIZE(AVG, BROKER_ONLY, false),
  BROKER_RESPONSE_QUEUE_SIZE(AVG, BROKER_ONLY, false),
  BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX(AVG, BROKER_ONLY, false),
  BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN(AVG, BROKER_ONLY, false),
  BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX(AVG, BROKER_ONLY, false),
  BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN(AVG, BROKER_ONLY, false),
  BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX(AVG, BROKER_ONLY, false),
  BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN(AVG, BROKER_ONLY, false),
  BROKER_PRODUCE_TOTAL_TIME_MS_MAX(AVG, BROKER_ONLY, false),
  BROKER_PRODUCE_TOTAL_TIME_MS_MEAN(AVG, BROKER_ONLY, false),
  BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX(AVG, BROKER_ONLY, false),
  BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN(AVG, BROKER_ONLY, false),
  BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX(AVG, BROKER_ONLY, false),
  BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN(AVG, BROKER_ONLY, false),
  BROKER_PRODUCE_LOCAL_TIME_MS_MAX(AVG, BROKER_ONLY, false),
  BROKER_PRODUCE_LOCAL_TIME_MS_MEAN(AVG, BROKER_ONLY, false),
  BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX(AVG, BROKER_ONLY, false),
  BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN(AVG, BROKER_ONLY, false),
  BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX(AVG, BROKER_ONLY, false),
  BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN(AVG, BROKER_ONLY, false),
  BROKER_LOG_FLUSH_RATE(AVG, BROKER_ONLY, false),
  BROKER_LOG_FLUSH_TIME_MS_MAX(AVG, BROKER_ONLY, false),
  BROKER_LOG_FLUSH_TIME_MS_MEAN(AVG, BROKER_ONLY, false);

  private final ValueComputingStrategy _valueComputingStrategy;
  private final DefScope _defScope;
  private final boolean _toPredict;
  private static final Map<RawMetricType, KafkaMetricDef> TYPE_TO_DEF = new HashMap<>();
  private static final MetricDef COMMON_METRIC_DEF = buildCommonMetricDef();
  private static final MetricDef BROKER_METRIC_DEF = buildBrokerMetricDef();
  private static final List<KafkaMetricDef> CACHED_VALUES =
      Collections.unmodifiableList(Arrays.asList(KafkaMetricDef.values()));
  private static final List<KafkaMetricDef> CACHED_COMMON_DEF_VALUES = buildCachedCommonDefValues();
  private static final List<KafkaMetricDef> CACHED_BROKER_DEF_VALUES = CACHED_VALUES;

  static {
    // Topic raw metrics
    TYPE_TO_DEF.put(RawMetricType.TOPIC_BYTES_IN, LEADER_BYTES_IN);
    TYPE_TO_DEF.put(RawMetricType.TOPIC_BYTES_OUT, LEADER_BYTES_OUT);
    TYPE_TO_DEF.put(RawMetricType.TOPIC_REPLICATION_BYTES_IN, REPLICATION_BYTES_IN_RATE);
    TYPE_TO_DEF.put(RawMetricType.TOPIC_REPLICATION_BYTES_OUT, REPLICATION_BYTES_OUT_RATE);
    TYPE_TO_DEF.put(RawMetricType.TOPIC_PRODUCE_REQUEST_RATE, PRODUCE_RATE);
    TYPE_TO_DEF.put(RawMetricType.TOPIC_FETCH_REQUEST_RATE, FETCH_RATE);
    TYPE_TO_DEF.put(RawMetricType.TOPIC_MESSAGES_IN_PER_SEC, MESSAGE_IN_RATE);
    // Partition raw metrics
    TYPE_TO_DEF.put(RawMetricType.PARTITION_SIZE, DISK_USAGE);
    // Broker raw metrics
    TYPE_TO_DEF.put(RawMetricType.ALL_TOPIC_BYTES_IN, LEADER_BYTES_IN);
    TYPE_TO_DEF.put(RawMetricType.ALL_TOPIC_BYTES_OUT, LEADER_BYTES_OUT);
    TYPE_TO_DEF.put(RawMetricType.ALL_TOPIC_REPLICATION_BYTES_IN, REPLICATION_BYTES_IN_RATE);
    TYPE_TO_DEF.put(RawMetricType.ALL_TOPIC_REPLICATION_BYTES_OUT, REPLICATION_BYTES_OUT_RATE);
    TYPE_TO_DEF.put(RawMetricType.ALL_TOPIC_PRODUCE_REQUEST_RATE, PRODUCE_RATE);
    TYPE_TO_DEF.put(RawMetricType.ALL_TOPIC_FETCH_REQUEST_RATE, FETCH_RATE);
    TYPE_TO_DEF.put(RawMetricType.ALL_TOPIC_MESSAGES_IN_PER_SEC, MESSAGE_IN_RATE);
    TYPE_TO_DEF.put(RawMetricType.BROKER_CPU_UTIL, CPU_USAGE);
    TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_REQUEST_RATE, BROKER_PRODUCE_REQUEST_RATE);
    TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_RATE, BROKER_CONSUMER_FETCH_REQUEST_RATE);
    TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_RATE, BROKER_FOLLOWER_FETCH_REQUEST_RATE);
    TYPE_TO_DEF.put(RawMetricType.BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT, BROKER_REQUEST_HANDLER_POOL_IDLE_PERCENT);
    TYPE_TO_DEF.put(RawMetricType.BROKER_REQUEST_QUEUE_SIZE, BROKER_REQUEST_QUEUE_SIZE);
    TYPE_TO_DEF.put(RawMetricType.BROKER_RESPONSE_QUEUE_SIZE, BROKER_RESPONSE_QUEUE_SIZE);
    TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX, BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX);
    TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN, BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN);
    TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX, BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX);
    TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN, BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN);
    TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX, BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX);
    TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN, BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN);
    TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_MAX, BROKER_PRODUCE_TOTAL_TIME_MS_MAX);
    TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_MEAN, BROKER_PRODUCE_TOTAL_TIME_MS_MEAN);
    TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX, BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX);
    TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN, BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN);
    TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX, BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX);
    TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN, BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN);
    TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_MAX, BROKER_PRODUCE_LOCAL_TIME_MS_MAX);
    TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_MEAN, BROKER_PRODUCE_LOCAL_TIME_MS_MEAN);
    TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX, BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX);
    TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN, BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN);
    TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX, BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX);
    TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN, BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN);
    TYPE_TO_DEF.put(RawMetricType.BROKER_LOG_FLUSH_RATE, BROKER_LOG_FLUSH_RATE);
    TYPE_TO_DEF.put(RawMetricType.BROKER_LOG_FLUSH_TIME_MS_MAX, BROKER_LOG_FLUSH_TIME_MS_MAX);
    TYPE_TO_DEF.put(RawMetricType.BROKER_LOG_FLUSH_TIME_MS_MEAN, BROKER_LOG_FLUSH_TIME_MS_MEAN);
  }

  /**
   * Construct the Kafka metric definition.
   * @param strategy the way to pick values among samples in a window.
   * @param defScope the scope the metric applies to. It is either BROKER_ONLY or COMMON. See {@link KafkaMetricDef}
   *                 class doc for more details.
   * @param toPredict Whether the metric is a metrics that should be predicted by the linear regression model (in
   *                  development).
   */
  KafkaMetricDef(ValueComputingStrategy strategy, DefScope defScope, boolean toPredict) {
    _valueComputingStrategy = strategy;
    _defScope = defScope;
    _toPredict = toPredict;
  }

  public ValueComputingStrategy valueComputingStrategy() {
    return _valueComputingStrategy;
  }

  public DefScope defScope() {
    return _defScope;
  }

  public static List<KafkaMetricDef> cachedCommonDefValues() {
    return CACHED_COMMON_DEF_VALUES;
  }

  public static List<KafkaMetricDef> cachedBrokerDefValues() {
    return CACHED_BROKER_DEF_VALUES;
  }

  public static KafkaMetricDef forRawMetricType(RawMetricType type) {
    return TYPE_TO_DEF.get(type);
  }

  public static MetricDef commonMetricDef() {
    return COMMON_METRIC_DEF;
  }

  public static MetricDef brokerMetricDef() {
    return BROKER_METRIC_DEF;
  }

  public static MetricInfo resourceToMetricInfo(Resource resource) {
    switch (resource) {
      case CPU:
        return COMMON_METRIC_DEF.metricInfo(KafkaMetricDef.CPU_USAGE.name());
      case DISK:
        return COMMON_METRIC_DEF.metricInfo(KafkaMetricDef.DISK_USAGE.name());
      case NW_IN:
        return COMMON_METRIC_DEF.metricInfo(KafkaMetricDef.LEADER_BYTES_IN.name());
      case NW_OUT:
        return COMMON_METRIC_DEF.metricInfo(KafkaMetricDef.LEADER_BYTES_OUT.name());
      default:
        throw new IllegalStateException("Should never be here");
    }
  }

  public static int resourceToMetricId(Resource resource) {
    return resourceToMetricInfo(resource).id();
  }

  public enum DefScope {
    BROKER_ONLY, COMMON
  }

  private static List<KafkaMetricDef> buildCachedCommonDefValues() {
    List<KafkaMetricDef> commonDefs = new ArrayList<>();
    for (KafkaMetricDef def : KafkaMetricDef.values()) {
      // All the metrics definitions are applicable to broker.
      if (def._defScope == COMMON) {
        // Add the common metrics definitions to the common metric def.
        commonDefs.add(def);
      }
    }
    return Collections.unmodifiableList(commonDefs);
  }

  private static MetricDef buildCommonMetricDef() {
    MetricDef metricDef = new MetricDef();
    for (KafkaMetricDef def : KafkaMetricDef.values()) {
      if (def._defScope == COMMON) {
        // Add the common metrics definitions to the common metric def.
        metricDef.define(def.name(), def.valueComputingStrategy().name(), def._toPredict);
      }
    }
    return metricDef;
  }

  private static MetricDef buildBrokerMetricDef() {
    MetricDef metricDef = new MetricDef();
    for (KafkaMetricDef def : KafkaMetricDef.values()) {
      // Add the all metrics definitions to the broker metric def.
      metricDef.define(def.name(), def.valueComputingStrategy().name(), def._toPredict);
    }
    return metricDef;
  }
}
