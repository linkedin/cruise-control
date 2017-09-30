/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.metricdefinition;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.metricdef.ValueComputingStrategy;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.cruisecontrol.metricdef.ValueComputingStrategy.AVG;
import static com.linkedin.cruisecontrol.metricdef.ValueComputingStrategy.LATEST;


/**
 * The metric definitions of Kafka Cruise Control.
 *
 * The class maps the raw metric types to metric definitions to be used by the analyzer and linear regression
 * model.
 */
public enum KafkaCruiseControlMetricDef {
  CPU_USAGE(AVG),
  DISK_USAGE(LATEST),
  LEADER_BYTES_IN(AVG),
  LEADER_BYTES_OUT(AVG),
  PRODUCE_RATE(AVG),
  FETCH_RATE(AVG),
  MESSAGE_IN_RATE(AVG),
  REPLICATION_BYTES_IN_RATE(AVG),
  REPLICATION_BYTES_OUT_RATE(AVG);

  private final ValueComputingStrategy _valueComputingStrategy;
  private static final Map<MetricType, KafkaCruiseControlMetricDef> TYPE_TO_DEF = new HashMap<>();
  private static final MetricDef METRIC_DEF;
  private static final List<KafkaCruiseControlMetricDef> CACHED_VALUES =
      Collections.unmodifiableList(Arrays.asList(values()));

  static {
    TYPE_TO_DEF.put(MetricType.ALL_TOPIC_BYTES_IN, LEADER_BYTES_IN);
    TYPE_TO_DEF.put(MetricType.ALL_TOPIC_BYTES_OUT, LEADER_BYTES_OUT);
    TYPE_TO_DEF.put(MetricType.ALL_TOPIC_REPLICATION_BYTES_IN, REPLICATION_BYTES_IN_RATE);
    TYPE_TO_DEF.put(MetricType.ALL_TOPIC_REPLICATION_BYTES_OUT, REPLICATION_BYTES_OUT_RATE);
    TYPE_TO_DEF.put(MetricType.ALL_TOPIC_PRODUCE_REQUEST_RATE, PRODUCE_RATE);
    TYPE_TO_DEF.put(MetricType.ALL_TOPIC_FETCH_REQUEST_RATE, FETCH_RATE);
    TYPE_TO_DEF.put(MetricType.ALL_TOPIC_MESSAGES_IN_PER_SEC, MESSAGE_IN_RATE);
    TYPE_TO_DEF.put(MetricType.TOPIC_BYTES_IN, LEADER_BYTES_IN);
    TYPE_TO_DEF.put(MetricType.TOPIC_BYTES_OUT, LEADER_BYTES_OUT);
    TYPE_TO_DEF.put(MetricType.TOPIC_REPLICATION_BYTES_IN, REPLICATION_BYTES_IN_RATE);
    TYPE_TO_DEF.put(MetricType.TOPIC_REPLICATION_BYTES_OUT, REPLICATION_BYTES_OUT_RATE);
    TYPE_TO_DEF.put(MetricType.TOPIC_PRODUCE_REQUEST_RATE, PRODUCE_RATE);
    TYPE_TO_DEF.put(MetricType.TOPIC_FETCH_REQUEST_RATE, FETCH_RATE);
    TYPE_TO_DEF.put(MetricType.TOPIC_MESSAGES_IN_PER_SEC, MESSAGE_IN_RATE);
    TYPE_TO_DEF.put(MetricType.PARTITION_SIZE, DISK_USAGE);
    TYPE_TO_DEF.put(MetricType.BROKER_CPU_UTIL, CPU_USAGE);

    METRIC_DEF = new MetricDef().define(CPU_USAGE.name(), AVG.name(), true)
                                .define(DISK_USAGE.name(), LATEST.name())
                                .define(LEADER_BYTES_IN.name(), AVG.name())
                                .define(LEADER_BYTES_OUT.name(), AVG.name())
                                .define(PRODUCE_RATE.name(), AVG.name())
                                .define(FETCH_RATE.name(), AVG.name())
                                .define(MESSAGE_IN_RATE.name(), AVG.name())
                                .define(REPLICATION_BYTES_IN_RATE.name(), AVG.name())
                                .define(REPLICATION_BYTES_OUT_RATE.name(), AVG.name());
  }

  KafkaCruiseControlMetricDef(ValueComputingStrategy strategy) {
    _valueComputingStrategy = strategy;
  }

  public ValueComputingStrategy valueComputingStrategy() {
    return _valueComputingStrategy;
  }

  public static List<KafkaCruiseControlMetricDef> cachedValues() {
    return CACHED_VALUES;
  }

  public static KafkaCruiseControlMetricDef forMetricType(MetricType type) {
    return TYPE_TO_DEF.get(type);
  }

  public static MetricDef metricDef() {
    return METRIC_DEF;
  }

  public static MetricInfo resourceToMetricInfo(Resource resource) {
    switch (resource) {
      case CPU:
        return METRIC_DEF.metricInfo(KafkaCruiseControlMetricDef.CPU_USAGE.name());
      case DISK:
        return METRIC_DEF.metricInfo(KafkaCruiseControlMetricDef.DISK_USAGE.name());
      case NW_IN:
        return METRIC_DEF.metricInfo(KafkaCruiseControlMetricDef.LEADER_BYTES_IN.name());
      case NW_OUT:
        return METRIC_DEF.metricInfo(KafkaCruiseControlMetricDef.LEADER_BYTES_OUT.name());
      default:
        throw new IllegalStateException("Should never be here");
    }
  }

  public static int resourceToMetricId(Resource resource) {
    return resourceToMetricInfo(resource).id();
  }

}
