/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.metricdefinition;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.metricdef.ValueComputingStrategy;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.cruisecontrol.metricdef.ValueComputingStrategy.AVG;
import static com.linkedin.cruisecontrol.metricdef.ValueComputingStrategy.LATEST;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaCruiseControlMetricDef.DefScope.COMMON;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaCruiseControlMetricDef.DefScope.BROKER_ONLY;


/**
 * The metric definitions of Kafka Cruise Control.
 *
 * The class maps the raw metric types to metric definitions to be used by the analyzer and linear regression
 * model (in development).
 */
public enum KafkaCruiseControlMetricDef {
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
  BROKER_REPLICATION_FETCH_REQUEST_RATE(AVG, BROKER_ONLY, false),
  BROKER_REQUEST_HANDLER_POOL_IDLE_PERCENT(AVG, BROKER_ONLY, false);

  private final ValueComputingStrategy _valueComputingStrategy;
  private final DefScope _defScope;
  private final boolean _toPredict;
  private static final Map<MetricType, KafkaCruiseControlMetricDef> TYPE_TO_DEF = new HashMap<>();
  private static final MetricDef METRIC_DEF = buildCommonMetricDef();
  private static final MetricDef BROKER_METRIC_DEF = buildBrokerMetricDef();
  private static final List<KafkaCruiseControlMetricDef> CACHED_VALUES = 
      Collections.unmodifiableList(Arrays.asList(KafkaCruiseControlMetricDef.values()));
  private static final List<KafkaCruiseControlMetricDef> CACHED_COMMON_DEF_VALUES = buildCachedCommonDefValues();
  private static final List<KafkaCruiseControlMetricDef> CACHED_BROKER_DEF_VALUES = CACHED_VALUES;

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
  }

  KafkaCruiseControlMetricDef(ValueComputingStrategy strategy, DefScope defScope, boolean toPredict) {
    _valueComputingStrategy = strategy;
    _defScope = defScope;
    _toPredict = toPredict;
  }

  public ValueComputingStrategy valueComputingStrategy() {
    return _valueComputingStrategy;
  }

  public static List<KafkaCruiseControlMetricDef> cachedCommonDefValues() {
    return CACHED_COMMON_DEF_VALUES;
  }

  public static KafkaCruiseControlMetricDef forMetricType(MetricType type) {
    return TYPE_TO_DEF.get(type);
  }

  public static MetricDef commonMetricDef() {
    return METRIC_DEF;
  }
  
  public static MetricDef brokerMetricDef() {
    return BROKER_METRIC_DEF;
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
  
  public enum DefScope {
    BROKER_ONLY, COMMON
  }
  
  private static List<KafkaCruiseControlMetricDef> buildCachedCommonDefValues() {
    List<KafkaCruiseControlMetricDef> commonDefs = new ArrayList<>();
    for (KafkaCruiseControlMetricDef def : CACHED_VALUES) {
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
    for (KafkaCruiseControlMetricDef def : CACHED_VALUES) {
      if (def._defScope == COMMON) {
        // Add the common metrics definitions to the common metric def.
        metricDef.define(def.name(), def.valueComputingStrategy().name(), def._toPredict);
      }
    }
    return metricDef;
  }
  
  private static MetricDef buildBrokerMetricDef() {
    MetricDef metricDef = new MetricDef();
    for (KafkaCruiseControlMetricDef def : CACHED_VALUES) {
      // Add the all metrics definitions to the broker metric def.
      metricDef.define(def.name(), def.valueComputingStrategy().name(), def._toPredict);
    }
    return metricDef;
  }
}
