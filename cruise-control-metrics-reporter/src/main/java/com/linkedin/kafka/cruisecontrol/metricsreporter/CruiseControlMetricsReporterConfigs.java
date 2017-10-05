/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.producer.ProducerConfig;


public class CruiseControlMetricsReporterConfigs {
  private static final Set<String> CONFIGS = new HashSet<>();
  public static final String PREFIX = "cruise.control.metrics.reporter.";
  // Two unique configurations
  public static final String CRUISE_CONTROL_METRICS_TOPIC_CONFIG = "cruise.control.metrics.topic";
  public static final String CRUISE_CONTROL_METRICS_REPORTING_INTERVAL_MS_CONFIG = PREFIX + "metrics.reporting.interval.ms";
  // Default values
  public static final String DEFAULT_CRUISE_CONTROL_METRICS_TOPIC = "__CruiseControlMetrics";
  static final long DEFAULT_CRUISE_CONTROL_METRICS_REPORTING_INTERVAL_MS = 60000;
  static final String PRODUCER_ID = "CruiseControlMetricsReporter";

  static {
    ProducerConfig.configNames().forEach(name -> CONFIGS.add(PREFIX + name));
  }

  private CruiseControlMetricsReporterConfigs() {

  }

  public static String config(String baseConfigName) {
    String configName = PREFIX + baseConfigName;
    if (!CONFIGS.contains(configName)) {
      throw new IllegalArgumentException("The base config name " + baseConfigName + " is not defined.");
    }
    return configName;
  }

  static Properties parseConfigMap(Map<String, ?> configMap) {
    Properties props = new Properties();
    for (Map.Entry<String, ?> entry : configMap.entrySet()) {
      if (entry.getKey().startsWith(PREFIX)) {
        props.put(entry.getKey().replace(PREFIX, ""), entry.getValue());
      }
    }
    return props;
  }
}
