/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;


public class CruiseControlMetricsReporterConfig extends AbstractConfig {
  private static final ConfigDef CONFIG;
  private static final Set<String> CONFIGS = new HashSet<>();
  public static final String PREFIX = "cruise.control.metrics.reporter.";
  // Two unique configurations
  public static final String CRUISE_CONTROL_METRICS_TOPIC_CONFIG = "cruise.control.metrics.topic";
  private static final String CRUISE_CONTROL_METRICS_TOPIC_DOC = "The topic to which cruise control metrics reporter "
      + "should send messages";
  public static final String CRUISE_CONTROL_METRICS_REPORTING_INTERVAL_MS_CONFIG = PREFIX + "metrics.reporting.interval.ms";
  private static final String CRUISE_CONTROL_METRICS_REPORTING_INTERVAL_MS_DOC = "The interval in milliseconds the "
      + "metrics reporter should report the metrics.";
  // Default values
  public static final String DEFAULT_CRUISE_CONTROL_METRICS_TOPIC = "__CruiseControlMetrics";
  private static final long DEFAULT_CRUISE_CONTROL_METRICS_REPORTING_INTERVAL_MS = 60000;
  private static final String PRODUCER_ID = "CruiseControlMetricsReporter";

  public CruiseControlMetricsReporterConfig(Map<?, ?> originals, boolean doLog) {
    super(CONFIG, originals, doLog);
  }

  static {
    ProducerConfig.configNames().forEach(name -> CONFIGS.add(PREFIX + name));
    CONFIG = new ConfigDef().define(PREFIX + CommonClientConfigs.CLIENT_ID_CONFIG,
                                    ConfigDef.Type.STRING,
                                    PRODUCER_ID,
                                    ConfigDef.Importance.LOW,
                                    "The producer id for cruise control metrics reporter")
                            .define(CRUISE_CONTROL_METRICS_TOPIC_CONFIG,
                                    ConfigDef.Type.STRING,
                                    DEFAULT_CRUISE_CONTROL_METRICS_TOPIC,
                                    ConfigDef.Importance.HIGH,
                                    CRUISE_CONTROL_METRICS_TOPIC_DOC)
                            .define(CRUISE_CONTROL_METRICS_REPORTING_INTERVAL_MS_CONFIG,
                                    ConfigDef.Type.LONG,
                                    DEFAULT_CRUISE_CONTROL_METRICS_REPORTING_INTERVAL_MS,
                                    ConfigDef.Importance.HIGH,
                                    CRUISE_CONTROL_METRICS_REPORTING_INTERVAL_MS_DOC);
  }

  public static String config(String baseConfigName) {
    String configName = PREFIX + baseConfigName;
    if (!CONFIGS.contains(configName)) {
      throw new IllegalArgumentException("The base config name " + baseConfigName + " is not defined.");
    }
    return configName;
  }

  static Properties parseProducerConfigs(Map<String, ?> configMap) {
    Properties props = new Properties();
    for (Map.Entry<String, ?> entry : configMap.entrySet()) {
      if (entry.getKey().startsWith(PREFIX)) {
        props.put(entry.getKey().replace(PREFIX, ""), entry.getValue());
      }
    }
    return props;
  }
}
