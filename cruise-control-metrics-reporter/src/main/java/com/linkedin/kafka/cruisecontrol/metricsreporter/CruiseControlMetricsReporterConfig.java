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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CruiseControlMetricsReporterConfig extends AbstractConfig {
  private static final ConfigDef CONFIG;
  private static final Set<String> CONFIGS = new HashSet<>();
  private static final Logger LOG = LoggerFactory.getLogger(CruiseControlMetricsReporterConfig.class);
  public static final String PREFIX = "cruise.control.metrics.reporter.";
  // Configurations
  public static final String CRUISE_CONTROL_METRICS_TOPIC_CONFIG = "cruise.control.metrics.topic";
  private static final String CRUISE_CONTROL_METRICS_TOPIC_DOC = "The topic to which Cruise Control metrics reporter "
      + "should send messages";
  public static final String CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG = "cruise.control.metrics.topic.auto.create";
  private static final String CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_DOC = "Cruise Control metrics reporter will enforce " +
      " the creation of the topic at launch";
  public static final String CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG = "cruise.control.metrics.topic.num.partitions";
  private static final String CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_DOC = "The number of partitions of Cruise Control metrics topic";
  public static final String CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG = "cruise.control.metrics.topic.replication.factor";
  private static final String CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor of Cruise Control metrics topic";
  public static final String CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG = PREFIX + "metrics.reporting.interval.ms";
  private static final String CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_DOC = "The interval in milliseconds the "
      + "metrics reporter should report the metrics.";
  public static final String CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS_CONFIG = PREFIX + "linger.ms";
  private static final String CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS_DOC = "The linger.ms configuration of KafkaProducer used in Cruise "
      + "Control metrics reporter. Set this config and cruise.control.metrics.reporter.batch.size to a large number to have better batching.";
  public static final String CRUISE_CONTROL_METRICS_REPORTER_MAX_BLOCK_MS_CONFIG = PREFIX + "max.block.ms";
  private static final String CRUISE_CONTROL_METRICS_REPORTER_MAX_BLOCK_MS_DOC = "The max.block.ms configuration of KafkaProducer used in Cruise "
      + "Control metrics reporter.";
  public static final String CRUISE_CONTROL_METRICS_REPORTER_BATCH_SIZE_CONFIG = PREFIX + "batch.size";
  private static final String CRUISE_CONTROL_METRICS_REPORTER_BATCH_SIZE_DOC = "The batch.size configuration of KafkaProducer used in Cruise "
      + "Control metrics reporter. Set this config and cruise.control.metrics.reporter.linger.ms to a large number to have better batching.";
  // Default values
  public static final String DEFAULT_CRUISE_CONTROL_METRICS_TOPIC = "__CruiseControlMetrics";
  private static final Integer DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS = -1;
  private static final boolean DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE = false;
  private static final Short DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR = -1;
  private static final long DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS = 60000;
  private static final String PRODUCER_ID = "CruiseControlMetricsReporter";
  private static final int DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS = 30 * 1000;
  private static final int DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_MAX_BLOCK_MS = 60 * 1000;
  private static final int DEFAULT_CRUISE_CONTROL_METRICS_BATCH_SIZE = 800 * 1000;

  public CruiseControlMetricsReporterConfig(Map<?, ?> originals, boolean doLog) {
    super(CONFIG, originals, doLog);
  }

  static {
    ProducerConfig.configNames().forEach(name -> CONFIGS.add(PREFIX + name));
    CONFIG = new ConfigDef()
        .define(PREFIX + CommonClientConfigs.CLIENT_ID_CONFIG,
                ConfigDef.Type.STRING,
                PRODUCER_ID,
                ConfigDef.Importance.LOW,
                "The producer id for Cruise Control metrics reporter")
        .define(CRUISE_CONTROL_METRICS_TOPIC_CONFIG,
                ConfigDef.Type.STRING,
                DEFAULT_CRUISE_CONTROL_METRICS_TOPIC,
                ConfigDef.Importance.HIGH,
                CRUISE_CONTROL_METRICS_TOPIC_DOC)
        .define(CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS,
                ConfigDef.Importance.HIGH,
                CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_DOC)
        .define(CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG,
                ConfigDef.Type.BOOLEAN,
                DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE,
                ConfigDef.Importance.LOW,
                CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_DOC)
        .define(CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS,
                ConfigDef.Importance.LOW,
                CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_DOC)
        .define(CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG,
                ConfigDef.Type.SHORT,
                DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR,
                ConfigDef.Importance.LOW,
                CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_DOC)
        .define(CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS,
                ConfigDef.Importance.LOW,
                CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS_DOC)
        .define(CRUISE_CONTROL_METRICS_REPORTER_MAX_BLOCK_MS_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_MAX_BLOCK_MS,
                ConfigDef.Importance.LOW,
                CRUISE_CONTROL_METRICS_REPORTER_MAX_BLOCK_MS_DOC)
        .define(CRUISE_CONTROL_METRICS_REPORTER_BATCH_SIZE_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_CRUISE_CONTROL_METRICS_BATCH_SIZE,
                ConfigDef.Importance.LOW,
                CRUISE_CONTROL_METRICS_REPORTER_BATCH_SIZE_DOC);
  }

  /**
   * @param baseConfigName Base config name.
   * @return Cruise Control metrics reporter config name.
   */
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
