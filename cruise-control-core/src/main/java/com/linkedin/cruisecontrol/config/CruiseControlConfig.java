/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.config;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;


/**
 * The configuration for Cruise Control.
 */
public class CruiseControlConfig extends AbstractConfig {
  private static final ConfigDef CONFIG;

  /**
   * <code>load.snapshot.interval.ms</code>
   */
  public static final String LOAD_SNAPSHOT_WINDOW_MS_CONFIG = "load.snapshot.window.ms";
  private static final String LOAD_SNAPSHOT_WINDOW_MS_DOC = "The interval in millisecond that is covered by each " +
      "load snapshot. The load snapshot will aggregate all the metric samples whose timestamp fall into its window. " +
      "The load snapshot window must be greater than the metric.sampling.interval.ms";

  /**
   * <code>num.load.snapshots</code>
   */
  public static final String NUM_LOAD_SNAPSHOTS_CONFIG = "num.load.snapshots";
  private static final String NUM_LOAD_SNAPSHOTS_DOC = "The maximum number of load snapshots the load monitor would keep. " +
      "Each snapshot covers a time window defined by load.snapshot.window.ms";

  /**
   * <code>min.samples.per.load.snapshot</code>
   */
  public static final String MIN_SAMPLES_PER_LOAD_SNAPSHOT_CONFIG = "min.samples.per.load.snapshot";
  private static final String MIN_SAMPLES_PER_LOAD_SNAPSHOT_DOC = "The minimum number of metric samples a valid load " +
      "snapshot should have. If a partition does not have enough samples in a snapshot window, the topic of the partition " +
      "will be removed from the snapshot due to in sufficient data.";

  public static final String MAX_ALLOWED_IMPUTATIONS_PER_ENTITY_CONFIG = "max.allowed.imputations.per.entity";
  private static final String MAX_ALLOWED_IMPUTATIONS_PER_ENTITY_DOC = "The maximum allowed number of imputations for "
      + "each entity. An entity will be considered as invalid if the total number imputations in all the windows goes "
      + "above this number.";

  static {
    CONFIG = new ConfigDef()
        .define(LOAD_SNAPSHOT_WINDOW_MS_CONFIG,
                ConfigDef.Type.LONG,
                60 * 60 * 1000,
                atLeast(0),
                ConfigDef.Importance.HIGH,
                LOAD_SNAPSHOT_WINDOW_MS_DOC)
        .define(NUM_LOAD_SNAPSHOTS_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                NUM_LOAD_SNAPSHOTS_DOC)
        .define(MIN_SAMPLES_PER_LOAD_SNAPSHOT_CONFIG,
                ConfigDef.Type.INT,
                3,
                atLeast(1),
                ConfigDef.Importance.LOW,
                MIN_SAMPLES_PER_LOAD_SNAPSHOT_DOC)
        .define(MAX_ALLOWED_IMPUTATIONS_PER_ENTITY_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                MAX_ALLOWED_IMPUTATIONS_PER_ENTITY_DOC);
  }

  private static ConfigDef mergeConfigDef(ConfigDef definition) {
    for (ConfigDef.ConfigKey configKey : definition.configKeys().values()) {
      if (!CONFIG.configKeys().containsKey(configKey.name)) {
        CONFIG.define(configKey.name, configKey.type, configKey.defaultValue, configKey.validator, configKey.importance,
                      configKey.documentation, configKey.group, configKey.orderInGroup, configKey.width,
                      configKey.displayName, configKey.dependents, configKey.recommender);
      }
    }
    return CONFIG;
  }

  public CruiseControlConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
    super(mergeConfigDef(definition), originals, doLog);
  }

  public CruiseControlConfig(ConfigDef definition, Map<?, ?> originals) {
    super(mergeConfigDef(definition), originals);
  }

  public CruiseControlConfig(Map<String, Object> parsedConfig) {
    super(parsedConfig);
  }
}
