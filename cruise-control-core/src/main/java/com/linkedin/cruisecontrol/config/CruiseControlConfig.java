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
   * <code>window.ms</code>
   */
  public static final String WINDOW_MS_CONFIG = "window.ms";
  private static final String WINDOW_MS_DOC = "The interval in millisecond that is covered by each window." +
      "Cruise control will aggregate all the metric samples whose timestamp fall into the same window. " +
      "The window.ms must be greater than the metric.sampling.interval.ms.";

  /**
   * <code>num.windows</code>
   */
  public static final String NUM_WINDOWS_CONFIG = "num.windows";
  private static final String NUM_WINDOWS_DOC = "The maximum number of windows the load monitor would keep. " +
      "Each window covers a time span defined by window.ms.";

  /**
   * <code>min.samples.per.window</code>
   */
  public static final String MIN_SAMPLES_PER_WINDOW_CONFIG = "min.samples.per.window";
  private static final String MIN_SAMPLES_PER_WINDOW_DOC = "The minimum number of metric samples a valid " +
      "window should have. If a partition does not have enough samples in a window, the entire group of the "
      + "entity will be removed from the aggregated metrics result due to insufficient data.";

  /**
   * <code>max.allowed.extrapolations.per.entity</code>
   */
  public static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_ENTITY_CONFIG = "max.allowed.extrapolations.per.entity";
  private static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_ENTITY_DOC = "The maximum allowed number of extrapolations for "
      + "each entity. An entity will be considered as invalid if the total number extrapolations in all the windows goes "
      + "above this number.";

  static {
    CONFIG = new ConfigDef()
        .define(WINDOW_MS_CONFIG,
                ConfigDef.Type.LONG,
                60 * 60 * 1000,
                atLeast(1),
                ConfigDef.Importance.HIGH, WINDOW_MS_DOC)
        .define(NUM_WINDOWS_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(1),
                ConfigDef.Importance.HIGH, NUM_WINDOWS_DOC)
        .define(MIN_SAMPLES_PER_WINDOW_CONFIG,
                ConfigDef.Type.INT,
                3,
                atLeast(1),
                ConfigDef.Importance.LOW, MIN_SAMPLES_PER_WINDOW_DOC)
        .define(MAX_ALLOWED_EXTRAPOLATIONS_PER_ENTITY_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                MAX_ALLOWED_EXTRAPOLATIONS_PER_ENTITY_DOC);
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
