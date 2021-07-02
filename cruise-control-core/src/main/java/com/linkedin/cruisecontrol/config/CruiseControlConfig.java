/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.config;

import com.linkedin.cruisecontrol.common.config.AbstractConfig;
import com.linkedin.cruisecontrol.common.config.ConfigDef;
import java.util.Map;

import static com.linkedin.cruisecontrol.common.config.ConfigDef.Range.atLeast;


/**
 * The configuration for Cruise Control.
 */
public class CruiseControlConfig extends AbstractConfig {
  private static final ConfigDef CONFIG;

  /**
   * <code>window.ms</code>
   */
  public static final String METRICS_WINDOW_MS_CONFIG = "metrics.window.ms";
  private static final String METRICS_WINDOW_MS_DOC = "The interval in millisecond that is covered by each window."
      + " Cruise control will aggregate all the metric samples whose timestamp fall into the same window. "
      + "The metrics.window.ms must be greater than the metric.sampling.interval.ms.";

  /**
   * <code>num.windows</code>
   */
  public static final String NUM_METRICS_WINDOWS_CONFIG = "num.metrics.windows";
  private static final String NUM_METRICS_WINDOWS_DOC = "The maximum number of windows the load monitor would keep. "
      + "Each window covers a time span defined by window.ms.";

  /**
   * <code>min.samples.per.window</code>
   */
  public static final String MIN_SAMPLES_PER_METRICS_WINDOW_CONFIG = "min.samples.per.metrics.window";
  private static final String MIN_SAMPLES_PER_METRICS_WINDOW_DOC = "The minimum number of metric samples a valid "
      + "window should have. If a partition does not have enough samples in a window, the entire group of the "
      + "entity will be removed from the aggregated metrics result due to insufficient data.";

  /**
   * <code>max.allowed.extrapolations.per.entity</code>
   */
  public static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_ENTITY_CONFIG = "max.allowed.extrapolations.per.entity";
  private static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_ENTITY_DOC = "The maximum allowed number of extrapolations for "
      + "each entity. An entity will be considered as invalid if the total number extrapolations in all the windows goes "
      + "above this number.";

  /**
   * <code>metric.sample.aggregator.completeness.cache.size</code>
   */
  public static final String METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG =
      "metric.sample.aggregator.completeness.cache.size";
  private static final String METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_DOC = "The metric sample aggregator "
      + "cache the completeness metadata for fast query. This configuration configures The number of completeness "
      + "cache slot to maintain.";

  /**
   * <code>metric.anomaly.analyzer.metrics</code>
   */
  public static final String METRIC_ANOMALY_FINDER_METRICS_CONFIG = "metric.anomaly.analyzer.metrics";
  private static final String METRIC_ANOMALY_ANALYZER_METRICS_DOC = "The metric ids that the metric anomaly detector "
      + "should detect if they are violated.";

  static {
    CONFIG = new ConfigDef()
        .define(METRICS_WINDOW_MS_CONFIG,
                ConfigDef.Type.LONG,
                60 * 60 * 1000,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                METRICS_WINDOW_MS_DOC)
        .define(NUM_METRICS_WINDOWS_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                NUM_METRICS_WINDOWS_DOC)
        .define(MIN_SAMPLES_PER_METRICS_WINDOW_CONFIG,
                ConfigDef.Type.INT,
                3,
                atLeast(1),
                ConfigDef.Importance.LOW,
                MIN_SAMPLES_PER_METRICS_WINDOW_DOC)
        .define(MAX_ALLOWED_EXTRAPOLATIONS_PER_ENTITY_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                MAX_ALLOWED_EXTRAPOLATIONS_PER_ENTITY_DOC)
        .define(METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(0),
                ConfigDef.Importance.LOW,
                METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_DOC)
        .define(METRIC_ANOMALY_FINDER_METRICS_CONFIG,
                ConfigDef.Type.LIST,
                "",
                ConfigDef.Importance.MEDIUM,
                METRIC_ANOMALY_ANALYZER_METRICS_DOC);
  }

  public CruiseControlConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
    super(mergeConfigDef(definition), originals, doLog);
  }

  public CruiseControlConfig(ConfigDef definition, Map<?, ?> originals) {
    super(mergeConfigDef(definition), originals);
  }

  public CruiseControlConfig(Map<String, Object> parsedConfig) {
    super(CONFIG, parsedConfig);
  }

  private static ConfigDef mergeConfigDef(ConfigDef definition) {
    for (ConfigDef.ConfigKey configKey : definition.configKeys().values()) {
      if (!CONFIG.configKeys().containsKey(configKey.name())) {
        CONFIG.define(configKey.name(), configKey.type(), configKey.defaultValue(), configKey.validator(), configKey.importance(),
                      configKey.documentation(), configKey.group(), configKey.orderInGroup(), configKey.width(),
                      configKey.displayName(), configKey.dependents(), configKey.recommender());
      }
    }
    return CONFIG;
  }
}
