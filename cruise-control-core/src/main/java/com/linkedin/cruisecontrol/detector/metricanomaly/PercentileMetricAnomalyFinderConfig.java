/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector.metricanomaly;

import com.linkedin.cruisecontrol.common.config.AbstractConfig;
import com.linkedin.cruisecontrol.common.config.ConfigDef;
import java.util.Map;

import static com.linkedin.cruisecontrol.common.config.ConfigDef.Range.between;


public class PercentileMetricAnomalyFinderConfig extends AbstractConfig {
  /**
   * <code>metric.anomaly.percentile.upper.threshold</code>
   */
  public static final String METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_CONFIG =
      "metric.anomaly.percentile.upper.threshold";
  private static final String METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_DOC =
      "The upper threshold for the metric anomaly detector to identify an increase in the metric "
          + "values of a broker as a metric anomaly.";

  /**
   * <code>metric.anomaly.percentile.lower.threshold</code>
   */
  public static final String METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_CONFIG =
      "metric.anomaly.percentile.lower.threshold";
  private static final String METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_DOC =
      "The lower threshold for the metric anomaly detector to identify a decrease in the metric "
          f+ "values of a broker as a metric anomaly.";

  private static ConfigDef CONFIG =
      new ConfigDef().define(METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_CONFIG, ConfigDef.Type.DOUBLE, 95.0,
                             between(0.01, 99.99), ConfigDef.Importance.MEDIUM,
                             METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_DOC)
                     .define(METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_CONFIG, ConfigDef.Type.DOUBLE, 2.0,
                             between(0.01, 99.99), ConfigDef.Importance.MEDIUM,
                             METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_DOC);

  PercentileMetricAnomalyFinderConfig(Map<?, ?> originals) {
    super(CONFIG, originals);
  }
}
