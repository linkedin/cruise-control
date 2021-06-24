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
  public static final double DEFAULT_METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD = 95.0;
  public static final String METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_DOC =
      "The upper percentile threshold for the metric anomaly detector to identify an increase in the metric "
      + "values of a broker as a metric anomaly. The current metric value is compared against the historical value "
      + "corresponding to given percentile in the metric history after the application of the upper margin.";

  /**
   * <code>metric.anomaly.percentile.lower.threshold</code>
   */
  public static final String METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_CONFIG =
      "metric.anomaly.percentile.lower.threshold";
  public static final double DEFAULT_METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD = 2.0;
  public static final String METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_DOC =
      "The lower percentile threshold for the metric anomaly detector to identify a decrease in the metric "
      + "values of a broker as a metric anomaly. The current metric value is compared against the historical value "
      + "corresponding to given percentile in the metric history after the application of the lower margin.";

  /**
   * <code>metric.anomaly.upper.margin</code>
   */
  public static final String METRIC_ANOMALY_UPPER_MARGIN_CONFIG = "metric.anomaly.upper.margin";
  public static final double DEFAULT_METRIC_ANOMALY_UPPER_MARGIN = 0.5;
  public static final String METRIC_ANOMALY_UPPER_MARGIN_DOC =
      "The upper margin of metric anomaly sets the minimum ratio that the current metric value should be greater than "
      + "the historical metric value determined via percentile upper threshold in order for the metric anomaly detector "
      + "to identify a metric anomaly. For example, if the historical metric value determined based on the percentile "
      + "upper threshold is 10, the current metric value is 12, and the upper margin is 0.5, then metric anomaly "
      + "detector will not consider the current metric value as an anomaly because 12 < (10 * (1 + 0.5)).";

  /**
   * <code>metric.anomaly.lower.margin</code>
   */
  public static final String METRIC_ANOMALY_LOWER_MARGIN_CONFIG = "metric.anomaly.lower.margin";
  public static final double DEFAULT_METRIC_ANOMALY_LOWER_MARGIN = 0.2;
  public static final String METRIC_ANOMALY_LOWER_MARGIN_DOC =
      "The lower margin of metric anomaly sets the minimum ratio that the current metric value should be smaller than "
      + "the historical metric value determined via percentile lower threshold in order for the metric anomaly detector "
      + "to identify a metric anomaly. For example, if the historical metric value determined based on the percentile "
      + "lower threshold is 5, the current metric value is 2, and the lower margin is 0.2, then metric anomaly "
      + "detector will not consider the current metric value as an anomaly because 2 > (5 * 0.2).";

  private static final ConfigDef CONFIG =
      new ConfigDef().define(METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_CONFIG,
                             ConfigDef.Type.DOUBLE,
                             DEFAULT_METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD,
                             between(0.01, 99.99),
                             ConfigDef.Importance.MEDIUM,
                             METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_DOC)
                     .define(METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_CONFIG,
                             ConfigDef.Type.DOUBLE,
                             DEFAULT_METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD,
                             between(0.01, 99.99),
                             ConfigDef.Importance.MEDIUM,
                             METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_DOC)
                     .define(METRIC_ANOMALY_UPPER_MARGIN_CONFIG,
                             ConfigDef.Type.DOUBLE,
                             DEFAULT_METRIC_ANOMALY_UPPER_MARGIN,
                             between(0.0, 10.0),
                             ConfigDef.Importance.MEDIUM,
                             METRIC_ANOMALY_UPPER_MARGIN_DOC)
                     .define(METRIC_ANOMALY_LOWER_MARGIN_CONFIG,
                             ConfigDef.Type.DOUBLE,
                             DEFAULT_METRIC_ANOMALY_LOWER_MARGIN,
                             between(0.0, 1.00),
                             ConfigDef.Importance.MEDIUM,
                             METRIC_ANOMALY_LOWER_MARGIN_DOC);

  PercentileMetricAnomalyFinderConfig(Map<?, ?> originals) {
    super(CONFIG, originals);
    sanityCheckPercentile();
  }

  /**
   * Sanity check to ensure that
   * <ul>
   *   <li>{@link #METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_CONFIG} is within (0.0, 100.0)}</li>
   *   <li>{@link #METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_CONFIG} is within (0.0, 100.0)}</li>
   *   <li>{@link #METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_CONFIG} >= {@link #METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_CONFIG}</li>
   * </ul>
   */
  private void sanityCheckPercentile() {
    double upperPercentile = getDouble(METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_CONFIG);
    double lowerPercentile = getDouble(METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_CONFIG);
    if (upperPercentile >= 100.0 || upperPercentile <= 0.0) {
      throw new IllegalArgumentException(String.format("Upper percentile (%f) is invalid, it should be within (0.0, 100.0).",
                                                       upperPercentile));
    }
    if (lowerPercentile >= 100.0 || lowerPercentile <= 0.0) {
      throw new IllegalArgumentException(String.format("Lower percentile (%f) is invalid, it should be within (0.0, 100.0).",
                                                       lowerPercentile));
    }

    if (lowerPercentile > upperPercentile) {
      throw new IllegalArgumentException(String.format("Lower percentile (%f) is larger than upper percentile (%f).",
                                                       lowerPercentile, upperPercentile));
    }
  }
}
