/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector;

import com.linkedin.cruisecontrol.config.CruiseControlConfig;
import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cruisecontrol.detector.AbstractMetricAnomalyAnalyzer.AnomalyTimeFrame.LATEST_WINDOW;


/**
 * An abstract class that implements metric anomaly analyzer to identify metric anomalies based on comparing the current
 * metrics to the percentile of the historical metric values.
 *
 * @param <E> The entity-level at which the metric anomaly analysis will be performed -- e.g. broker-level.
 * @param <M> The specific anomaly corresponding to the entity-level metrics -- e.g. KafkaMetricAnomaly.
 */
public abstract class AbstractMetricAnomalyAnalyzer<E extends Entity, M extends MetricAnomaly>
    implements MetricAnomalyAnalyzer<E, M> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMetricAnomalyAnalyzer.class);
  private Double _anomalyUpperPercentile;
  private Double _anomalyLowerPercentile;
  private final Percentile _percentile;
  protected List<String> _metricAnomalyAnalyzerMetrics;

  public AbstractMetricAnomalyAnalyzer() {
    _percentile = new Percentile();
  }

  /**
   * Anomaly description is populated if either of the two is satisfied for the current metric value (CMV):
   * 1) CMV > upperPercentile, or
   * 2) CMV < lowerPercentile.
   *
   * @param metricId Metric id for anomaly detection.
   * @param history Historical changes in the metric values
   * @param current Current metric values
   * @param anomalyTimeFrame The time frame for which the anomaly detection is requested
   * @param upperPercentile Percentile threshold for identifying a current increase as causing metric anomaly.
   * @param lowerPercentile Percentile threshold for identifying a current decrease as causing metric anomaly.
   * @return Anomaly description if an anomaly is detected, null otherwise.
   */
  private String anomalyDescription(Integer metricId,
                                    ValuesAndExtrapolations history,
                                    ValuesAndExtrapolations current,
                                    AnomalyTimeFrame anomalyTimeFrame,
                                    double upperPercentile,
                                    double lowerPercentile) {
    _percentile.setData(history.metricValues().valuesFor(metricId).doubleArray());
    double currentMetricValue = current.metricValues().valuesFor(metricId).latest();
    double upperThreshold = _percentile.evaluate(upperPercentile);
    double lowerThreshold = _percentile.evaluate(lowerPercentile);

    List<Long> windows = history.windows();
    long historyStart = windows.get(windows.size() - 1);
    long historyEnd = windows.get(0);

    // 1: metric value too large, -1: metric value too small, 0: no anomaly.
    int anomalyType = currentMetricValue > upperThreshold ? 1 : currentMetricValue < lowerThreshold ? -1 : 0;
    switch (anomalyType) {
      case 1:
        // More than the upper threshold (metric value too large anomaly).
        return String.format("Metric value(%.3f) in %s time frame is over the threshold(%.1f percentile %.3f). "
                             + "History(%d to %d).",
                             currentMetricValue, anomalyTimeFrame, upperPercentile,
                             upperThreshold, historyStart, historyEnd);
      case -1:
        // Less than the lower threshold (metric value too small anomaly).
        return String.format("Metric value(%.3f) in %s time frame is under the threshold(%.1f percentile %.3f). "
                             + "History(%d to %d).",
                             currentMetricValue, anomalyTimeFrame, lowerPercentile,
                             lowerThreshold, historyStart, historyEnd);
      default:
        // No anomaly.
        return null;
    }
  }

  public abstract M createMetricAnomaly(String description, E entity, Integer metricId, List<Long> windows);

  /**
   * Get a collection of metric anomalies for entities if an anomaly in their current aggregated metrics
   * values is detected for their metric ids, based on their history.
   *
   * @param metricsHistoryByEntity Metrics history by entity.
   * @param currentMetricsByEntity Current metrics by entity.
   * @return A collection of metric anomalies for entities if an anomaly in their current aggregated metrics values is
   * detected for their metric ids, based on their history.
   */
  @Override
  public Collection<M> metricAnomalies(Map<E, ValuesAndExtrapolations> metricsHistoryByEntity,
                                       Map<E, ValuesAndExtrapolations> currentMetricsByEntity) {

    if (metricsHistoryByEntity == null || currentMetricsByEntity == null) {
      throw new IllegalArgumentException("Metrics history or current metrics cannot be null.");
    }

    Collection<M> metricAnomalies = new HashSet<>();
    for (Map.Entry<E, ValuesAndExtrapolations> entry : currentMetricsByEntity.entrySet()) {
      E entity = entry.getKey();
      ValuesAndExtrapolations history = metricsHistoryByEntity.get(entity);
      ValuesAndExtrapolations current = currentMetricsByEntity.get(entity);

      List<Long> windows = entry.getValue().windows();

      for (Integer metricId : entry.getValue().metricValues().metricIds()) {
        // TODO: Add anomaly detection in time frames other than the LATEST_WINDOW.
        String anomalyDescription = anomalyDescription(metricId, history, current, LATEST_WINDOW, _anomalyUpperPercentile, _anomalyLowerPercentile);
        LOG.trace("Anomaly for metric id {} for entity {} in time frame {}: {}.",
                  metricId, entity, LATEST_WINDOW, anomalyDescription == null ? "none" : anomalyDescription);
        if (anomalyDescription != null) {
          M metricAnomaly = createMetricAnomaly(anomalyDescription, entity, metricId, windows);
          metricAnomalies.add(metricAnomaly);
        }
      }
    }

    return metricAnomalies;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs) {
    _anomalyUpperPercentile = Double.parseDouble((String) configs.get(CruiseControlConfig.METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_CONFIG));
    _anomalyLowerPercentile = Double.parseDouble((String) configs.get(CruiseControlConfig.METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_CONFIG));

    String trimmedMetrics = ((String) configs.get(CruiseControlConfig.METRIC_ANOMALY_ANALYZER_METRICS_CONFIG)).trim();
    if (trimmedMetrics.isEmpty()) {
      _metricAnomalyAnalyzerMetrics = Collections.emptyList();
    } else {
      _metricAnomalyAnalyzerMetrics = Arrays.asList(trimmedMetrics.split("\\s*,\\s*", -1));
    }

    if (_anomalyUpperPercentile == null || _anomalyLowerPercentile == null || _metricAnomalyAnalyzerMetrics == null) {
      throw new IllegalArgumentException(
          String.format("Abstract metric anomaly analyzer configuration is missing required configs. Required configs: "
                        + "anomalyPercentile(upper %.4f lower %.4f), metricAnomalyAnalyzerMetrics %s.",
                        _anomalyUpperPercentile, _anomalyLowerPercentile, _metricAnomalyAnalyzerMetrics));
    }
  }

  /**
   * The scope of the metric anomaly detection time frame:
   *
   * LATEST_WINDOW: The latest window.
   * DAILY: The day of the latest window compared to the day(s) corresponding to the same day of the week.
   * WEEKLY: The week of the latest window compared to the previous weeks.
   * MONTHLY: The month of the latest window compared to the previous months.
   */
  public enum AnomalyTimeFrame {
    LATEST_WINDOW, DAILY, WEEKLY, MONTHLY
  }
}