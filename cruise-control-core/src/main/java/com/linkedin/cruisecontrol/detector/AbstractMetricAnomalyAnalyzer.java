/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector;

import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cruisecontrol.detector.AbstractMetricAnomalyAnalyzer.AnomalyTimeFrame.LATEST_WINDOW;


public abstract class AbstractMetricAnomalyAnalyzer<M extends Entity, N extends MetricAnomaly> implements MetricAnomalyAnalyzer<M, N> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMetricAnomalyAnalyzer.class);
  private final Percentile _percentile;

  public AbstractMetricAnomalyAnalyzer() {
    _percentile = new Percentile();
  }

  /**
   * Anomaly description is populated if either of the two is satisfied for the current metric value (CMV):
   * 1) CMV > anomalyPercentileThreshold, or
   * 2) CMV < (1 - anomalyPercentileThreshold).
   *
   * @param metricId Metric id for anomaly detection.
   * @param history Historical changes in the metric values
   * @param current Current metric values
   * @param anomalyTimeFrame The time frame for which the anomaly detection is requested
   * @param anomalyPercentileThreshold Percentile threshold for identifying a current value as causing metric anomaly.
   * @return Anomaly description if an anomaly is detected, null otherwise.
   */
  private String anomalyDescription(Integer metricId,
                                    ValuesAndExtrapolations history,
                                    ValuesAndExtrapolations current,
                                    AnomalyTimeFrame anomalyTimeFrame,
                                    double anomalyPercentileThreshold) {
    _percentile.setData(history.metricValues().valuesFor(metricId).doubleArray());
    double currentMetricValue = current.metricValues().valuesFor(metricId).latest();
    double upperThreshold = _percentile.evaluate(anomalyPercentileThreshold);
    double lowerThreshold = _percentile.evaluate(100.0 - anomalyPercentileThreshold);

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
                             currentMetricValue, anomalyTimeFrame, anomalyPercentileThreshold,
                             upperThreshold, historyStart, historyEnd);
      case -1:
        // Less than the lower threshold (metric value too small anomaly).
        return String.format("Metric value(%.3f) in %s time frame is under the threshold(%.1f percentile %.3f). "
                             + "History(%d to %d).",
                             currentMetricValue, anomalyTimeFrame, 100.0 - anomalyPercentileThreshold,
                             lowerThreshold, historyStart, historyEnd);
      default:
        // No anomaly.
        return null;
    }
  }

  public abstract N createMetricAnomaly(long startTime, long endTime, String description, M entity, Integer metricId);

  /**
   * Get a collection of metric anomalies for brokers if an anomaly in their current aggregated metrics values is
   * detected for their metric ids, based on their history.
   *
   * @param anomalyPercentileThreshold Metric anomaly percentile threshold.
   * @param metricsHistoryByBroker Metrics history by broker entity.
   * @param currentMetricsByBroker Current metrics by broker entity.
   * @return A collection of metric anomalies for brokers if an anomaly in their current aggregated metrics values is
   * detected for their metric ids, based on their history.
   */
  @Override
  public Collection<N> metricAnomalies(double anomalyPercentileThreshold,
                                       Map<M, ValuesAndExtrapolations> metricsHistoryByBroker,
                                       Map<M, ValuesAndExtrapolations> currentMetricsByBroker) {
    long endTime = System.currentTimeMillis();
    Collection<N> metricAnomalies = new HashSet<>();

    for (Map.Entry<M, ValuesAndExtrapolations> entry : currentMetricsByBroker.entrySet()) {
      M entity = entry.getKey();
      ValuesAndExtrapolations history = metricsHistoryByBroker.get(entity);
      ValuesAndExtrapolations current = currentMetricsByBroker.get(entity);

      List<Long> windows = entry.getValue().windows();
      long startTime = windows.get(windows.size() - 1);

      for (Integer metricId : entry.getValue().metricValues().metricIds()) {
        // TODO: Add anomaly detection in time frames other than the LATEST_WINDOW.
        String anomalyDescription = anomalyDescription(metricId, history, current, LATEST_WINDOW, anomalyPercentileThreshold);
        LOG.trace("Anomaly for metric id {} for entity {} in time frame {}: {}.",
                  metricId, entity, LATEST_WINDOW, anomalyDescription == null ? "none" : anomalyDescription);
        if (anomalyDescription != null) {
          N metricAnomaly = createMetricAnomaly(startTime, endTime, anomalyDescription, entity, metricId);
          metricAnomalies.add(metricAnomaly);
        }
      }
    }

    return metricAnomalies;
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