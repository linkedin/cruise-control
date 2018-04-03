/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerEntity;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomalyAnalyzer.AnomalyTimeFrame.LATEST_WINDOW;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;


/**
 * Identifies whether there are metric anomalies in brokers for the selected metric ids.
 */
public class KafkaMetricAnomalyAnalyzer implements MetricAnomalyAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricAnomalyAnalyzer.class);
  // Anomaly is detected if either of the two is satisfied for the current metric value (CMV):
  // 1) CMV > METRIC_ANOMALY_PERCENTILE_THRESHOLD, or
  // 2) CMV < (1 - METRIC_ANOMALY_PERCENTILE_THRESHOLD).
  private static final Double METRIC_ANOMALY_PERCENTILE_THRESHOLD = 95.0;
  private static final Set<Integer> INTERESTED_METRICS_FOR_METRIC_ANALYZER =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList((int) BROKER_PRODUCE_LOCAL_TIME_MS_MAX.id(),
                                                              (int) BROKER_PRODUCE_LOCAL_TIME_MS_MEAN.id(),
                                                              (int) BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX.id(),
                                                              (int) BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN.id(),
                                                              (int) BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX.id(),
                                                              (int) BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN.id(),
                                                              (int) BROKER_LOG_FLUSH_TIME_MS_MAX.id(),
                                                              (int) BROKER_LOG_FLUSH_TIME_MS_MEAN.id())));
  private final Percentile _percentile;

  public KafkaMetricAnomalyAnalyzer() {
    _percentile = new Percentile(METRIC_ANOMALY_PERCENTILE_THRESHOLD);
  }

  private String anomalyDescription(Integer metricId,
                                    AggregatedMetricValues history,
                                    AggregatedMetricValues current,
                                    AnomalyTimeFrame anomalyTimeFrame) {

    _percentile.setData(history.valuesFor(metricId).doubleArray());
    double currentMetricValue = current.valuesFor(metricId).latest();

    String description = null;
    double upperThreshold = _percentile.evaluate(METRIC_ANOMALY_PERCENTILE_THRESHOLD);
    double lowerThreshold = _percentile.evaluate(100.0 - METRIC_ANOMALY_PERCENTILE_THRESHOLD);
    if (currentMetricValue > upperThreshold) {
      // More than the upper threshold (metric value too large anomaly)
      description = String.format("Metric value(%.3f) in %s time frame is over the threshold(%.1f percentile %.3f).",
                                  currentMetricValue, anomalyTimeFrame, METRIC_ANOMALY_PERCENTILE_THRESHOLD, upperThreshold);
    } else if (currentMetricValue < lowerThreshold) {
      // Less than the lower threshold (metric value too small anomaly)
      description = String.format("Metric value(%.3f) in %s time frame is under the threshold(%.1f percentile %.3f).",
                                  currentMetricValue, anomalyTimeFrame, METRIC_ANOMALY_PERCENTILE_THRESHOLD, lowerThreshold);
    }

    return description;
  }

  /**
   * Check whether the given metricId is in the interested metrics for metric anomaly analysis.
   *
   * @param metricId The metric id to be checked for being interested or not.
   * @return True if the given metricId is in the interested metrics, false otherwise.
   */
  private boolean isInterested(Integer metricId) {
    return INTERESTED_METRICS_FOR_METRIC_ANALYZER.contains(metricId);
  }

  /**
   * Get a collection of metric anomalies for brokers if an anomaly in their current aggregated metrics values is
   * detected for their metric ids, based on their history.
   *
   * @param metricsHistoryByBroker Metrics history by broker entity.
   * @param currentMetricsByBroker Current metrics by broker entity.
   * @return A collection of metric anomalies for brokers if an anomaly in their current aggregated metrics values is
   * detected for their metric ids, based on their history.
   */
  @Override
  public Collection<MetricAnomaly> metricAnomalies(Map<BrokerEntity, ValuesAndExtrapolations> metricsHistoryByBroker,
                                                   Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker) {
    long endTime = System.currentTimeMillis();
    Collection<MetricAnomaly> metricAnomalies = new HashSet<>();

    for (Map.Entry<BrokerEntity, ValuesAndExtrapolations> entry : currentMetricsByBroker.entrySet()) {
      BrokerEntity brokerEntity = entry.getKey();
      AggregatedMetricValues history = metricsHistoryByBroker.get(brokerEntity).metricValues();
      AggregatedMetricValues current = currentMetricsByBroker.get(brokerEntity).metricValues();

      List<Long> windows = entry.getValue().windows();
      long startTime = windows.get(windows.size() - 1);

      for (Integer metricId : entry.getValue().metricValues().metricIds()) {
        if (!isInterested(metricId)) {
          continue;
        }
        // TODO: Add anomaly detection in time frames other than the LATEST_WINDOW.
        String anomalyDescription = anomalyDescription(metricId, history, current, LATEST_WINDOW);
        LOG.trace("Anomaly for metric id {} for broker {} in time frame {}: {}.",
                  metricId, brokerEntity, LATEST_WINDOW, anomalyDescription == null ? "none" : anomalyDescription);
        if (anomalyDescription != null) {
          MetricAnomaly metricAnomaly = new MetricAnomaly(startTime, endTime, anomalyDescription, brokerEntity, metricId);
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
