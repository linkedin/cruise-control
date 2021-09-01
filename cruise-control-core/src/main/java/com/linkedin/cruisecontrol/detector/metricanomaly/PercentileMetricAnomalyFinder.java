/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector.metricanomaly;

import com.linkedin.cruisecontrol.config.CruiseControlConfig;
import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
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

import static com.linkedin.cruisecontrol.CruiseControlUtils.utcDateFor;
import static com.linkedin.cruisecontrol.detector.metricanomaly.PercentileMetricAnomalyFinderUtils.isDataSufficient;
import static com.linkedin.cruisecontrol.detector.metricanomaly.PercentileMetricAnomalyFinderUtils.SIGNIFICANT_METRIC_VALUE_THRESHOLD;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


/**
 * An abstract class that implements metric anomaly analyzer to identify metric anomalies based on comparing the current
 * metrics to the percentile of the historical metric values.
 *
 * The class takes an upper percentile threshold and a lower percentile threshold. If a value goes beyond the
 * upper percentile threshold or falls below the lower percentile threshold, that value will be considered as
 * an anomaly.
 *
 * @param <E> The entity-level at which the metric anomaly analysis will be performed -- e.g. broker-level.
 */
public abstract class PercentileMetricAnomalyFinder<E extends Entity> implements MetricAnomalyFinder<E> {
  private static final Logger LOG = LoggerFactory.getLogger(PercentileMetricAnomalyFinder.class);
  private final Percentile _percentile;
  protected double _anomalyUpperMargin;
  protected double _anomalyLowerMargin;
  protected Double _anomalyUpperPercentile;
  protected Double _anomalyLowerPercentile;
  protected Set<String> _interestedMetrics;
  protected int _numRecentAnomalies;

  public PercentileMetricAnomalyFinder() {
    _percentile = new Percentile();
    _numRecentAnomalies = 0;
  }

  /**
   * Anomaly description is populated if either of the two is satisfied for the current metric value (CMV):
   * 1) CMV > upperPercentile, or
   * 2) CMV < lowerPercentile.
   *
   * @param entity the entity to detect anomaly.
   * @param metricId Metric id for anomaly detection.
   * @param history Historical changes in the metric values
   * @param current Current metric values
   * @return Anomaly if an anomaly is detected, null otherwise.
   */
  private MetricAnomaly<E> getAnomalyForMetric(E entity,
                                               Short metricId,
                                               ValuesAndExtrapolations history,
                                               ValuesAndExtrapolations current) {
    MetricValues historyMetricValues = history.metricValues().valuesFor(metricId);
    if (historyMetricValues == null) {
      // No history metric values exist for the given metricId.
      return null;
    }
    _percentile.setData(historyMetricValues.doubleArray());

    double upperPercentileMetricValue = _percentile.evaluate(_anomalyUpperPercentile);
    if (upperPercentileMetricValue <= SIGNIFICANT_METRIC_VALUE_THRESHOLD) {
      return null;
    }

    double upperThreshold = upperPercentileMetricValue * (1 + _anomalyUpperMargin);
    double lowerThreshold = _percentile.evaluate(_anomalyLowerPercentile) * _anomalyLowerMargin;
    double currentMetricValue = current.metricValues().valuesFor(metricId).latest();

    long currentWindow = current.window(0);

    if (currentMetricValue > upperThreshold || currentMetricValue < lowerThreshold) {
      String description = description(entity, metricId, currentWindow, history.windows(), currentMetricValue,
                                       upperThreshold, lowerThreshold);
      return createMetricAnomaly(description, entity, metricId, current.windows());
    } else {
      return null;
    }
  }

  /**
   * Create a string describing what exactly is the anomaly.
   * @param entity the entity for which the anomaly was detected.
   * @param metricId the metric id for which the anomaly was detected.
   * @param currentWindow the time windows of the current metric values.
   * @param historyWindows the time windows of the historic metric values.
   * @param currentMetricValue the current metric value.
   * @param upperThreshold the upper threshold of the normal metric value.
   * @param lowerThreshold the lower threshold of the normal metric value.
   * @return A string that describes the metric anomaly.
   */
  protected String description(E entity,
                               Short metricId,
                               Long currentWindow,
                               List<Long> historyWindows,
                               double currentMetricValue,
                               double upperThreshold,
                               double lowerThreshold) {
    int numHistoryWindows = historyWindows.size();
    return String.format("Metric value %.3f of %s for %s in window %s is out of the normal range for percentile: [%.2f, %.2f] "
                         + "(value: [%.3f, %.3f] with margins (lower: %.3f, upper: %.3f)) in %d history windows from %s to %s.",
                         currentMetricValue, toMetricName(metricId), entity, utcDateFor(currentWindow),
                         _anomalyLowerPercentile, _anomalyUpperPercentile, lowerThreshold, upperThreshold,
                         _anomalyLowerMargin, _anomalyUpperMargin, numHistoryWindows,
                         utcDateFor(historyWindows.get(0)), utcDateFor(historyWindows.get(numHistoryWindows - 1)));
  }

  /**
   * @param metricId Metric id.
   * @return the metric name from metric id.
   */
  protected abstract String toMetricName(Short metricId);

  protected abstract MetricAnomaly<E> createMetricAnomaly(String description, E entity, Short metricId, List<Long> windows);

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
  public Collection<MetricAnomaly<E>> metricAnomalies(Map<E, ValuesAndExtrapolations> metricsHistoryByEntity,
                                                      Map<E, ValuesAndExtrapolations> currentMetricsByEntity) {

    validateNotNull(metricsHistoryByEntity, "Metrics history cannot be null.");
    validateNotNull(currentMetricsByEntity, "Current metrics cannot be null.");

    if (metricsHistoryByEntity.isEmpty() || !isDataSufficient(metricsHistoryByEntity.values().iterator().next().metricValues().length(),
                                                              _anomalyUpperPercentile, _anomalyLowerPercentile)) {
      return Collections.emptySet();
    }

    Set<MetricAnomaly<E>> metricAnomalies = new HashSet<>();
    for (Map.Entry<E, ValuesAndExtrapolations> entry : currentMetricsByEntity.entrySet()) {
      E entity = entry.getKey();
      ValuesAndExtrapolations history = metricsHistoryByEntity.get(entity);
      if (history == null) {
        // No historical data exist for the given entity to identify metric anomalies.
        continue;
      }
      ValuesAndExtrapolations current = currentMetricsByEntity.get(entity);
      List<Long> windows = current.windows();

      for (Short metricId : entry.getValue().metricValues().metricIds()) {
        // Skip the metrics that are not interested.
        if (_interestedMetrics.contains(toMetricName(metricId))) {
          MetricAnomaly<E> metricAnomaly = getAnomalyForMetric(entity, metricId, history, current);
          LOG.trace("Anomaly for metric id {} for entity {} in time frame {}: {}.", metricId, entity, windows,
                    metricAnomaly == null ? "none" : metricAnomaly.description());
          if (metricAnomaly != null) {
            metricAnomalies.add(metricAnomaly);
          }
        }
      }
    }

    _numRecentAnomalies = metricAnomalies.size();
    return metricAnomalies;
  }

  @Override
  public int numAnomaliesOfType(MetricAnomalyType type) {
    // Percentile Metric Anomaly Finder can only report the recent number of metric anomalies.
    return type != MetricAnomalyType.RECENT ? 0 : _numRecentAnomalies;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    PercentileMetricAnomalyFinderConfig internalConfig = new PercentileMetricAnomalyFinderConfig(configs);
    _anomalyUpperPercentile =
        internalConfig.getDouble(PercentileMetricAnomalyFinderConfig.METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_CONFIG);
    _anomalyLowerPercentile =
        internalConfig.getDouble(PercentileMetricAnomalyFinderConfig.METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_CONFIG);

    _anomalyUpperMargin = internalConfig.getDouble(PercentileMetricAnomalyFinderConfig.METRIC_ANOMALY_UPPER_MARGIN_CONFIG);
    _anomalyLowerMargin = internalConfig.getDouble(PercentileMetricAnomalyFinderConfig.METRIC_ANOMALY_LOWER_MARGIN_CONFIG);

    String trimmedMetrics = ((String) configs.get(CruiseControlConfig.METRIC_ANOMALY_FINDER_METRICS_CONFIG)).trim();
    _interestedMetrics = new HashSet<>(Arrays.asList(trimmedMetrics.split(",")));
    // In case there is an empty string metric.
    _interestedMetrics.removeIf(String::isEmpty);
  }
}
