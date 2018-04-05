/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerEntity;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class will be scheduled to periodically check if {@link KafkaMetricAnomalyAnalyzer} identifies a metric anomaly.
 * An alert will be triggered if one of the goals is not met.
 */
public class MetricAnomalyDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricAnomalyDetector.class);
  private final LoadMonitor _loadMonitor;
  private final Queue<Anomaly<KafkaCruiseControl, KafkaCruiseControlException>> _anomalies;
  private final double _metricAnomalyPercentileThreshold;
  private final KafkaMetricAnomalyAnalyzer _kafkaMetricAnomalyAnalyzer;

  public MetricAnomalyDetector(KafkaCruiseControlConfig config,
                               LoadMonitor loadMonitor,
                               Queue<Anomaly<KafkaCruiseControl, KafkaCruiseControlException>> anomalies) {
    _loadMonitor = loadMonitor;
    _anomalies = anomalies;
    _metricAnomalyPercentileThreshold = config.getDouble(KafkaCruiseControlConfig.METRIC_ANOMALY_PERCENTILE_THRESHOLD_CONFIG);
    _kafkaMetricAnomalyAnalyzer =
        new KafkaMetricAnomalyAnalyzer(config.getList(KafkaCruiseControlConfig.METRIC_ANOMALY_ANALYZER_METRICS_CONFIG));
  }

  @Override
  public void run() {
    // Check if the load monitor is ready.
    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = _loadMonitor.taskRunnerState();
    if (!ViolationUtils.isLoadMonitorReady(loadMonitorTaskRunnerState)) {
      LOG.info("Skipping metric anomaly detection because load monitor is in {} state.", loadMonitorTaskRunnerState);
      return;
    }

    // Get the historical and current values of broker metrics.
    Map<BrokerEntity, ValuesAndExtrapolations> metricsHistoryByBroker = _loadMonitor.brokerMetrics().valuesAndExtrapolations();
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker = _loadMonitor.currentBrokerMetricValues();

    Collection<KafkaMetricAnomaly> anomalies =
        _kafkaMetricAnomalyAnalyzer.metricAnomalies(_metricAnomalyPercentileThreshold, metricsHistoryByBroker, currentMetricsByBroker);
    _anomalies.addAll(anomalies);
    LOG.debug("Metric anomaly detection finished.");
  }
}
