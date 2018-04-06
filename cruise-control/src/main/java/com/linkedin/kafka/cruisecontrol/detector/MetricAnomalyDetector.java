/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.detector.MetricAnomalyAnalyzer;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerEntity;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomalyAnalyzer.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;


/**
 * This class will be scheduled to periodically check if {@link KafkaMetricAnomalyAnalyzer} identifies a metric anomaly.
 * An alert will be triggered if one of the goals is not met.
 */
public class MetricAnomalyDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricAnomalyDetector.class);
  private final LoadMonitor _loadMonitor;
  private final Queue<Anomaly> _anomalies;
  private final List<MetricAnomalyAnalyzer> _kafkaMetricAnomalyAnalyzers;

  @SuppressWarnings("unchecked")
  public MetricAnomalyDetector(KafkaCruiseControlConfig config,
                               LoadMonitor loadMonitor,
                               Queue<Anomaly> anomalies,
                               KafkaCruiseControl kafkaCruiseControl) {
    _loadMonitor = loadMonitor;
    _anomalies = anomalies;

    _kafkaMetricAnomalyAnalyzers = config.getConfiguredInstances(
        KafkaCruiseControlConfig.METRIC_ANOMALY_ANALYZER_CLASSES_CONFIG, MetricAnomalyAnalyzer.class);

    Map<String, Object> originalConfigs = new HashMap<>(config.originals());
    originalConfigs.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, kafkaCruiseControl);

    for (MetricAnomalyAnalyzer<BrokerEntity, KafkaMetricAnomaly> kafkaMetricAnomalyAnalyzer : _kafkaMetricAnomalyAnalyzers) {
      kafkaMetricAnomalyAnalyzer.configure(originalConfigs);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
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

    for (MetricAnomalyAnalyzer<BrokerEntity, KafkaMetricAnomaly> kafkaMetricAnomalyAnalyzer : _kafkaMetricAnomalyAnalyzers) {
      _anomalies.addAll(kafkaMetricAnomalyAnalyzer.metricAnomalies(metricsHistoryByBroker, currentMetricsByBroker));
    }

    LOG.debug("Metric anomaly detection finished.");
  }
}
