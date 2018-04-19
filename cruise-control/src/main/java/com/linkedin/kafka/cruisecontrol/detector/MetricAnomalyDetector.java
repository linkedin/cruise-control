/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomalyFinder;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerEntity;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will be scheduled to periodically check if {@link KafkaMetricAnomalyFinder} identifies a metric anomaly.
 * An alert will be triggered if one of the goals is not met.
 */
public class MetricAnomalyDetector implements Runnable {
  public static final String KAFKA_CRUISE_CONTROL_OBJECT_CONFIG = "kafka.cruise.control.object";
  private static final Logger LOG = LoggerFactory.getLogger(MetricAnomalyDetector.class);
  private final LoadMonitor _loadMonitor;
  private final Queue<Anomaly> _anomalies;
  private final List<MetricAnomalyFinder> _kafkaMetricAnomalyFinders;

  @SuppressWarnings("unchecked")
  public MetricAnomalyDetector(KafkaCruiseControlConfig config,
                               LoadMonitor loadMonitor,
                               Queue<Anomaly> anomalies,
                               KafkaCruiseControl kafkaCruiseControl) {
    _loadMonitor = loadMonitor;
    _anomalies = anomalies;

    Map<String, Object> configWithCruiseControlObject = Collections.singletonMap(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG,
                                                                                 kafkaCruiseControl);
    _kafkaMetricAnomalyFinders = config.getConfiguredInstances(
        KafkaCruiseControlConfig.METRIC_ANOMALY_FINDER_CLASSES_CONFIG,
        MetricAnomalyFinder.class,
        configWithCruiseControlObject);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run() {
    try {
      // Check if the load monitor is ready.
      LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = _loadMonitor.taskRunnerState();
      if (!ViolationUtils.isLoadMonitorReady(loadMonitorTaskRunnerState)) {
        LOG.info("Skipping metric anomaly detection because load monitor is in {} state.", loadMonitorTaskRunnerState);
        return;
      }

      // Get the historical and current values of broker metrics.
      Map<BrokerEntity, ValuesAndExtrapolations> metricsHistoryByBroker = _loadMonitor.brokerMetrics().valuesAndExtrapolations();
      Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker = _loadMonitor.currentBrokerMetricValues();

      for (MetricAnomalyFinder<BrokerEntity> kafkaMetricAnomalyFinder : _kafkaMetricAnomalyFinders) {
        _anomalies.addAll(kafkaMetricAnomalyFinder.metricAnomalies(metricsHistoryByBroker, currentMetricsByBroker));
      }

    } catch (Exception e) {
      LOG.warn("Metric Anomaly Detector encountered exception: ", e);
    } finally {
      LOG.debug("Metric anomaly detection finished.");
    }
  }
}
