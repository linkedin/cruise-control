/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomaly;
import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomalyFinder;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;


/**
 * This class will be scheduled to periodically check if {@link KafkaMetricAnomalyFinder} identifies a metric anomaly.
 * An alert will be triggered if one of the goals is not met.
 */
public class MetricAnomalyDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricAnomalyDetector.class);
  public static final String METRIC_ANOMALY_DESCRIPTION_CONFIG = "metric.anomaly.description";
  public static final String METRIC_ANOMALY_TIME_WINDOW_CONFIG = "metric.anomaly.time.window";
  public static final String METRIC_ANOMALY_BROKER_ENTITY_CONFIG = "metric.anomaly.broker.entity";
  public static final String METRIC_ANOMALY_METRIC_ID_CONFIG = "metric.anomaly.metric.id";
  private final LoadMonitor _loadMonitor;
  private final Queue<KafkaAnomaly> _anomalies;
  private final List<MetricAnomalyFinder> _kafkaMetricAnomalyFinders;

  public MetricAnomalyDetector(LoadMonitor loadMonitor,
                               Queue<KafkaAnomaly> anomalies,
                               KafkaCruiseControl kafkaCruiseControl) {
    _loadMonitor = loadMonitor;
    _anomalies = anomalies;

    Map<String, Object> configWithCruiseControlObject = Collections.singletonMap(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG,
                                                                                 kafkaCruiseControl);
    _kafkaMetricAnomalyFinders = kafkaCruiseControl.config().getConfiguredInstances(
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
      if (!AnomalyUtils.isLoadMonitorReady(loadMonitorTaskRunnerState)) {
        LOG.info("Skipping metric anomaly detection because load monitor is in {} state.", loadMonitorTaskRunnerState);
        return;
      }

      // Get the historical and current values of broker metrics.
      Map<BrokerEntity, ValuesAndExtrapolations> metricsHistoryByBroker = _loadMonitor.brokerMetrics().valuesAndExtrapolations();
      Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker = _loadMonitor.currentBrokerMetricValues();

      for (MetricAnomalyFinder<BrokerEntity> kafkaMetricAnomalyFinder : _kafkaMetricAnomalyFinders) {
        for (MetricAnomaly<BrokerEntity> metricAnomaly: kafkaMetricAnomalyFinder.metricAnomalies(metricsHistoryByBroker, currentMetricsByBroker)) {
          _anomalies.add((KafkaMetricAnomaly) metricAnomaly);
        }
      }

    } catch (Exception e) {
      LOG.warn("Metric Anomaly Detector encountered exception: ", e);
    } finally {
      LOG.debug("Metric anomaly detection finished.");
    }
  }
}
