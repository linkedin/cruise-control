/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomalyFinder;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.shouldSkipAnomalyDetection;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.MAX_METADATA_WAIT_MS;

/**
 * This class will be scheduled to periodically check if {@link KafkaMetricAnomalyFinder} identifies a metric anomaly.
 * An alert will be triggered if one of the goals is not met.
 */
public class MetricAnomalyDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricAnomalyDetector.class);
  public static final String METRIC_ANOMALY_DESCRIPTION_OBJECT_CONFIG = "metric.anomaly.description.object";
  public static final String METRIC_ANOMALY_BROKER_ENTITIES_OBJECT_CONFIG = "metric.anomaly.broker.entities.object";
  public static final String METRIC_ANOMALY_FIXABLE_OBJECT_CONFIG = "metric.anomaly.fixable.object";
  private final Queue<Anomaly> _anomalies;
  private final List<MetricAnomalyFinder> _kafkaMetricAnomalyFinders;
  private final KafkaCruiseControl _kafkaCruiseControl;

  public MetricAnomalyDetector(Queue<Anomaly> anomalies,
                               KafkaCruiseControl kafkaCruiseControl) {
    _anomalies = anomalies;
    _kafkaCruiseControl = kafkaCruiseControl;
    Map<String, Object> configWithCruiseControlObject = Collections.singletonMap(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG,
                                                                                 kafkaCruiseControl);
    _kafkaMetricAnomalyFinders = kafkaCruiseControl.config().getConfiguredInstances(
        AnomalyDetectorConfig.METRIC_ANOMALY_FINDER_CLASSES_CONFIG,
        MetricAnomalyFinder.class,
        configWithCruiseControlObject);
  }

  /**
   * Skip metric anomaly detection if any of the following is true:
   * <ul>
   *  <li>There is offline replicas in the cluster, which means there is dead brokers/disks. In this case
   * {@link BrokerFailureDetector} or {@link DiskFailureDetector} should take care of the anomaly.</li>
   *  <li>{@link AnomalyDetectorUtils#shouldSkipAnomalyDetection(KafkaCruiseControl)} returns true.
   * </ul>
   *
   * @return True to skip metrics anomaly detection based on the current state, false otherwise.
   */
  private boolean shouldSkipMetricAnomalyDetection() {
    Set<Integer> brokersWithOfflineReplicas = _kafkaCruiseControl.loadMonitor().brokersWithOfflineReplicas(MAX_METADATA_WAIT_MS);
    if (!brokersWithOfflineReplicas.isEmpty()) {
      LOG.info("Skipping metric anomaly detection because there are dead brokers/disks in the cluster, flawed brokers: {}",
               brokersWithOfflineReplicas);
      return true;
    }
    return shouldSkipAnomalyDetection(_kafkaCruiseControl);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run() {
    try {
      if (shouldSkipMetricAnomalyDetection()) {
        return;
      }

      // Get the historical and current values of broker metrics.
      Map<BrokerEntity, ValuesAndExtrapolations> metricsHistoryByBroker = _kafkaCruiseControl.loadMonitor().brokerMetrics().valuesAndExtrapolations();
      Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker = _kafkaCruiseControl.loadMonitor().currentBrokerMetricValues();

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
