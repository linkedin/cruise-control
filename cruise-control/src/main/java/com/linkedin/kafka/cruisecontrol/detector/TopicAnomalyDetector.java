/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.MAX_METADATA_WAIT_MS;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.shouldSkipAnomalyDetection;

/**
 * This class will be scheduled to periodically check if {@link TopicAnomalyFinder} identifies a topic anomaly.
 * An alert will be triggered if one of the desired topic property is not met.
 */
public class TopicAnomalyDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TopicAnomalyDetector.class);
  private final Queue<Anomaly> _anomalies;
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final List<TopicAnomalyFinder> _topicAnomalyFinders;

  TopicAnomalyDetector(Queue<Anomaly> anomalies,
                       KafkaCruiseControl kafkaCruiseControl) {
    _anomalies = anomalies;
    _kafkaCruiseControl = kafkaCruiseControl;
    Map<String, Object> configWithCruiseControlObject = Collections.singletonMap(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG,
                                                                                 kafkaCruiseControl);
    _topicAnomalyFinders = kafkaCruiseControl.config().getConfiguredInstances(AnomalyDetectorConfig.TOPIC_ANOMALY_FINDER_CLASSES_CONFIG,
                                                                              TopicAnomalyFinder.class,
                                                                              configWithCruiseControlObject);
  }

  /**
   * Skip topic anomaly detection if any of the following is true:
   * <ul>
   *  <li>There is offline replicas in the cluster, which means there is dead brokers/disks. In this case
   * {@link BrokerFailureDetector} or {@link DiskFailureDetector} should take care of the anomaly.</li>
   *  <li>{@link AnomalyDetectorUtils#shouldSkipAnomalyDetection(KafkaCruiseControl)} returns true.
   * </ul>
   *
   * @return True to skip topic anomaly detection based on the current state, false otherwise.
   */
  private boolean shouldSkipMetricAnomalyDetection() {
    Set<Integer> brokersWithOfflineReplicas = _kafkaCruiseControl.loadMonitor().brokersWithOfflineReplicas(MAX_METADATA_WAIT_MS);
    if (!brokersWithOfflineReplicas.isEmpty()) {
      LOG.info("Skipping topic anomaly detection because there are dead brokers/disks in the cluster, flawed brokers: {}",
          brokersWithOfflineReplicas);
      return true;
    }
    return shouldSkipAnomalyDetection(_kafkaCruiseControl);
  }

  @Override
  public void run() {
    try {
      if (shouldSkipMetricAnomalyDetection()) {
        return;
      }
      for (TopicAnomalyFinder topicAnomalyFinder : _topicAnomalyFinders) {
        _anomalies.addAll(topicAnomalyFinder.topicAnomalies());
      }
    } catch (Exception e) {
      LOG.warn("Topic anomaly detector encountered exception: ", e);
    } finally {
      LOG.debug("Topic anomaly detection finished.");
    }
  }
}
