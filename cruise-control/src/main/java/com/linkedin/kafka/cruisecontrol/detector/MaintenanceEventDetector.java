/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.MAINTENANCE_EVENT_READER_CLASS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getAnomalyDetectionStatus;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;


public class MaintenanceEventDetector extends AbstractAnomalyDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceEventDetector.class);
  public static final long DETECTION_NOT_READY_BACKOFF_MS = 10000L;
  // TODO: Make this configurable.
  public static final Duration READ_EVENTS_TIMEOUT = Duration.ofSeconds(5);
  private volatile boolean _shutdown;
  private final MaintenanceEventReader _maintenanceEventReader;

  public MaintenanceEventDetector(Queue<Anomaly> anomalies, KafkaCruiseControl kafkaCruiseControl) {
    super(anomalies, kafkaCruiseControl);
    KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
    _shutdown = false;
    Map<String, Object> configWithCruiseControlObject = Collections.singletonMap(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG,
                                                                                 kafkaCruiseControl);
    _maintenanceEventReader = config.getConfiguredInstance(MAINTENANCE_EVENT_READER_CLASS_CONFIG,
                                                           MaintenanceEventReader.class,
                                                           configWithCruiseControlObject);
  }

  void shutdown() {
    _shutdown = true;
  }

  @Override
  public void run() {
    while (!_shutdown) {
      try {
        if (getAnomalyDetectionStatus(_kafkaCruiseControl, false) != AnomalyDetectionStatus.READY) {
          _kafkaCruiseControl.sleep(DETECTION_NOT_READY_BACKOFF_MS);
        }

        // Ready for retrieving maintenance events for anomaly detection.
        Set<MaintenanceEvent> maintenanceEvents = _maintenanceEventReader.readEvents(READ_EVENTS_TIMEOUT);
        _anomalies.addAll(maintenanceEvents);
      } catch (Exception e) {
        LOG.warn("Maintenance event detector encountered an exception.", e);
      }
    }

    LOG.debug("Maintenance event detector is shutdown.");
  }
}
