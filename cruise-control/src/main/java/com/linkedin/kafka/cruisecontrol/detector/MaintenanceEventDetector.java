/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getAnomalyDetectionStatus;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;


public class MaintenanceEventDetector extends AbstractAnomalyDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceEventDetector.class);
  public static final long DETECTION_NOT_READY_BACKOFF_MS = TimeUnit.SECONDS.toMillis(10);
  public static final long IDEMPOTENCE_CACHE_CLEANER_PERIOD_SECONDS = 5;
  public static final long IDEMPOTENCE_CACHE_CLEANER_INITIAL_DELAY_SECONDS = 0;
  // TODO: Make this configurable.
  public static final Duration READ_EVENTS_TIMEOUT = Duration.ofSeconds(5);
  private volatile boolean _shutdown;
  private final MaintenanceEventReader _maintenanceEventReader;
  private final boolean _idempotenceEnabled;
  private final Duration _idempotenceRetention;
  private final ScheduledExecutorService _idempotenceCacheCleaner;
  private final Map<Long, MaintenanceEvent> _maintenanceEventByTime;

  public MaintenanceEventDetector(Queue<Anomaly> anomalies, KafkaCruiseControl kafkaCruiseControl) {
    super(anomalies, kafkaCruiseControl);
    KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
    _shutdown = false;
    Map<String, Object> configWithCruiseControlObject = Collections.singletonMap(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG,
                                                                                 kafkaCruiseControl);
    _maintenanceEventReader = config.getConfiguredInstance(AnomalyDetectorConfig.MAINTENANCE_EVENT_READER_CLASS_CONFIG,
                                                           MaintenanceEventReader.class,
                                                           configWithCruiseControlObject);
    _idempotenceEnabled = config.getBoolean(AnomalyDetectorConfig.MAINTENANCE_EVENT_ENABLE_IDEMPOTENCE_CONFIG);
    _idempotenceRetention = Duration.ofMillis(config.getLong(AnomalyDetectorConfig.MAINTENANCE_EVENT_IDEMPOTENCE_RETENTION_MS_CONFIG));
    if (_idempotenceEnabled) {
      int maxIdempotenceCacheSize = config.getInt(AnomalyDetectorConfig.MAINTENANCE_EVENT_MAX_IDEMPOTENCE_CACHE_SIZE_CONFIG);
      _maintenanceEventByTime = new LinkedHashMap<Long, MaintenanceEvent>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, MaintenanceEvent> eldest) {
          return this.size() > maxIdempotenceCacheSize;
        }
      };
      _idempotenceCacheCleaner = Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("IdempotenceCacheCleaner"));
      _idempotenceCacheCleaner.scheduleAtFixedRate(new IdempotenceCacheCleaner(),
                                                   IDEMPOTENCE_CACHE_CLEANER_INITIAL_DELAY_SECONDS,
                                                   IDEMPOTENCE_CACHE_CLEANER_PERIOD_SECONDS,
                                                   TimeUnit.SECONDS);
    } else {
      _maintenanceEventByTime = null;
      _idempotenceCacheCleaner = null;
    }
  }

  private synchronized void removeOldEventsFromIdempotenceCache() {
    LOG.debug("Remove old events from idempotence cache.");
    _maintenanceEventByTime.entrySet().removeIf(entry -> (entry.getKey() + _idempotenceRetention.toMillis() < System.currentTimeMillis()));
  }

  /**
   * A runnable class to remove old events from idempotence cache.
   * The cleaner is expected to run only if {@link AnomalyDetectorConfig#MAINTENANCE_EVENT_ENABLE_IDEMPOTENCE_CONFIG} is enabled.
   */
  private class IdempotenceCacheCleaner implements Runnable {
    @Override
    public void run() {
      try {
        removeOldEventsFromIdempotenceCache();
      } catch (Throwable t) {
        LOG.warn("Received exception when trying to remove old events from idempotence cache.", t);
      }
    }
  }

  /**
   * Ensure idempotency upon reading given events from maintenance event reader.
   * <ul>
   *   <li>Drop duplicates from the given maintenance events, and</li>
   *   <li>Refresh the idempotence cache with new events if the event is not already in the cache.</li>
   * </ul>
   *
   * A {@link MaintenanceEvent} is considered as a duplicate, if it requires the same actions as any event in the idempotence cache.
   *
   * @param newEvents New maintenance events to (1) drop the duplicates from and (2) refresh the idempotence cache with.
   */
  private synchronized void ensureIdempotency(Set<MaintenanceEvent> newEvents) {
    // Drop duplicates from the given maintenance events
    newEvents.removeIf(_maintenanceEventByTime::containsValue);
    // Refresh the idempotence cache with new events
    newEvents.forEach(event -> _maintenanceEventByTime.put(event.detectionTimeMs(), event));
  }

  void shutdown() {
    _shutdown = true;
    if (_idempotenceCacheCleaner != null) {
      _idempotenceCacheCleaner.shutdownNow();
    }
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
        if (_idempotenceEnabled) {
          ensureIdempotency(maintenanceEvents);
        }
        _anomalies.addAll(maintenanceEvents);
      } catch (Exception e) {
        LOG.warn("Maintenance event detector encountered an exception.", e);
      }
    }
    try {
      _maintenanceEventReader.close();
    } catch (Exception e) {
      LOG.error("Received exception while closing maintenance event reader", e);
    }

    LOG.debug("Maintenance event detector is shutdown.");
  }
}
