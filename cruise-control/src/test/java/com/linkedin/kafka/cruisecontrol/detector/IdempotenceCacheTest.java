/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T1;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MaintenanceEvent.BROKERS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MaintenanceEvent.MAINTENANCE_EVENT_TYPE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MaintenanceEvent.TOPICS_WITH_RF_UPDATE_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class IdempotenceCacheTest {
  private static final Duration MOCK_CACHE_CLEANER_PERIOD = Duration.ofMillis(200);
  private static final Duration MOCK_CACHE_CLEANER_INITIAL_DELAY = Duration.ofSeconds(0);

  private static final long MOCK_INITIAL_ANOMALY_TIME_MS = 100L;
  private static final long MOCK_DISTINCT_ANOMALY_TIME_MS = 2 * MOCK_INITIAL_ANOMALY_TIME_MS;
  private static final Set<Integer> MOCK_BROKERS_OBJECT;
  static {
    Set<Integer> mockBrokersObject = new HashSet<>();
    for (int brokerId = 0; brokerId < 2; brokerId++) {
      mockBrokersObject.add(brokerId);
    }
    MOCK_BROKERS_OBJECT = Collections.unmodifiableSet(mockBrokersObject);
  }
  private static final Set<Integer> MOCK_BROKERS_OBJECT_DISTINCT = Collections.singleton(0);
  // Note that the use of MOCK_TOPICS_WITH_RF_UPDATE assumes the use of a cluster model, which contains T1.
  private static final Map<Short, String> MOCK_TOPICS_WITH_RF_UPDATE = Collections.singletonMap((short) 2, T1);
  // Note that the use of MOCK_TOPICS_WITH_RF_UPDATE assumes the use of a cluster model, which contains T1.
  private static final Map<Short, String> MOCK_TOPICS_WITH_RF_UPDATE_DISTINCT = Collections.singletonMap((short) 1, T1);
  private KafkaCruiseControlConfig _config;
  private KafkaCruiseControl _mockKafkaCruiseControl;

  /**
   * Setup the unit test.
   */
  @Before
  public void setup() {
    _mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    // Create Kafka Cruise Control config
    _config = new KafkaCruiseControlConfig(testProperties());
  }

  @Test
  public void testIdempotence() {
    // 1. Generate initial and duplicate events
    Set<MaintenanceEvent> initialEvents = new HashSet<>();
    Set<MaintenanceEvent> duplicateEvents = new HashSet<>();
    Map<String, Object> parameterConfigOverrides = new HashMap<>();
    parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _mockKafkaCruiseControl);
    parameterConfigOverrides.put(BROKERS_OBJECT_CONFIG, MOCK_BROKERS_OBJECT);
    parameterConfigOverrides.put(TOPICS_WITH_RF_UPDATE_CONFIG, MOCK_TOPICS_WITH_RF_UPDATE);

    for (MaintenanceEventType eventType : MaintenanceEventType.cachedValues()) {
      parameterConfigOverrides.put(MAINTENANCE_EVENT_TYPE_CONFIG, eventType);
      // Expect mocks.
      EasyMock.expect(_mockKafkaCruiseControl.config()).andReturn(_config).times(4);
      EasyMock.replay(_mockKafkaCruiseControl);
      parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, MOCK_INITIAL_ANOMALY_TIME_MS);
      initialEvents.add(_config.getConfiguredInstance(AnomalyDetectorConfig.MAINTENANCE_EVENT_CLASS_CONFIG,
                                                      MaintenanceEvent.class,
                                                      parameterConfigOverrides));
      duplicateEvents.add(_config.getConfiguredInstance(AnomalyDetectorConfig.MAINTENANCE_EVENT_CLASS_CONFIG,
                                                        MaintenanceEvent.class,
                                                        parameterConfigOverrides));
      EasyMock.verify(_mockKafkaCruiseControl);
      EasyMock.reset(_mockKafkaCruiseControl);
    }
    assertEquals(MaintenanceEventType.cachedValues().size(), initialEvents.size());
    assertEquals(MaintenanceEventType.cachedValues().size(), duplicateEvents.size());

    // 2. Generate distinct events: All except FIX_OFFLINE_REPLICAS and REBALANCE events are distinct from initial events.
    Set<MaintenanceEvent> distinctEvents = new HashSet<>();
    parameterConfigOverrides.put(BROKERS_OBJECT_CONFIG, MOCK_BROKERS_OBJECT_DISTINCT);
    parameterConfigOverrides.put(TOPICS_WITH_RF_UPDATE_CONFIG, MOCK_TOPICS_WITH_RF_UPDATE_DISTINCT);

    for (MaintenanceEventType eventType : MaintenanceEventType.cachedValues()) {
      parameterConfigOverrides.put(MAINTENANCE_EVENT_TYPE_CONFIG, eventType);
      // Expect mocks.
      EasyMock.expect(_mockKafkaCruiseControl.config()).andReturn(_config).times(2);
      EasyMock.replay(_mockKafkaCruiseControl);
      parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, MOCK_DISTINCT_ANOMALY_TIME_MS);
      distinctEvents.add(_config.getConfiguredInstance(AnomalyDetectorConfig.MAINTENANCE_EVENT_CLASS_CONFIG,
                                                        MaintenanceEvent.class,
                                                        parameterConfigOverrides));
      EasyMock.verify(_mockKafkaCruiseControl);
      EasyMock.reset(_mockKafkaCruiseControl);
    }
    assertEquals(MaintenanceEventType.cachedValues().size(), distinctEvents.size());

    // 3. Test idempotence.
    EasyMock.expect(_mockKafkaCruiseControl.config()).andReturn(_config).once();
    // The background thread cannot remove the events.
    EasyMock.expect(_mockKafkaCruiseControl.timeMs()).andReturn(MOCK_INITIAL_ANOMALY_TIME_MS).anyTimes();
    EasyMock.replay(_mockKafkaCruiseControl);

    IdempotenceCache idempotenceCache = new IdempotenceCache(_mockKafkaCruiseControl, MOCK_CACHE_CLEANER_PERIOD, MOCK_CACHE_CLEANER_INITIAL_DELAY);
    assertTrue(idempotenceCache.timeByMaintenanceEvent().isEmpty());

    // 3.1. Pass initial events.
    idempotenceCache.ensureIdempotency(initialEvents);
    assertEquals(MaintenanceEventType.cachedValues().size(), initialEvents.size());
    Map<MaintenanceEvent, Long> timeByMaintenanceEvent = idempotenceCache.timeByMaintenanceEvent();
    assertEquals(initialEvents.size(), timeByMaintenanceEvent.size());

    // 3.2. Pass the duplicate events.
    idempotenceCache.ensureIdempotency(duplicateEvents);
    assertTrue(duplicateEvents.isEmpty());
    timeByMaintenanceEvent = idempotenceCache.timeByMaintenanceEvent();
    assertEquals(initialEvents.size(), timeByMaintenanceEvent.size());

    // 3.3. Pass distinct events.
    idempotenceCache.ensureIdempotency(distinctEvents);
    // All except FIX_OFFLINE_REPLICAS and REBALANCE events are expected to be distinct
    assertEquals(MaintenanceEventType.cachedValues().size() - 2, distinctEvents.size());
    timeByMaintenanceEvent = idempotenceCache.timeByMaintenanceEvent();
    assertEquals(initialEvents.size() + MaintenanceEventType.cachedValues().size() - 2, timeByMaintenanceEvent.size());

    EasyMock.verify(_mockKafkaCruiseControl);
  }

  private static Properties testProperties() {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.setProperty(AnomalyDetectorConfig.MAINTENANCE_EVENT_IDEMPOTENCE_RETENTION_MS_CONFIG, "2");
    properties.setProperty(AnomalyDetectorConfig.MAINTENANCE_EVENT_MAX_IDEMPOTENCE_CACHE_SIZE_CONFIG, "10");

    return properties;
  }
}
