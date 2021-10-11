/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomaly;
import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomalyType;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorTestUtils.createHistory;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorTestUtils.createCurrentMetrics;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorTestUtils.BROKER_ENTITIES;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorTestUtils.ANOMALY_DETECTION_TIME_MS;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorTestUtils.createMetricAnomalyFinder;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;


public class SlowBrokerFinderTest {
  private static final double NORMAL_BYTES_IN_RATE = 4096;
  // The maximal ratio of fluctuation value to metric value for byte in rate metrics.
  private static final double BYTE_IN_RATE_VARIANCE_RATIO = 0.1;
  private static final double NORMAL_LOG_FLUSH_TIME_MS = 100.0;
  // The maximal ratio of fluctuation value to metric value for log flush time metric.
  private static final double LOG_FLUSH_TIME_MS_VARIANCE_RATIO = 0.25;
  private static final double SMALL_BYTES_IN_RATE = 1024;
  private static final int METRIC_HISTORY_WINDOW_SIZE = 10;
  private static final int CURRENT_METRIC_WINDOW = 11;
  private static final int METRIC_ANOMALY_MULTIPLIER = 4;

  /**
   * Test slow broker finder can detect the abnormal metric rise of single broker.
   * The metric values used in test case is extracted from one incident we encountered in production.
   */
  @Test
  public void testDetectingSlowBrokerFromHistory() {
    SlowBrokerFinder slowBrokerFinder = createSlowBrokerFinder();
    Map<BrokerEntity, ValuesAndExtrapolations> history =
        createHistory(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS),
                      populateMetricValues(NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                           NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                           NORMAL_LOG_FLUSH_TIME_MS * LOG_FLUSH_TIME_MS_VARIANCE_RATIO),
                      METRIC_HISTORY_WINDOW_SIZE, BROKER_ENTITIES.get(0));
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics =
        createCurrentMetrics(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE,
                                     NORMAL_LOG_FLUSH_TIME_MS * METRIC_ANOMALY_MULTIPLIER),
                             CURRENT_METRIC_WINDOW, BROKER_ENTITIES.get(0));
    Collection<MetricAnomaly<BrokerEntity>> anomalies = slowBrokerFinder.metricAnomalies(history, currentMetrics);
    assertEquals("There should be exactly a single slow broker", 1, anomalies.size());
    assertEquals("There should be exactly a single recent", 1, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.RECENT));
    assertEquals("There should be no suspect anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.SUSPECT));
    assertEquals("There should be no persistent anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.PERSISTENT));
    MetricAnomaly<BrokerEntity> anomaly = anomalies.iterator().next();
    assertTrue(anomaly.entities().containsKey(BROKER_ENTITIES.get(0)));
    assertEquals(ANOMALY_DETECTION_TIME_MS, (long) anomaly.entities().get(BROKER_ENTITIES.get(0)));
  }

  /**
   * Test slow broker finder can detect broker which has consistently abnormally high metric in the cluster.
   * The metric values used in test case is extracted from one incident we encountered in production.
   */
  @Test
  public void testDetectingSlowBrokerFromPeer() {
    SlowBrokerFinder slowBrokerFinder = createSlowBrokerFinder();
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = new HashMap<>();
    Map<BrokerEntity, ValuesAndExtrapolations> history = new HashMap<>();
    currentMetrics.putAll(createCurrentMetrics(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE,
                                                       NORMAL_LOG_FLUSH_TIME_MS * METRIC_ANOMALY_MULTIPLIER),
                                               CURRENT_METRIC_WINDOW, BROKER_ENTITIES.get(0)));
    history.putAll(createHistory(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE,
                                         NORMAL_LOG_FLUSH_TIME_MS * METRIC_ANOMALY_MULTIPLIER),
                                 populateMetricValues(NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                                      NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                                      NORMAL_LOG_FLUSH_TIME_MS * LOG_FLUSH_TIME_MS_VARIANCE_RATIO),
                                 METRIC_HISTORY_WINDOW_SIZE, BROKER_ENTITIES.get(0)));
    for (int i = 1; i < BROKER_ENTITIES.size(); i++) {
      currentMetrics.putAll(createCurrentMetrics(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS),
                            CURRENT_METRIC_WINDOW, BROKER_ENTITIES.get(i)));
      history.putAll(createHistory(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS),
                                   populateMetricValues(NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                                        NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                                        NORMAL_LOG_FLUSH_TIME_MS * LOG_FLUSH_TIME_MS_VARIANCE_RATIO),
                                   METRIC_HISTORY_WINDOW_SIZE, BROKER_ENTITIES.get(i)));
    }
    Collection<MetricAnomaly<BrokerEntity>> anomalies = slowBrokerFinder.metricAnomalies(history, currentMetrics);
    assertEquals("There should be exactly a single slow broker", 1, anomalies.size());
    assertEquals("There should be exactly a single recent", 1, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.RECENT));
    assertEquals("There should be no suspect anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.SUSPECT));
    assertEquals("There should be no persistent anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.PERSISTENT));
    MetricAnomaly<BrokerEntity> anomaly = anomalies.iterator().next();
    assertTrue(anomaly.entities().containsKey(BROKER_ENTITIES.get(0)));
    assertEquals(ANOMALY_DETECTION_TIME_MS, (long) anomaly.entities().get(BROKER_ENTITIES.get(0)));
  }

  /**
   * Test slow broker finder skips broker with negligible traffic during detection.
   */
  @Test
  public void testExcludingSmallTrafficBroker() {
    SlowBrokerFinder slowBrokerFinder = createSlowBrokerFinder();
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = new HashMap<>();
    Map<BrokerEntity, ValuesAndExtrapolations> history = new HashMap<>();
    currentMetrics.putAll(createCurrentMetrics(populateMetricValues(SMALL_BYTES_IN_RATE, SMALL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS),
                          CURRENT_METRIC_WINDOW, BROKER_ENTITIES.get(0)));
    history.putAll(createHistory(populateMetricValues(SMALL_BYTES_IN_RATE, SMALL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS),
                                 populateMetricValues(SMALL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                                      SMALL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                                      NORMAL_LOG_FLUSH_TIME_MS * LOG_FLUSH_TIME_MS_VARIANCE_RATIO),
                                 METRIC_HISTORY_WINDOW_SIZE, BROKER_ENTITIES.get(0)));
    for (int i = 1; i < BROKER_ENTITIES.size(); i++) {
      currentMetrics.putAll(createCurrentMetrics(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS),
                            CURRENT_METRIC_WINDOW, BROKER_ENTITIES.get(i)));
      history.putAll(createHistory(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS),
                                   populateMetricValues(NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                                        NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                                        NORMAL_LOG_FLUSH_TIME_MS * LOG_FLUSH_TIME_MS_VARIANCE_RATIO),
                                   METRIC_HISTORY_WINDOW_SIZE, BROKER_ENTITIES.get(0)));
    }
    Collection<MetricAnomaly<BrokerEntity>> anomalies = slowBrokerFinder.metricAnomalies(history, currentMetrics);
    assertTrue(anomalies.isEmpty());
    assertEquals("There should be no recent anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.RECENT));
    assertEquals("There should be no suspect anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.SUSPECT));
    assertEquals("There should be no persistent anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.PERSISTENT));
  }

  /**
   * Test slow broker finder skips broker without enough metric history during detection.
   */
  @Test
  public void testInsufficientData() {
    SlowBrokerFinder slowBrokerFinder = createSlowBrokerFinder();
    Map<BrokerEntity, ValuesAndExtrapolations> history =
        createHistory(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS),
                      populateMetricValues(NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                           NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                           NORMAL_LOG_FLUSH_TIME_MS * LOG_FLUSH_TIME_MS_VARIANCE_RATIO),
                      METRIC_HISTORY_WINDOW_SIZE / 2, BROKER_ENTITIES.get(0));
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics =
        createCurrentMetrics(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE,
                                     NORMAL_LOG_FLUSH_TIME_MS * METRIC_ANOMALY_MULTIPLIER),
                             METRIC_HISTORY_WINDOW_SIZE / 2 + 1, BROKER_ENTITIES.get(0));
    Collection<MetricAnomaly<BrokerEntity>> anomalies = slowBrokerFinder.metricAnomalies(history, currentMetrics);
    assertTrue(anomalies.isEmpty());
    assertEquals("There should be no recent anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.RECENT));
    assertEquals("There should be no suspect anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.SUSPECT));
    assertEquals("There should be no persistent anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.PERSISTENT));
  }

  /**
   * Test slow broker finder does not report false positive anomaly due to broker traffic fluctuation.
   */
  @Test
  public void testNoFalsePositiveDetectionDueToTrafficFluctuation() {
    SlowBrokerFinder slowBrokerFinder = createSlowBrokerFinder();
    Map<BrokerEntity, ValuesAndExtrapolations> history =
        createHistory(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS),
                      populateMetricValues(NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                           NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                           NORMAL_LOG_FLUSH_TIME_MS * LOG_FLUSH_TIME_MS_VARIANCE_RATIO),
                      METRIC_HISTORY_WINDOW_SIZE, BROKER_ENTITIES.get(0));
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics =
        createCurrentMetrics(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS),
                             CURRENT_METRIC_WINDOW, BROKER_ENTITIES.get(0));
    Collection<MetricAnomaly<BrokerEntity>> anomalies = slowBrokerFinder.metricAnomalies(history, currentMetrics);
    assertTrue(anomalies.isEmpty());
    assertEquals("There should be no recent anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.RECENT));
    assertEquals("There should be no suspect anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.SUSPECT));
    assertEquals("There should be no persistent anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.PERSISTENT));
  }

  /**
   * Test slow broker finder does not report false positive anomaly when the broker's absolute log flush time is small.
   */
  @Test
  public void testNoFalsePositiveDetectionOnSmallLogFlushTime() {
    SlowBrokerFinder slowBrokerFinder = createSlowBrokerFinder();
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = new HashMap<>();
    Map<BrokerEntity, ValuesAndExtrapolations> history = new HashMap<>();
    currentMetrics.putAll(createCurrentMetrics(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE,
                                                                    NORMAL_LOG_FLUSH_TIME_MS / 10 * METRIC_ANOMALY_MULTIPLIER),
                                               CURRENT_METRIC_WINDOW, BROKER_ENTITIES.get(0)));
    history.putAll(createHistory(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE,
                                                      NORMAL_LOG_FLUSH_TIME_MS / 10 * METRIC_ANOMALY_MULTIPLIER),
                                                      populateMetricValues(NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                                                           NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                                                           NORMAL_LOG_FLUSH_TIME_MS / 10 * LOG_FLUSH_TIME_MS_VARIANCE_RATIO),
                                                      METRIC_HISTORY_WINDOW_SIZE, BROKER_ENTITIES.get(0)));
    for (int i = 1; i < BROKER_ENTITIES.size(); i++) {
      currentMetrics.putAll(createCurrentMetrics(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS / 10),
                                                 CURRENT_METRIC_WINDOW, BROKER_ENTITIES.get(i)));
      history.putAll(createHistory(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS / 10),
                                   populateMetricValues(NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                                        NORMAL_BYTES_IN_RATE * BYTE_IN_RATE_VARIANCE_RATIO,
                                                        NORMAL_LOG_FLUSH_TIME_MS / 10 * LOG_FLUSH_TIME_MS_VARIANCE_RATIO),
                                   METRIC_HISTORY_WINDOW_SIZE, BROKER_ENTITIES.get(i)));
    }
    Collection<MetricAnomaly<BrokerEntity>> anomalies = slowBrokerFinder.metricAnomalies(history, currentMetrics);
    assertTrue(anomalies.isEmpty());
    assertEquals("There should be no recent anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.RECENT));
    assertEquals("There should be no suspect anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.SUSPECT));
    assertEquals("There should be no persistent anomaly", 0, slowBrokerFinder.numAnomaliesOfType(MetricAnomalyType.PERSISTENT));
  }

  private Map<Short, Double> populateMetricValues(double leaderBytesInRate, double replicationBytesInRate, double logFlushTimeMs) {
    return Map.of(KafkaMetricDef.brokerMetricDef().metricInfo(KafkaMetricDef.BROKER_LOG_FLUSH_TIME_MS_999TH.name()).id(), logFlushTimeMs,
                  KafkaMetricDef.brokerMetricDef().metricInfo(KafkaMetricDef.LEADER_BYTES_IN.name()).id(), leaderBytesInRate,
                  KafkaMetricDef.brokerMetricDef().metricInfo(KafkaMetricDef.REPLICATION_BYTES_IN_RATE.name()).id(), replicationBytesInRate);
  }

  private SlowBrokerFinder createSlowBrokerFinder() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnomalyDetectorConfig.METRIC_ANOMALY_FINDER_CLASSES_CONFIG, SlowBrokerFinder.class.getName());
    props.setProperty(AnomalyDetectorConfig.METRIC_ANOMALY_CLASS_CONFIG, SlowBrokers.class.getName());
    props.setProperty(SlowBrokerFinder.SLOW_BROKER_DEMOTION_SCORE_CONFIG, "0");
    props.setProperty(SlowBrokerFinder.SLOW_BROKER_LOG_FLUSH_TIME_THRESHOLD_MS_CONFIG, "150.0");
    return (SlowBrokerFinder) createMetricAnomalyFinder(props);
  }
}
