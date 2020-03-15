/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomaly;
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
  private final static double NORMAL_BYTES_IN_RATE = 1024.0 * 1024.0;
  private final static double SMALL_BYTES_IN_RATE = 1024.0;
  private final static double NORMAL_LOG_FLUSH_TIME_MS = 100.0;

  /**
   * Test slow broker finder can detect the abnormal metric rise of single broker.
   */
  @Test
  public void testDetectingSlowBrokerFromHistory() {
    SlowBrokerFinder slowBrokerFinder = createSlowBrokerFinder();
    Map<BrokerEntity, ValuesAndExtrapolations> history =
        createHistory(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS), 10, BROKER_ENTITIES.get(0));
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics =
        createCurrentMetrics(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS * 5),
                      11, BROKER_ENTITIES.get(0));
    Collection<MetricAnomaly<BrokerEntity>> anomalies = slowBrokerFinder.metricAnomalies(history, currentMetrics);
    assertTrue("There should be exactly a single slow broker", anomalies.size() == 1);
    MetricAnomaly<BrokerEntity> anomaly = anomalies.iterator().next();
    assertTrue(anomaly.entities().containsKey(BROKER_ENTITIES.get(0)));
    assertEquals(ANOMALY_DETECTION_TIME_MS, (long) anomaly.entities().get(BROKER_ENTITIES.get(0)));
  }

  /**
   * Test slow broker finder can detect broker with abnormally high metric in the cluster.
   */
  @Test
  public void testDetectingSlowBrokerFromPeer() {
    SlowBrokerFinder slowBrokerFinder = createSlowBrokerFinder();
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = new HashMap<>(BROKER_ENTITIES.size());
    Map<BrokerEntity, ValuesAndExtrapolations> history = new HashMap<>(BROKER_ENTITIES.size());
    currentMetrics.putAll(createCurrentMetrics(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS * 11),
                          11, BROKER_ENTITIES.get(0)));
    history.putAll(createHistory(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS * 11),
                   10, BROKER_ENTITIES.get(0)));
    for (int i = 1; i < BROKER_ENTITIES.size(); i++) {
      currentMetrics.putAll(createCurrentMetrics(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS),
                            11, BROKER_ENTITIES.get(i)));
      history.putAll(createHistory(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS),
                     10, BROKER_ENTITIES.get(i)));
    }
    Collection<MetricAnomaly<BrokerEntity>> anomalies = slowBrokerFinder.metricAnomalies(history, currentMetrics);
    assertTrue("There should be exactly a single slow broker", anomalies.size() == 1);
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
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = new HashMap<>(BROKER_ENTITIES.size());
    Map<BrokerEntity, ValuesAndExtrapolations> history = new HashMap<>(BROKER_ENTITIES.size());
    currentMetrics.putAll(
        createCurrentMetrics(populateMetricValues(SMALL_BYTES_IN_RATE, SMALL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS), 11, BROKER_ENTITIES.get(0)));
    history.putAll(
        createHistory(populateMetricValues(SMALL_BYTES_IN_RATE, SMALL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS), 10, BROKER_ENTITIES.get(0)));
    for (int i = 1; i < BROKER_ENTITIES.size(); i++) {
      currentMetrics.putAll(createCurrentMetrics(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS),
                            11, BROKER_ENTITIES.get(i)));
      history.putAll(
          createHistory(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS), 10, BROKER_ENTITIES.get(i)));
    }
    Collection<MetricAnomaly<BrokerEntity>> anomalies = slowBrokerFinder.metricAnomalies(history, currentMetrics);
    assertTrue(anomalies.isEmpty());
  }

  /**
   * Test slow broker finder skips broker without enough metric history during detection.
   */
  @Test
  public void testInsufficientData() {
    SlowBrokerFinder slowBrokerFinder = createSlowBrokerFinder();
    Map<BrokerEntity, ValuesAndExtrapolations> history =
        createHistory(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS), 5, BROKER_ENTITIES.get(0));
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics =
        createCurrentMetrics(populateMetricValues(NORMAL_BYTES_IN_RATE, NORMAL_BYTES_IN_RATE, NORMAL_LOG_FLUSH_TIME_MS * 2),
                             6, BROKER_ENTITIES.get(0));
    Collection<MetricAnomaly<BrokerEntity>> anomalies = slowBrokerFinder.metricAnomalies(history, currentMetrics);
    assertTrue(anomalies.isEmpty());
  }

  private Map<Short, Double> populateMetricValues(double leaderBytesInRate, double replicationBytesInRate, double logFlushTimeMs) {
    Map<Short, Double> metricValueById = new HashMap<>(3);
    metricValueById.put(KafkaMetricDef.brokerMetricDef().metricInfo(KafkaMetricDef.BROKER_LOG_FLUSH_TIME_MS_999TH.name()).id(), logFlushTimeMs);
    metricValueById.put(KafkaMetricDef.brokerMetricDef().metricInfo(KafkaMetricDef.LEADER_BYTES_IN.name()).id(), leaderBytesInRate);
    metricValueById.put(KafkaMetricDef.brokerMetricDef().metricInfo(KafkaMetricDef.REPLICATION_BYTES_IN_RATE.name()).id(), replicationBytesInRate);
    return metricValueById;
  }

  private SlowBrokerFinder createSlowBrokerFinder() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnomalyDetectorConfig.METRIC_ANOMALY_FINDER_CLASSES_CONFIG, SlowBrokerFinder.class.getName());
    props.setProperty(AnomalyDetectorConfig.METRIC_ANOMALY_CLASS_CONFIG, SlowBrokers.class.getName());
    props.setProperty(SlowBrokerFinder.SLOW_BROKER_DEMOTION_SCORE_CONFIG, "0");
    return (SlowBrokerFinder) createMetricAnomalyFinder(props);
  }
}
