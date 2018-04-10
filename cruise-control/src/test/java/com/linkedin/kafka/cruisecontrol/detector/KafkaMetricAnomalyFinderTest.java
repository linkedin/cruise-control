/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.config.CruiseControlConfig;
import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomaly;
import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomalyFinder;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.easymock.EasyMock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.linkedin.cruisecontrol.detector.metricanomaly.PercentileMetricAnomalyFinderConfig.METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_CONFIG;
import static com.linkedin.cruisecontrol.detector.metricanomaly.PercentileMetricAnomalyFinderConfig.METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;


public class KafkaMetricAnomalyFinderTest {
  private final BrokerEntity _brokerEntity = new BrokerEntity("test-host", 0);

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Test
  public void testMetricAnomaliesWithNullArguments() {
    MetricAnomalyFinder<BrokerEntity> anomalyFinder = createKafkaMetricAnomalyFinder();

    expected.expect(IllegalArgumentException.class);
    assertTrue("IllegalArgumentException is expected for null history or null current metrics.",
               anomalyFinder.metricAnomalies(null, null).isEmpty());
  }

  @Test
  public void testMetricAnomalies() {
    MetricAnomalyFinder<BrokerEntity> anomalyFinder = createKafkaMetricAnomalyFinder();
    Map<BrokerEntity, ValuesAndExtrapolations> history = createHistory(20);
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = createCurrentMetrics(21, 21.0);

    Collection<MetricAnomaly<BrokerEntity>> anomalies = anomalyFinder.metricAnomalies(history, currentMetrics);

    assertTrue("There should be exactly a single metric anomaly", anomalies.size() == 1);

    MetricAnomaly<BrokerEntity> anomaly = anomalies.iterator().next();
    List<Long> expectedWindow = new ArrayList<>();
    expectedWindow.add(21L);

    assertEquals(34, anomaly.metricId().intValue());
    assertEquals(_brokerEntity, anomaly.entity());
    assertEquals(expectedWindow, anomaly.windows());
  }

  @Test
  public void testInsufficientData() {
    MetricAnomalyFinder<BrokerEntity> anomalyAnalyzer = createKafkaMetricAnomalyFinder();
    Map<BrokerEntity, ValuesAndExtrapolations> history = createHistory(19);
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = createCurrentMetrics(20, 20.0);

    Collection<MetricAnomaly<BrokerEntity>> anomalies = anomalyAnalyzer.metricAnomalies(history, currentMetrics);
    assertTrue(anomalies.isEmpty());
  }

  /**
   * Windows: numValues, numValues - 1, ..., 2, 1
   * metric id : only 34
   * metric values: [numValues, numValues - 1, ..., 1.0]
   */
  private ValuesAndExtrapolations createHistoryValuesAndExtrapolations(int numValues) {
    Map<Integer, MetricValues> valuesByMetricId = new HashMap<>();
    MetricValues historicalMetricValues = new MetricValues(numValues);
    double[] values = new double[numValues];
    for (int i = 0; i < values.length; i++) {
      values[i] = numValues - i;
    }
    historicalMetricValues.add(values);
    valuesByMetricId.put(34, historicalMetricValues);

    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(valuesByMetricId);
    ValuesAndExtrapolations historyValuesAndExtrapolations = new ValuesAndExtrapolations(aggregatedMetricValues, null);

    List<Long> windows = new ArrayList<>();
    for (long i = numValues; i > 0; i--) {
      windows.add(i);
    }
    historyValuesAndExtrapolations.setWindows(windows);

    return historyValuesAndExtrapolations;
  }

  /**
   * Create history with single broker.
   */
  private Map<BrokerEntity, ValuesAndExtrapolations> createHistory(int numValues) {
    Map<BrokerEntity, ValuesAndExtrapolations> history = new HashMap<>();
    ValuesAndExtrapolations valuesAndExtrapolations = createHistoryValuesAndExtrapolations(numValues);
    history.put(_brokerEntity, valuesAndExtrapolations);
    return history;
  }

  /**
   * Windows: window
   * metric id : only 34
   * metric values: [value]
   */
  private ValuesAndExtrapolations createCurrentValuesAndExtrapolations(long window, double value) {
    Map<Integer, MetricValues> valuesByMetricId = new HashMap<>();
    MetricValues currentMetricValues = new MetricValues(1);
    double[] values = new double[] {value};
    currentMetricValues.add(values);
    valuesByMetricId.put(34, currentMetricValues);

    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(valuesByMetricId);
    ValuesAndExtrapolations currentValuesAndExtrapolations = new ValuesAndExtrapolations(aggregatedMetricValues, null);

    List<Long> windows = new ArrayList<>();
    windows.add(window);
    currentValuesAndExtrapolations.setWindows(windows);

    return currentValuesAndExtrapolations;
  }
  /**
   * Create current metrics with single broker (with anomaly).
   */
  private Map<BrokerEntity, ValuesAndExtrapolations> createCurrentMetrics(long window, double value) {
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = new HashMap<>();
    ValuesAndExtrapolations valuesAndExtrapolations = createCurrentValuesAndExtrapolations(window, value);
    currentMetrics.put(_brokerEntity, valuesAndExtrapolations);
    return currentMetrics;
  }

  @SuppressWarnings("unchecked")
  private MetricAnomalyFinder<BrokerEntity> createKafkaMetricAnomalyFinder() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.METRIC_ANOMALY_ANALYZER_CLASSES_CONFIG, KafkaMetricAnomalyFinder.class.getName());
    props.setProperty(METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_CONFIG, "95.0");
    props.setProperty(METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_CONFIG, "2.0");
    props.setProperty(CruiseControlConfig.METRIC_ANOMALY_ANALYZER_METRICS_CONFIG,
                      "BROKER_PRODUCE_LOCAL_TIME_MS_MAX,BROKER_PRODUCE_LOCAL_TIME_MS_MEAN,BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX,"
                      + "BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN,BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX,"
                      + "BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN,BROKER_LOG_FLUSH_TIME_MS_MAX,BROKER_LOG_FLUSH_TIME_MS_MEAN");

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    Map<String, Object> originalConfigs = new HashMap<>(config.originals());
    originalConfigs.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, mockKafkaCruiseControl);

    List<MetricAnomalyFinder> kafkaMetricAnomalyFinders = config.getConfiguredInstances(
        KafkaCruiseControlConfig.METRIC_ANOMALY_ANALYZER_CLASSES_CONFIG,
        MetricAnomalyFinder.class,
        originalConfigs);
    return kafkaMetricAnomalyFinders.get(0);
  }
}
