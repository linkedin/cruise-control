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
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
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
import static com.linkedin.cruisecontrol.detector.metricanomaly.PercentileMetricAnomalyFinderConfig.METRIC_ANOMALY_LOWER_MARGIN_CONFIG;
import static com.linkedin.cruisecontrol.detector.metricanomaly.PercentileMetricAnomalyFinderConfig.METRIC_ANOMALY_UPPER_MARGIN_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;


public class KafkaMetricAnomalyFinderTest {
  private final BrokerEntity _brokerEntity = new BrokerEntity("test-host", 0);
  private final long _anomalyDetectionTimeMs = 100L;

  @Rule
  public ExpectedException _expected = ExpectedException.none();

  @Test
  public void testMetricAnomaliesWithNullArguments() {
    MetricAnomalyFinder<BrokerEntity> anomalyFinder = createKafkaMetricAnomalyFinder();

    _expected.expect(IllegalArgumentException.class);
    assertTrue("IllegalArgumentException is expected for null history or null current metrics.",
               anomalyFinder.metricAnomalies(null, null).isEmpty());
  }

  @Test
  public void testMetricAnomalies() {
    MetricAnomalyFinder<BrokerEntity> anomalyFinder = createKafkaMetricAnomalyFinder();
    Map<BrokerEntity, ValuesAndExtrapolations> history = createHistory(20);
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = createCurrentMetrics(21, 30.0);

    Collection<MetricAnomaly<BrokerEntity>> anomalies = anomalyFinder.metricAnomalies(history, currentMetrics);

    assertTrue("There should be exactly a single metric anomaly", anomalies.size() == 1);

    MetricAnomaly<BrokerEntity> anomaly = anomalies.iterator().next();
    assertTrue(anomaly.entities().containsKey(_brokerEntity));
    assertEquals(_anomalyDetectionTimeMs, (long) anomaly.entities().get(_brokerEntity));
  }

  @Test
  public void testInsufficientData() {
    MetricAnomalyFinder<BrokerEntity> anomalyFinder = createKafkaMetricAnomalyFinder();
    Map<BrokerEntity, ValuesAndExtrapolations> history = createHistory(19);
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = createCurrentMetrics(20, 20.0);

    Collection<MetricAnomaly<BrokerEntity>> anomalies = anomalyFinder.metricAnomalies(history, currentMetrics);
    assertTrue(anomalies.isEmpty());
  }

  /**
   * Windows: numValues, numValues - 1, ..., 2, 1
   * metric id : only 55
   * metric values: [numValues, numValues - 1, ..., 1.0]
   */
  private ValuesAndExtrapolations createHistoryValuesAndExtrapolations(int numValues) {
    Map<Short, MetricValues> valuesByMetricId = new HashMap<>();
    MetricValues historicalMetricValues = new MetricValues(numValues);
    double[] values = new double[numValues];
    for (int i = 0; i < values.length; i++) {
      values[i] = numValues - i;
    }
    historicalMetricValues.add(values);
    valuesByMetricId.put((short) 55, historicalMetricValues);

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
   * metric id : only 55
   * metric values: [value]
   */
  private ValuesAndExtrapolations createCurrentValuesAndExtrapolations(long window, double value) {
    Map<Short, MetricValues> valuesByMetricId = new HashMap<>();
    MetricValues currentMetricValues = new MetricValues(1);
    double[] values = new double[] {value};
    currentMetricValues.add(values);
    valuesByMetricId.put((short) 55, currentMetricValues);

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
    props.setProperty(AnomalyDetectorConfig.METRIC_ANOMALY_FINDER_CLASSES_CONFIG, KafkaMetricAnomalyFinder.class.getName());
    props.setProperty(METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_CONFIG, "95.0");
    props.setProperty(METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_CONFIG, "5.0");
    props.setProperty(METRIC_ANOMALY_UPPER_MARGIN_CONFIG, "0.5");
    props.setProperty(METRIC_ANOMALY_LOWER_MARGIN_CONFIG, "0.2");
    props.setProperty(CruiseControlConfig.METRIC_ANOMALY_FINDER_METRICS_CONFIG,
                      "BROKER_PRODUCE_LOCAL_TIME_MS_50TH,BROKER_PRODUCE_LOCAL_TIME_MS_999TH,BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH,"
                      + "BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH,BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH,"
                      + "BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH,BROKER_LOG_FLUSH_TIME_MS_50TH,BROKER_LOG_FLUSH_TIME_MS_999TH");

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    EasyMock.expect(mockKafkaCruiseControl.config()).andReturn(config);
    EasyMock.expect(mockKafkaCruiseControl.timeMs()).andReturn(_anomalyDetectionTimeMs).anyTimes();
    EasyMock.replay(mockKafkaCruiseControl);
    Map<String, Object> originalConfigs = new HashMap<>(config.originals());
    originalConfigs.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, mockKafkaCruiseControl);

    List<MetricAnomalyFinder> kafkaMetricAnomalyFinders = config.getConfiguredInstances(
        AnomalyDetectorConfig.METRIC_ANOMALY_FINDER_CLASSES_CONFIG,
        MetricAnomalyFinder.class,
        originalConfigs);
    return kafkaMetricAnomalyFinders.get(0);
  }
}
