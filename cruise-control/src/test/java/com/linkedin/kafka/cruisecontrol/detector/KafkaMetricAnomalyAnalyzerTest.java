/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.config.CruiseControlConfig;
import com.linkedin.cruisecontrol.detector.MetricAnomalyAnalyzer;
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

import static com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomalyAnalyzer.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;


public class KafkaMetricAnomalyAnalyzerTest {
  private final BrokerEntity _brokerEntity = new BrokerEntity("test-host", 0);

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Test
  public void testMetricAnomaliesWithNullArguments() {
    MetricAnomalyAnalyzer<BrokerEntity, KafkaMetricAnomaly> anomalyAnalyzer = createKafkaMetricAnomalyAnalyzer();

    expected.expect(IllegalArgumentException.class);
    assertTrue("IllegalArgumentException is expected for null history or null current metrics.",
               anomalyAnalyzer.metricAnomalies(null, null).isEmpty());
  }

  @Test
  public void testMetricAnomalies() {
    MetricAnomalyAnalyzer<BrokerEntity, KafkaMetricAnomaly> anomalyAnalyzer = createKafkaMetricAnomalyAnalyzer();
    Map<BrokerEntity, ValuesAndExtrapolations> history = createHistory();
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = createCurrentMetrics();

    Collection<KafkaMetricAnomaly> anomalies = anomalyAnalyzer.metricAnomalies(history, currentMetrics);

    assertTrue("There should be exactly a single metric anomaly", anomalies.size() == 1);

    KafkaMetricAnomaly anomaly = anomalies.iterator().next();
    List<Long> expectedWindow = new ArrayList<>();
    expectedWindow.add(4L);

    assertEquals(34, anomaly.metricId().intValue());
    assertEquals(_brokerEntity, anomaly.entity());
    assertEquals(expectedWindow, anomaly.windows());
  }

  /**
   * Windows: 3, 2, 1
   * metric id : only 34
   * metric values: [3.0, 2.0, 1.0]
   */
  private ValuesAndExtrapolations createHistoryValuesAndExtrapolations() {
    Map<Integer, MetricValues> valuesByMetricId = new HashMap<>();
    MetricValues historicalMetricValues = new MetricValues(3);
    double[] values = new double[] {3.0, 2.0, 1.0};
    historicalMetricValues.add(values);
    valuesByMetricId.put(34, historicalMetricValues);

    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(valuesByMetricId);
    ValuesAndExtrapolations historyValuesAndExtrapolations = new ValuesAndExtrapolations(aggregatedMetricValues, null);

    List<Long> windows = new ArrayList<>();
    for (long i = 3; i > 0; i--) {
      windows.add(i);
    }
    historyValuesAndExtrapolations.setWindows(windows);

    return historyValuesAndExtrapolations;
  }

  /**
   * Create history with single broker.
   */
  private Map<BrokerEntity, ValuesAndExtrapolations> createHistory() {
    Map<BrokerEntity, ValuesAndExtrapolations> history = new HashMap<>();
    ValuesAndExtrapolations valuesAndExtrapolations = createHistoryValuesAndExtrapolations();
    history.put(_brokerEntity, valuesAndExtrapolations);
    return history;
  }

  /**
   * Windows: 4
   * metric id : only 34
   * metric values: [4.0]
   */
  private ValuesAndExtrapolations createCurrentValuesAndExtrapolations() {
    Map<Integer, MetricValues> valuesByMetricId = new HashMap<>();
    MetricValues currentMetricValues = new MetricValues(1);
    double[] values = new double[] {4.0};
    currentMetricValues.add(values);
    valuesByMetricId.put(34, currentMetricValues);

    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(valuesByMetricId);
    ValuesAndExtrapolations currentValuesAndExtrapolations = new ValuesAndExtrapolations(aggregatedMetricValues, null);

    List<Long> windows = new ArrayList<>();
    windows.add(4L);
    currentValuesAndExtrapolations.setWindows(windows);

    return currentValuesAndExtrapolations;
  }
  /**
   * Create current metrics with single broker (with anomaly).
   */
  private Map<BrokerEntity, ValuesAndExtrapolations> createCurrentMetrics() {
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = new HashMap<>();
    ValuesAndExtrapolations valuesAndExtrapolations = createCurrentValuesAndExtrapolations();
    currentMetrics.put(_brokerEntity, valuesAndExtrapolations);
    return currentMetrics;
  }

  @SuppressWarnings("unchecked")
  private MetricAnomalyAnalyzer<BrokerEntity, KafkaMetricAnomaly> createKafkaMetricAnomalyAnalyzer() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.METRIC_ANOMALY_ANALYZER_CLASSES_CONFIG, KafkaMetricAnomalyAnalyzer.class.getName());
    props.setProperty(CruiseControlConfig.METRIC_ANOMALY_PERCENTILE_UPPER_THRESHOLD_CONFIG, "95.0");
    props.setProperty(CruiseControlConfig.METRIC_ANOMALY_PERCENTILE_LOWER_THRESHOLD_CONFIG, "2.0");
    props.setProperty(CruiseControlConfig.METRIC_ANOMALY_ANALYZER_METRICS_CONFIG,
                      "BROKER_PRODUCE_LOCAL_TIME_MS_MAX,BROKER_PRODUCE_LOCAL_TIME_MS_MEAN,BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX,"
                      + "BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN,BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX,"
                      + "BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN,BROKER_LOG_FLUSH_TIME_MS_MAX,BROKER_LOG_FLUSH_TIME_MS_MEAN");

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    Map<String, Object> originalConfigs = new HashMap<>(config.originals());
    originalConfigs.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, mockKafkaCruiseControl);

    List<MetricAnomalyAnalyzer> kafkaMetricAnomalyAnalyzers = config.getConfiguredInstances(
        KafkaCruiseControlConfig.METRIC_ANOMALY_ANALYZER_CLASSES_CONFIG, MetricAnomalyAnalyzer.class);

    kafkaMetricAnomalyAnalyzers.get(0).configure(originalConfigs);

    return kafkaMetricAnomalyAnalyzers.get(0);
  }
}
