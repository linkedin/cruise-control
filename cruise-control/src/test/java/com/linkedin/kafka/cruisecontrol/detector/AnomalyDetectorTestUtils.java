/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomalyFinder;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.easymock.EasyMock;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;


public final class AnomalyDetectorTestUtils {
  public static final List<BrokerEntity> BROKER_ENTITIES;
  public static final long ANOMALY_DETECTION_TIME_MS = 100L;
  static {
    List<BrokerEntity> brokerEntities = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      brokerEntities.add(new BrokerEntity("test-host", i));
    }
    BROKER_ENTITIES = Collections.unmodifiableList(brokerEntities);
  }

  private AnomalyDetectorTestUtils() {

  }

  /**
   * Create history load for broker.
   * @param metricValueById A set of metrics with corresponding load value.
   * @param metricFluctuationById A set of metrics with corresponding fluctuation. The fluctuation here refers to the maximal
   *                              fluctuation of metric value.
   * @param numWindows  Number of windows to populate.
   * @param broker The subject broker.
   * @return The load for the broker.
   */
  public static Map<BrokerEntity, ValuesAndExtrapolations> createHistory(Map<Short, Double> metricValueById,
                                                                         Map<Short, Double> metricFluctuationById,
                                                                         int numWindows,
                                                                         BrokerEntity broker) {
    Random random = new Random(0);
    Map<Short, MetricValues> valuesByMetricId = new HashMap<>();
    for (Map.Entry<Short, Double> entry : metricValueById.entrySet()) {
      MetricValues historicalMetricValues = new MetricValues(numWindows);
      double[] values = new double[numWindows];
      for (int i = 0; i < numWindows; i++) {
        // Add a uniformly-distributed fluctuation to metric value.
        values[i] = entry.getValue() + (random.nextInt(201) - 100) * metricFluctuationById.get(entry.getKey()) / 100;
      }
      historicalMetricValues.add(values);
      valuesByMetricId.put(entry.getKey(), historicalMetricValues);
    }
    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(valuesByMetricId);
    ValuesAndExtrapolations historyValuesAndExtrapolations = new ValuesAndExtrapolations(aggregatedMetricValues, null);
    List<Long> windows = new ArrayList<>();
    for (long i = numWindows; i > 0; i--) {
      windows.add(i);
    }
    historyValuesAndExtrapolations.setWindows(windows);
    return Collections.singletonMap(broker, historyValuesAndExtrapolations);
  }

  /**
   * Create current load snapshot for broker.
   * @param metricValueById A set of metrics with corresponding load value.
   * @param window  Current window number.
   * @param broker The subject broker.
   * @return The load for the broker.
   */
  public static Map<BrokerEntity, ValuesAndExtrapolations> createCurrentMetrics(Map<Short, Double> metricValueById,
                                                                                long window,
                                                                                BrokerEntity broker) {
    Map<Short, MetricValues> valuesByMetricId = new HashMap<>();
    for (Map.Entry<Short, Double> entry : metricValueById.entrySet()) {
      MetricValues currentMetricValues = new MetricValues(1);
      double[] values = new double[] {entry.getValue()};
      currentMetricValues.add(values);
      valuesByMetricId.put(entry.getKey(), currentMetricValues);
    }
    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(valuesByMetricId);
    ValuesAndExtrapolations currentValuesAndExtrapolations = new ValuesAndExtrapolations(aggregatedMetricValues, null);
    List<Long> windows = new ArrayList<>();
    windows.add(window);
    currentValuesAndExtrapolations.setWindows(windows);
    return Collections.singletonMap(broker, currentValuesAndExtrapolations);
  }

  /**
   * Create metric anomaly finder based on config.
   * @param props Cruise Control config.
   * @return The created anomaly finder.
   */
  @SuppressWarnings("unchecked")
  public static MetricAnomalyFinder<BrokerEntity> createMetricAnomalyFinder(Properties props) {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    EasyMock.expect(mockKafkaCruiseControl.config()).andReturn(config).anyTimes();
    EasyMock.expect(mockKafkaCruiseControl.timeMs()).andReturn(ANOMALY_DETECTION_TIME_MS).anyTimes();
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
