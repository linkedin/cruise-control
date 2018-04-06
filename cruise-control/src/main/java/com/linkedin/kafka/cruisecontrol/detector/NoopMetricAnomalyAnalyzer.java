/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.MetricAnomalyAnalyzer;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerEntity;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;


/**
 * A no-op metric anomaly analyzer.
 */
public class NoopMetricAnomalyAnalyzer implements MetricAnomalyAnalyzer<BrokerEntity, KafkaMetricAnomaly> {

  @Override
  public Collection<KafkaMetricAnomaly> metricAnomalies(Map<BrokerEntity, ValuesAndExtrapolations> metricsHistoryByEntity,
                                                        Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByEntity) {
    return Collections.emptySet();
  }

  @Override
  public void configure(Map<String, ?> configs) {
  }
}
