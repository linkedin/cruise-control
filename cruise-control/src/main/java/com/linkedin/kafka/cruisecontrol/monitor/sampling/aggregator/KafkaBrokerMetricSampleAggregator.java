/*
 *
 *  * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *  
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaCruiseControlMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerEntity;


/**
 * The metrics sample aggregator for brokers.
 */
public class KafkaBrokerMetricSampleAggregator extends MetricSampleAggregator<String, BrokerEntity> {
  /**
   * Construct the metric sample aggregator.
   *
   * @param config The load monitor configurations.
   */
  public KafkaBrokerMetricSampleAggregator(KafkaCruiseControlConfig config) {
    super(config.getInt(KafkaCruiseControlConfig.NUM_METRICS_WINDOWS_CONFIG),
          config.getLong(KafkaCruiseControlConfig.METRICS_WINDOW_MS_CONFIG),
          config.getInt(KafkaCruiseControlConfig.MIN_SAMPLES_PER_METRICS_WINDOW_CONFIG),
          config.getInt(KafkaCruiseControlConfig.MAX_ALLOWED_EXTRAPOLATIONS_PER_ENTITY_CONFIG),
          config.getInt(KafkaCruiseControlConfig.METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG),
          KafkaCruiseControlMetricDef.commonMetricDef());
  }
}
