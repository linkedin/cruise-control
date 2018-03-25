/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregationOptions;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregationResult;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerEntity;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The metrics sample aggregator for brokers.
 */
public class KafkaBrokerMetricSampleAggregator extends MetricSampleAggregator<String, BrokerEntity> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaBrokerMetricSampleAggregator.class);
  /**
   * Construct the metric sample aggregator.
   *
   * @param config The load monitor configurations.
   */
  public KafkaBrokerMetricSampleAggregator(KafkaCruiseControlConfig config) {
    super(config.getInt(KafkaCruiseControlConfig.NUM_BROKER_METRICS_WINDOWS_CONFIG),
          config.getLong(KafkaCruiseControlConfig.BROKER_METRICS_WINDOW_MS_CONFIG),
          config.getInt(KafkaCruiseControlConfig.MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_CONFIG),
          config.getInt(KafkaCruiseControlConfig.MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER_CONFIG),
          config.getInt(KafkaCruiseControlConfig.BROKER_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG),
          KafkaMetricDef.brokerMetricDef());
  }

  public MetricSampleAggregationResult<String, BrokerEntity> aggregate(Set<BrokerEntity> brokerEntities) {
    AggregationOptions<String, BrokerEntity>  aggregationOptions =
        new AggregationOptions<>(0.0, 0.0, 1, brokerEntities,
                                 AggregationOptions.Granularity.ENTITY, false);
    if (super.numAvailableWindows() < 1) {
      LOG.trace("No window is available for any broker.");
      return null;
    }
    try {
      return aggregate(-1, System.currentTimeMillis(), aggregationOptions);
    } catch (NotEnoughValidWindowsException e) {
      return null;
    }
  }
}
