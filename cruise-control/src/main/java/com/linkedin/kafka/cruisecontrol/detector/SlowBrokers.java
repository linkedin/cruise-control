/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.DemoteBrokerRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RemoveBrokersRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Map;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.extractKafkaCruiseControlObjectFromConfig;


/**
 * The slow brokers that have been detected.
 */
public class SlowBrokers extends KafkaMetricAnomaly {
  protected RemoveBrokersRunnable _removeBrokersRunnable;
  protected DemoteBrokerRunnable _demoteBrokerRunnable;

  /**
   * An anomaly to indicate slow broker(s).
   */
  public SlowBrokers() {
  }

  @Override
  public boolean fix() throws KafkaCruiseControlException {
    if (!_fixable) {
      return false;
    }

    if (_removeBrokersRunnable != null) {
      // Fix the cluster by removing the slow brokers.
      _optimizationResult = new OptimizationResult(_removeBrokersRunnable.removeBrokers(), null);
      // Ensure that only the relevant response is cached to avoid memory pressure.
      _optimizationResult.discardIrrelevantAndCacheJsonAndPlaintext();
      return true;
    } else if (_demoteBrokerRunnable != null) {
      // Fix the cluster by demoting the slow brokers.
      _optimizationResult = new OptimizationResult(_demoteBrokerRunnable.demoteBrokers(), null);
      // Ensure that only the relevant response is cached to avoid memory pressure.
      _optimizationResult.discardIrrelevantAndCacheJsonAndPlaintext();
      return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    KafkaCruiseControl kafkaCruiseControl = extractKafkaCruiseControlObjectFromConfig(configs, KafkaAnomalyType.METRIC_ANOMALY);
    if (_fixable) {
      Boolean removeSlowBroker = (Boolean) configs.get(SlowBrokerFinder.REMOVE_SLOW_BROKERS_CONFIG);
      if (removeSlowBroker == null) {
        throw new IllegalArgumentException(String.format("Missing %s for slow broker anomaly.", SlowBrokerFinder.REMOVE_SLOW_BROKERS_CONFIG));
      }
      KafkaCruiseControlConfig config = kafkaCruiseControl.config();
      boolean allowCapacityEstimation = config.getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
      boolean excludeRecentlyDemotedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
      boolean excludeRecentlyRemovedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
      if (removeSlowBroker) {
        _removeBrokersRunnable =
            new RemoveBrokersRunnable(kafkaCruiseControl,
                                      _brokerEntitiesWithDetectionTimeMs.keySet().stream().mapToInt(e -> e.brokerId()).boxed()
                                                                        .collect(Collectors.toSet()),
                                      getSelfHealingGoalNames(config),
                                      allowCapacityEstimation,
                                      excludeRecentlyDemotedBrokers,
                                      excludeRecentlyRemovedBrokers,
                                      _anomalyId.toString(),
                                      String.format("Self healing for slow brokers: %s", this));
      } else {
        _demoteBrokerRunnable =
            new DemoteBrokerRunnable(kafkaCruiseControl,
                                     _brokerEntitiesWithDetectionTimeMs.keySet().stream().mapToInt(e -> e.brokerId()).boxed()
                                                                       .collect(Collectors.toSet()),
                                     allowCapacityEstimation,
                                     excludeRecentlyDemotedBrokers,
                                     _anomalyId.toString(),
                                     String.format("Self healing for slow brokers: %s", this));
      }
    }
  }
}
