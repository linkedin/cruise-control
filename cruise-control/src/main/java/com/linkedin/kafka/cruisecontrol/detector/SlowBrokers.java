/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.DemoteBrokerRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RemoveBrokersRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Map;
import java.util.UUID;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.toDateString;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.extractKafkaCruiseControlObjectFromConfig;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType.SLOW_BROKER;


/**
 * The slow broker that have been detected.
 */
public class SlowBrokers extends KafkaAnomaly {
  protected static final String ID_PREFIX = AnomalyType.SLOW_BROKER.toString();
  protected Map<Integer, Long> _slowBrokers;
  protected Boolean _fixable;
  protected RemoveBrokersRunnable _removeBrokersRunnable;
  protected DemoteBrokerRunnable _demoteBrokerRunnable;
  protected String _anomalyId;


  /**
   * An anomaly to indicate slow broker(s).
   */
  public SlowBrokers() {
  }

  /**
   * Detected slow brokers.
   *
   * @return Detection time by broker id for slow brokers.
   */
  public Map<Integer, Long> slowBrokers() {
    return _slowBrokers;
  }

  /**
   * Whether the detected slow broker anomaly is fixable or not.
   *
   * @return True if the anomaly is fixable.
   */
  public boolean fixable() {
    return _fixable;
  }

  /**
   * Whether the self healing operation should remove all the replicas from the detected slow brokers.
   *
   * @return True to remove all replicas off detected slow brokers.
   */
  public boolean removeSlowBrokers() {
    return _fixable && _removeBrokersRunnable != null;
  }

  @Override
  public String anomalyId() {
    return _anomalyId;
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

  @Override
  public AnomalyType anomalyType() {
    return AnomalyType.SLOW_BROKER;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder().append("{\n");
    _slowBrokers.forEach((key, value) -> {
      sb.append("\tBroker ").append(key).append("'s performance degraded at ").append(toDateString(value)).append("\n");
    });
    sb.append("}");
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    KafkaCruiseControl kafkaCruiseControl = extractKafkaCruiseControlObjectFromConfig(configs, SLOW_BROKER);
    _slowBrokers = (Map<Integer, Long>) configs.get(SlowBrokerDetector.SLOW_BROKERS_OBJECT_CONFIG);
    if (_slowBrokers == null || _slowBrokers.isEmpty()) {
      throw new IllegalArgumentException("Missing broker ids for slow broker anomaly.");
    }
    _fixable = (Boolean) configs.get(SlowBrokerDetector.SLOW_BROKERS_FIXABLE_CONFIG);
    if (_fixable == null) {
      throw new IllegalArgumentException(String.format("Missing %s for slow broker anomaly.", SlowBrokerDetector.SLOW_BROKERS_FIXABLE_CONFIG));
    }
    if (_fixable) {
      Boolean removeSlowBroker = (Boolean) configs.get(SlowBrokerDetector.REMOVE_SLOW_BROKERS_CONFIG);
      if (removeSlowBroker == null) {
        throw new IllegalArgumentException(String.format("Missing %s for slow broker anomaly.", SlowBrokerDetector.REMOVE_SLOW_BROKERS_CONFIG));
      }
      _anomalyId = String.format("%s-%s", ID_PREFIX, UUID.randomUUID().toString().substring(ID_PREFIX.length() + 1));
      _optimizationResult = null;
      KafkaCruiseControlConfig config = kafkaCruiseControl.config();
      boolean allowCapacityEstimation = config.getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
      boolean excludeRecentlyDemotedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
      boolean excludeRecentlyRemovedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
      if (removeSlowBroker) {
        _removeBrokersRunnable =
            new RemoveBrokersRunnable(kafkaCruiseControl,
                                      _slowBrokers.keySet(),
                                      getSelfHealingGoalNames(config),
                                      allowCapacityEstimation,
                                      excludeRecentlyDemotedBrokers,
                                      excludeRecentlyRemovedBrokers,
                                      _anomalyId);
      } else {
        _demoteBrokerRunnable =
            new DemoteBrokerRunnable(kafkaCruiseControl,
                                     _slowBrokers.keySet(),
                                     allowCapacityEstimation,
                                     excludeRecentlyDemotedBrokers,
                                     _anomalyId);
      }
    }
  }
}
