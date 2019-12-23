/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomaly;
import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomalyFinder;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cruisecontrol.detector.metricanomaly.PercentileMetricAnomalyFinderUtils.isDataSufficient;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.toDateString;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_DESCRIPTION_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_BROKER_ENTITIES_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_FIXABLE_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.ALL_TOPIC_BYTES_IN;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.ALL_TOPIC_REPLICATION_BYTES_IN;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_LOG_FLUSH_TIME_MS_999TH;


/**
 * This class will check whether there is broker performance degradation (i.e. slow broker) from collected broker metrics.
 *
 * Slow brokers are identified by calculating a derived broker metric
 * {@code BROKER_LOG_FLUSH_TIME_MS_999TH / (ALL_TOPIC_BYTES_IN + ALL_TOPIC_REPLICATION_BYTES_IN) for each broker and then
 * checking in two ways.
 * <ul>
 *   <li>Comparing the latest metric value against broker's own history. If the latest value is larger than
 *       {@link #HISTORY_METRIC_MARGIN} * ({@link #HISTORY_METRIC_PERCENTILE_THRESHOLD} of historical values), it is
 *       considered to be abnormally high.</li>
 *   <li>Comparing the latest metric value against the latest metric value of all active brokers in cluster (i.e. brokers
 *       which serve non-zero traffic). If the value is larger than {@link #PEER_METRIC_MARGIN} * ({@link #PEER_METRIC_PERCENTILE_THRESHOLD}
 *       of all metric values), it is considered to be abnormally high.</li>
 * </ul>
 *
 * If certain broker's metric value is abnormally high, the broker is marked as a slow broker suspect by the finder.
 * Then if this suspect broker's derived metric anomaly persists for some time, it is confirmed to be a slow broker and
 * the finder will report {@link SlowBrokers}} anomaly with broker demotion as self-healing proposal. If the metric
 * anomaly still persists for an extended time, the finder will eventually report {@link SlowBrokers}} anomaly with broker
 * removal as self-healing proposal.
 *
 * The time to report slow broker for demotion and removal is controlled by an internal broker scoring system.
 * The system keeps a "slowness score" for brokers which have metric anomaly detected recently. The scores are updated in
 * each round of detection with following rules.
 * <ul>
 *   <li> For any broker not in the scoring system, once there is metric anomaly detected on it, the broker is added to the system
 *        with the initial "slowness score" of one. </li>
 *   <li> For any broker in the scoring system, if there is metric anomaly detected on it, its "slowness score" increases
 *        by 1. Once the score exceeds {@link #SLOW_BROKER_DEMOTION_SCORE}, finder begins to report the broker as slow broker
 *        with broker demotion as self-healing proposal; once the score reaches {@link #SLOW_BROKER_DECOMMISSION_SCORE},
 *        finder begin to report the broker as slow broker with broker removal as self-healing proposal (if
 *        {@link #SELF_HEALING_SLOW_BROKERS_REMOVAL_ENABLED_CONFIG is configed to be true}).</li>
 *   <li> For any broker in the scoring system, if there is no metric anomaly detected on it, its "slowness score" decreases by 1.
 *        Once "slowness score" reaches zero, the broker is dropped from scoring system.</li>
 * </ul>
 *
 * Note: if there are too many brokers being confirmed as slow broker in the same run, the finder will report the {@link SlowBrokers}
 * anomaly as unfixable. Because this often indicates some serious issue in the cluster and probably requires administrator's
 * intervention to decide the right remediation strategy.
 */
public class SlowBrokerFinder implements MetricAnomalyFinder<BrokerEntity> {
  private static final Logger LOG = LoggerFactory.getLogger(SlowBrokerFinder.class);
  // The config to enable finder reporting slow broker anomaly with broker removal as self-healing proposal.
  public static final String SELF_HEALING_SLOW_BROKERS_REMOVAL_ENABLED_CONFIG = "self.healing.slow.brokers.removal.enabled";
  // The config finder uses to indicate anomaly to perform broker demotion or broker removal for self-healing.
  public static final String REMOVE_SLOW_BROKERS_CONFIG = "remove.slow.brokers";
  // Todo: make following configs configurable.
  // The percentile threshold used to compare latest metric value against historical value.
  private static final double HISTORY_METRIC_PERCENTILE_THRESHOLD = 90.0;
  // The margin used to compare latest metric value against historical value.
  private static final double HISTORY_METRIC_MARGIN = 3.0;
  // The percentile threshold used to compare last metric value against peers' latest value.
  private static final double PEER_METRIC_PERCENTILE_THRESHOLD = 50.0;
  // The margin used to compare last metric value against peers' latest value.
  private static final double PEER_METRIC_MARGIN = 5.0;
  // The score threshold to trigger a demotion for slow broker.
  private static final int SLOW_BROKER_DEMOTION_SCORE = 5;
  // The score threshold to trigger a removal for slow broker.
  private static final int SLOW_BROKER_DECOMMISSION_SCORE = 50;
  // The maximum ratio of slow brokers in the cluster to trigger self-healing operation.
  private static double SELF_HEALING_UNFIXABLE_RATIO = 0.1;
  private KafkaCruiseControl _kafkaCruiseControl;
  private boolean _slowBrokerRemovalEnabled;
  private final Map<BrokerEntity, Integer> _brokerSlownessScore;
  private final Map<BrokerEntity, Long> _detectedSlowBrokers;
  private final Percentile _percentile;

  public SlowBrokerFinder() {
    _brokerSlownessScore = new HashMap<>();
    _detectedSlowBrokers = new HashMap<>();
    _percentile = new Percentile();
  }

  private Set<BrokerEntity> detectMetricAnomalies(Map<BrokerEntity, ValuesAndExtrapolations> metricsHistoryByBroker,
                                                  Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker) {
    // Preprocess raw metrics to get the derived metrics for each broker.
    Map<BrokerEntity, List<Double>> historicalValueByBroker = new HashMap<>();
    Map<BrokerEntity, Double> currentValueByBroker = new HashMap<>();
    for (Map.Entry<BrokerEntity, ValuesAndExtrapolations> entry : currentMetricsByBroker.entrySet()) {
      BrokerEntity entity = entry.getKey();
      ValuesAndExtrapolations valuesAndExtrapolations = entry.getValue();
      double latestTotalBytesIn = valuesAndExtrapolations.metricValues().valuesFor(ALL_TOPIC_BYTES_IN.id()).latest() +
                                  valuesAndExtrapolations.metricValues().valuesFor(ALL_TOPIC_REPLICATION_BYTES_IN.id()).latest();
      double latestLogFlushTime = valuesAndExtrapolations.metricValues().valuesFor(BROKER_LOG_FLUSH_TIME_MS_999TH.id()).latest();
      // Ignore brokers which currently does not serve any traffic.
      if (latestTotalBytesIn > 0) {
        currentValueByBroker.put(entity, latestLogFlushTime / latestTotalBytesIn);
        valuesAndExtrapolations = metricsHistoryByBroker.get(entity);
        double[] historicalBytesIn = valuesAndExtrapolations.metricValues().valuesFor(ALL_TOPIC_BYTES_IN.id()).doubleArray();
        double[] historicalReplicationBytesIn = valuesAndExtrapolations.metricValues().valuesFor(ALL_TOPIC_REPLICATION_BYTES_IN.id()).doubleArray();
        double[] historicalLogFlushTime = valuesAndExtrapolations.metricValues().valuesFor(BROKER_LOG_FLUSH_TIME_MS_999TH.id()).doubleArray();
        List<Double> historicalValue = new ArrayList<>(historicalBytesIn.length);
        for (int i = 0; i < historicalBytesIn.length; i++) {
          double totalBytesIn = historicalBytesIn[i] + historicalReplicationBytesIn[i];
          if (totalBytesIn > 0) {
            historicalValue.add(historicalLogFlushTime[i] / totalBytesIn);
          }
        }
        historicalValueByBroker.put(entity, historicalValue);
      }
    }

    Set<BrokerEntity> detectedMetricAnomalies = new HashSet<>();
    // Detect metric anomalies by comparing each broker's current derived metric value against historical value.
    detectMetricAnomaliesFromHistory(historicalValueByBroker, currentValueByBroker, detectedMetricAnomalies);
    // Detect metric anomalies by comparing each broker's derived metric value against its peers' value.
    detectMetricAnomaliesFromPeers(currentValueByBroker, detectedMetricAnomalies);
    return detectedMetricAnomalies;
  }

  private void detectMetricAnomaliesFromHistory(Map<BrokerEntity, List<Double>> historicalValue,
                                                Map<BrokerEntity, Double> currentValue,
                                                Set<BrokerEntity> detectedMetricAnomalies) {
    for (Map.Entry<BrokerEntity, Double> entry : currentValue.entrySet()) {
      BrokerEntity entity = entry.getKey();
      if (isDataSufficient(historicalValue.size(), HISTORY_METRIC_PERCENTILE_THRESHOLD, HISTORY_METRIC_PERCENTILE_THRESHOLD)) {
        double [] data = historicalValue.get(entity).stream().mapToDouble(i -> i).toArray();
        _percentile.setData(data);
        if (currentValue.get(entity) > _percentile.evaluate(HISTORY_METRIC_PERCENTILE_THRESHOLD) * HISTORY_METRIC_MARGIN) {
          detectedMetricAnomalies.add(entity);
        }
      }
    }
  }

  private void detectMetricAnomaliesFromPeers(Map<BrokerEntity, Double> currentValue,
                                              Set<BrokerEntity> detectedMetricAnomalies) {
    if (isDataSufficient(currentValue.size(), PEER_METRIC_PERCENTILE_THRESHOLD, PEER_METRIC_PERCENTILE_THRESHOLD)) {
      double [] data = currentValue.values().stream().mapToDouble(i -> i).toArray();
      _percentile.setData(data);
      double base = _percentile.evaluate(PEER_METRIC_PERCENTILE_THRESHOLD);
      for (Map.Entry<BrokerEntity, Double> entry : currentValue.entrySet()) {
        if (currentValue.get(entry.getKey()) > base * PEER_METRIC_MARGIN) {
          detectedMetricAnomalies.add(entry.getKey());
        }
      }
    }
  }

  private SlowBrokers createSlowBrokersAnomaly(Map<BrokerEntity, Long> detectedBrokers,
                                               boolean fixable,
                                               boolean removeSlowBroker,
                                               String description) {
    Map<String, Object> parameterConfigOverrides = new HashMap<>(5);
    parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl);
    parameterConfigOverrides.put(METRIC_ANOMALY_DESCRIPTION_OBJECT_CONFIG, description);
    parameterConfigOverrides.put(METRIC_ANOMALY_BROKER_ENTITIES_OBJECT_CONFIG, detectedBrokers);
    parameterConfigOverrides.put(REMOVE_SLOW_BROKERS_CONFIG, removeSlowBroker);
    parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, _kafkaCruiseControl.timeMs());
    parameterConfigOverrides.put(METRIC_ANOMALY_FIXABLE_OBJECT_CONFIG, fixable);
    return _kafkaCruiseControl.config().getConfiguredInstance(AnomalyDetectorConfig.METRIC_ANOMALY_CLASS_CONFIG,
                                                              SlowBrokers.class,
                                                              parameterConfigOverrides);
  }

  private String getSlowBrokerDescription(Map<BrokerEntity, Long> detectedBrokers) {
    StringBuilder descriptionSb = new StringBuilder().append("{\n");
    detectedBrokers.forEach((key, value) -> {
      descriptionSb.append("\tBroker ").append(key.brokerId()).append("'s performance degraded at ").append(toDateString(value)).append("\n");
    });
    descriptionSb.append("}");
    return descriptionSb.toString();
  }

  @Override
  public Collection<MetricAnomaly<BrokerEntity>> metricAnomalies(Map<BrokerEntity, ValuesAndExtrapolations> metricsHistoryByBroker,
                                                                 Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker) {
    try {
      Set<BrokerEntity> detectedMetricAnomalies = detectMetricAnomalies(metricsHistoryByBroker, currentMetricsByBroker);
      updateBrokerSlownessScore(detectedMetricAnomalies);
      return createSlowBrokerAnomalies(detectedMetricAnomalies, metricsHistoryByBroker.size());
    } catch (Exception e) {
      LOG.warn("Slow broker detector encountered exception: ", e);
    } finally {
      LOG.debug("Slow broker detection finished.");
    }
    return Collections.emptySet();
  }

  private void updateBrokerSlownessScore(Set<BrokerEntity> detectedMetricAnomalies) {
    for (BrokerEntity broker : detectedMetricAnomalies) {
      // Update slow broker detection time and slowness score.
      Long currentTimeMs = _kafkaCruiseControl.timeMs();
      _detectedSlowBrokers.putIfAbsent(broker, currentTimeMs);
      _brokerSlownessScore.compute(broker, (k, v) -> (v == null) ? 1 : Math.min(v + 1, SLOW_BROKER_DECOMMISSION_SCORE));
    }
    // For brokers which are previously detected as slow brokers, decrease their slowness score if their metrics has
    // recovered back to normal range.
    for (Map.Entry<BrokerEntity, Integer> entry : _brokerSlownessScore.entrySet()) {
      BrokerEntity broker = entry.getKey();
      if (!detectedMetricAnomalies.contains(broker)) {
        Integer score = _brokerSlownessScore.get(broker);
        // If the score reaches zero, remove its suspicion.
        if (score != null && --score == 0) {
          _brokerSlownessScore.remove(broker);
          _detectedSlowBrokers.remove(broker);
        }
      }
    }
  }

  private Set<MetricAnomaly<BrokerEntity>> createSlowBrokerAnomalies(Set<BrokerEntity> detectedMetricAnomalies,
                                                                     int clusterSize) {
    Set<MetricAnomaly<BrokerEntity>> detectedSlowBrokers = new HashSet<>();
    Map<BrokerEntity, Long> brokersToDemote = new HashMap<>();
    Map<BrokerEntity, Long> brokersToRemove = new HashMap<>();

    for (BrokerEntity broker : detectedMetricAnomalies) {
      // Report anomaly if slowness score reaches threshold for broker decommission/demotion.
      int slownessScore = _brokerSlownessScore.get(broker);
      if (slownessScore == SLOW_BROKER_DECOMMISSION_SCORE) {
        brokersToRemove.put(broker, _detectedSlowBrokers.get(broker));
      } else if (slownessScore >= SLOW_BROKER_DEMOTION_SCORE) {
        brokersToDemote.put(broker, _detectedSlowBrokers.get(broker));
      }
    }

    // If too many brokers in the cluster are detected as slow brokers, report anomaly as not fixable.
    // Otherwise report anomaly with brokers to be removed/demoted.
    if (brokersToDemote.size() + brokersToRemove.size() > clusterSize * SELF_HEALING_UNFIXABLE_RATIO) {
      brokersToRemove.forEach(brokersToDemote::put);
      detectedSlowBrokers.add(createSlowBrokersAnomaly(brokersToDemote, false, false, getSlowBrokerDescription(brokersToDemote)));
    } else {
      if (!brokersToDemote.isEmpty()) {
        detectedSlowBrokers.add(createSlowBrokersAnomaly(brokersToDemote, true, false, getSlowBrokerDescription(brokersToDemote)));
      }
      if (!brokersToRemove.isEmpty()) {
        detectedSlowBrokers.add(createSlowBrokersAnomaly(brokersToRemove, _slowBrokerRemovalEnabled, true, getSlowBrokerDescription(brokersToRemove)));
      }
    }
    return detectedSlowBrokers;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    _kafkaCruiseControl = (KafkaCruiseControl) configs.get(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
    if (_kafkaCruiseControl == null) {
      throw new IllegalArgumentException("Slow broker detector is missing " + KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
    }
    // Config for slow broker removal.
    _slowBrokerRemovalEnabled = Boolean.parseBoolean((String) _kafkaCruiseControl.config().originals().get(SELF_HEALING_SLOW_BROKERS_REMOVAL_ENABLED_CONFIG));
  }
}
