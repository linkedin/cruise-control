/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomaly;
import com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomalyFinder;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomalyFinderUtils.isDataSufficient;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.toDateString;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_DESCRIPTION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_BROKER_ENTITIES_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_FIXABLE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.ALL_TOPIC_BYTES_IN;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.ALL_TOPIC_REPLICATION_BYTES_IN;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_LOG_FLUSH_TIME_MS_999TH;


/**
 * This class will check whether there is broker performance degradation (i.e slow broker) from collected broker metrics.
 *
 * Slow brokers are identified by calculating a derived broker metric
 * {@code BROKER_LOG_FLUSH_TIME_MS_999TH / (ALL_TOPIC_BYTES_IN + ALL_TOPIC_REPLICATION_BYTES_IN) for each broker and then
 * checking in two ways.
 * <ul>
 *   <li>Comparing the latest against broker's own history.</li>
 *   <li>Comparing the latest against the last of rest brokers in cluster.</li>
 * </ul>
 *
 * If certain broker's metric value is abnormally high(out of the normal range based on historical value/cluster average),
 * it is marked as a slow broker suspect by the finder. Then if this suspect broker's derived metric anomaly persists
 * for some time, it is confirmed to be a slow broker and the finder will report a {@link SlowBrokers}} anomaly
 * with broker demotion as self-healing proposal. If the metric anomaly still persists for an extended time, the finder
 * will eventually report another {@link SlowBrokers}} anomaly with broker removal as self-healing proposal.
 *
 * Note if there are too many brokers confirmed as slow broker in the same run, the finder will report the {@link SlowBrokers}
 * anomaly as unfixable. Because this often indicates some serious issue in the cluster and probably requires administrator's
 * intervention to decide the right remediation strategy.
 */
public class SlowBrokerFinder implements MetricAnomalyFinder<BrokerEntity> {
  private static final Logger LOG = LoggerFactory.getLogger(SlowBrokerFinder.class);
  public static final String SELF_HEALING_SLOW_BROKERS_REMOVAL_ENABLED_CONFIG = "self.healing.slow.brokers.removal.enabled";
  public static final String REMOVE_SLOW_BROKERS_CONFIG = "remove.slow.brokers";
  // Todo: make following configs configurable.
  // The percentile threshold used to compare latest metric value against historical value.
  private static final double HISTORY_METRIC_PERCENTILE_THRESHOLD = 90.0;
  // The margin used to compare latest metric value against historical value.
  private static final double HISTORY_METRIC_MARGIN = 3.0;
  // The percentile threshold used to compare last metric value again peers' latest value.
  private static final double PEER_METRIC_PERCENTILE_THRESHOLD = 50.0;
  // The margin used to compare last metric value again peers' latest value.
  private static final double PEER_METRIC_MARGIN = 5.0;
  // The score threshold to trigger a demotion for slow broker.
  private static final int SLOW_BROKER_DEMOTION_SCORE = 5;
  // The score threshold to trigger a removal for slow broker.
  private static final int SLOW_BROKER_DECOMMISSION_SCORE = 50;
  // The maximal ratio of slow brokers in the cluster to trigger self-healing operation.
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

  private Set<BrokerEntity> detectSlowBrokers(Map<BrokerEntity, ValuesAndExtrapolations> metricsHistoryByBroker,
                                              Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker) {
    // Preprocess raw metrics to get the derived metrics for each broker.
    Map<BrokerEntity, List<Double>> historyValueByBroker = new HashMap<>();
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
        double[] historyBytesIn = valuesAndExtrapolations.metricValues().valuesFor(ALL_TOPIC_BYTES_IN.id()).doubleArray();
        double[] historyReplicationBytesIn = valuesAndExtrapolations.metricValues().valuesFor(ALL_TOPIC_REPLICATION_BYTES_IN.id()).doubleArray();
        double[] historyLogFlushTime = valuesAndExtrapolations.metricValues().valuesFor(BROKER_LOG_FLUSH_TIME_MS_999TH.id()).doubleArray();
        List<Double> historyValue = new ArrayList<>(historyBytesIn.length);
        for (int i = 0; i < historyBytesIn.length; i++) {
          double totalBytesIn = historyBytesIn[i] + historyReplicationBytesIn[i];
          if (totalBytesIn > 0) {
            historyValue.add(historyLogFlushTime[i] / totalBytesIn);
          }
        }
        historyValueByBroker.put(entity, historyValue);
      }
    }

    Set<BrokerEntity> detectedSlowBrokers = new HashSet<>();
    // Detect slow broker by comparing each broker's current derived metric value against historical value.
    detectSlowBrokersFromHistory(historyValueByBroker, currentValueByBroker, detectedSlowBrokers);
    // Detect slow broker by comparing each broker's derived metric value against its peers' value.
    detectSlowBrokersFromPeers(currentValueByBroker, detectedSlowBrokers);
    return detectedSlowBrokers;
  }

  private void detectSlowBrokersFromHistory(Map<BrokerEntity, List<Double>> historyValue,
                                            Map<BrokerEntity, Double> currentValue,
                                            Set<BrokerEntity> detectedSlowBrokers) {
    for (Map.Entry<BrokerEntity, Double> entry : currentValue.entrySet()) {
      BrokerEntity entity = entry.getKey();
      if (isDataSufficient(historyValue.size(), HISTORY_METRIC_PERCENTILE_THRESHOLD, HISTORY_METRIC_PERCENTILE_THRESHOLD)) {
        double [] data = historyValue.get(entity).stream().mapToDouble(i -> i).toArray();
        _percentile.setData(data);
        if (currentValue.get(entity) > _percentile.evaluate(HISTORY_METRIC_PERCENTILE_THRESHOLD) * HISTORY_METRIC_MARGIN) {
          detectedSlowBrokers.add(entity);
        }
      }
    }
  }

  private void detectSlowBrokersFromPeers(Map<BrokerEntity, Double> currentValue,
                                          Set<BrokerEntity> detectedSlowBrokers) {
    if (isDataSufficient(currentValue.size(), PEER_METRIC_PERCENTILE_THRESHOLD, PEER_METRIC_PERCENTILE_THRESHOLD)) {
      double [] data = currentValue.values().stream().mapToDouble(i -> i).toArray();
      _percentile.setData(data);
      double base = _percentile.evaluate(PEER_METRIC_PERCENTILE_THRESHOLD);
      for (Map.Entry<BrokerEntity, Double> entry  : currentValue.entrySet()) {
        if (currentValue.get(entry.getKey()) > base * PEER_METRIC_MARGIN) {
          detectedSlowBrokers.add(entry.getKey());
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
    parameterConfigOverrides.put(METRIC_ANOMALY_DESCRIPTION_CONFIG, description);
    parameterConfigOverrides.put(METRIC_ANOMALY_BROKER_ENTITIES_CONFIG, detectedBrokers);
    parameterConfigOverrides.put(REMOVE_SLOW_BROKERS_CONFIG, removeSlowBroker);
    parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_CONFIG, _kafkaCruiseControl.timeMs());
    parameterConfigOverrides.put(METRIC_ANOMALY_FIXABLE_CONFIG, fixable);
    return _kafkaCruiseControl.config().getConfiguredInstance(KafkaCruiseControlConfig.METRIC_ANOMALY_CLASS_CONFIG,
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
    Set<MetricAnomaly<BrokerEntity>> detectedAnomalies = new HashSet<>();
    try {
      Set<BrokerEntity> detectedSlowBrokers = detectSlowBrokers(metricsHistoryByBroker, currentMetricsByBroker);

      Map<BrokerEntity, Long> brokersToDemote = new HashMap<>();
      Map<BrokerEntity, Long> brokersToRemove = new HashMap<>();
      for (BrokerEntity broker : detectedSlowBrokers) {
        // Update slow broker detection time and slowness score.
        Long currentTimeMs = _kafkaCruiseControl.timeMs();
        _detectedSlowBrokers.putIfAbsent(broker, currentTimeMs);
        Integer slownessScore = _brokerSlownessScore.compute(broker,
                                                             (k, v) -> (v == null) ? 1 : Math.min(v + 1, SLOW_BROKER_DECOMMISSION_SCORE));

        // Report anomaly if slowness score reaches threshold for broker decommission/demotion.
        if (slownessScore == SLOW_BROKER_DECOMMISSION_SCORE) {
          brokersToRemove.put(broker, _detectedSlowBrokers.get(broker));
        } else if (slownessScore == SLOW_BROKER_DEMOTION_SCORE) {
          brokersToDemote.put(broker, _detectedSlowBrokers.get(broker));
        }
      }

      // If too many brokers in the cluster are detected as slow brokers, report anomaly as not fixable.
      // Otherwise report anomaly with which broker to be removed/demoted.
      if (brokersToDemote.size() + brokersToRemove.size() > currentMetricsByBroker.size() * SELF_HEALING_UNFIXABLE_RATIO) {
        brokersToRemove.forEach(brokersToDemote::put);
        detectedAnomalies.add(createSlowBrokersAnomaly(brokersToDemote, false, false, getSlowBrokerDescription(brokersToDemote)));
      } else {
        if (!brokersToDemote.isEmpty()) {
          detectedAnomalies.add(createSlowBrokersAnomaly(brokersToDemote, true, false, getSlowBrokerDescription(brokersToDemote)));
        }
        if (!brokersToRemove.isEmpty()) {
          detectedAnomalies.add(createSlowBrokersAnomaly(brokersToRemove, _slowBrokerRemovalEnabled, true, getSlowBrokerDescription(brokersToRemove)));
        }
      }

      // For brokers which are previously detected as slow brokers, decrease their slowness score if their metrics has
      // recovered back to normal range. If the score reaches zero, remove its suspicion.
      for (Map.Entry<BrokerEntity, Integer> entry : _brokerSlownessScore.entrySet()) {
        BrokerEntity broker = entry.getKey();
        if (!detectedSlowBrokers.contains(broker)) {
          Integer score = _brokerSlownessScore.get(broker);
          if (score != null && --score == 0) {
            _brokerSlownessScore.remove(broker);
            _detectedSlowBrokers.remove(broker);
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Slow broker detector encountered exception: ", e);
    } finally {
      LOG.debug("Slow broker detection finished.");
    }
    return detectedAnomalies;
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
