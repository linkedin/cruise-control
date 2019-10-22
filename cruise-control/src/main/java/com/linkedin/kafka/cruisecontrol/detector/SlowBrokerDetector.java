/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.utils.Time;

import static com.linkedin.cruisecontrol.detector.metricanomaly.MetricAnomalyFinderUtils.isDataSufficient;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.MAX_METADATA_WAIT_MS;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.shouldSkipAnomalyDetection;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.ALL_TOPIC_BYTES_IN;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.ALL_TOPIC_REPLICATION_BYTES_IN;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_LOG_FLUSH_TIME_MS_999TH;

/**
 * This class will be scheduled to periodically check if there is any slow broker.
 *
 * Slow brokers are detected by calculating a derived broker metric
 * {@code BROKER_LOG_FLUSH_TIME_MS_999TH / (ALL_TOPIC_BYTES_IN + ALL_TOPIC_REPLICATION_BYTES_IN) for each broker and then
 * checking in two ways.
 * <ul>
 *   <li>Comparing the latest against broker's own history.</li>
 *   <li>Comparing the latest against the last of rest brokers in cluster.</li>
 * </ul>
 *
 * If certain broker's metric value is abnormally high(out of the normal range based on historical value/cluster average),
 * it is marked as a slow broker suspect by the detector. Then if this suspect broker's derived metric anomaly persists
 * for some time, it is confirmed to be a slow broker and the detector will report a {@link SlowBrokers}} anomaly
 * with broker demotion as self-healing proposal. If the metric anomaly still persists for an extended time, the detector
 * will eventually report another {@link SlowBrokers}} anomaly with broker removal as self-healing proposal.
 *
 * Note if there are too many brokers confirmed as slow broker in the same run, the detector will report the {@link SlowBrokers}
 * anomaly as unfixable. Because this often indicates some serious issue in the cluster and probably requires administrator's
 * intervention to decide the right remediation strategy.
 */
public class SlowBrokerDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(SlowBrokerDetector.class);
  public static final String SLOW_BROKERS_OBJECT_CONFIG = "slow.brokers.object";
  public static final String REMOVE_SLOW_BROKERS_CONFIG = "remove.slow.brokers";
  public static final String SLOW_BROKERS_FIXABLE_CONFIG = "slow.brokers.fixable";
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
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final LoadMonitor _loadMonitor;
  private final Queue<KafkaAnomaly> _anomalies;
  private final Time _time;
  private final Map<Integer, Integer> _brokerSlownessScore;
  private final Map<Integer, Long> _detectedSlowBrokers;
  private final Percentile _percentile;

  public SlowBrokerDetector(LoadMonitor loadMonitor,
                            KafkaCruiseControl kafkaCruiseControl,
                            Time time,
                            Queue<KafkaAnomaly> anomalies) {
    _loadMonitor = loadMonitor;
    _kafkaCruiseControl = kafkaCruiseControl;
    _anomalies = anomalies;
    _time = time;
    _brokerSlownessScore = new HashMap<>();
    _detectedSlowBrokers = new HashMap<>();
    _percentile = new Percentile();
  }

  /**
   * Skip slow broker detection if any of the following is true:
   * <ul>
   *  <li>There is offline replicas in the cluster, which means there is dead brokers/disks. In this case
   * {@link BrokerFailureDetector} or {@link DiskFailureDetector} should take care of the anomaly.</li>
   *  <li>{@link AnomalyDetectorUtils#shouldSkipAnomalyDetection(LoadMonitor, KafkaCruiseControl)} returns true.
   * </ul>
   *
   * @return True to skip slow broker detection based on the current state, false otherwise.
   */
  private boolean shouldSkipSlowBrokerDetection() {
    Set<Integer> brokersWithOfflineReplicas = _loadMonitor.brokersWithOfflineReplicas(MAX_METADATA_WAIT_MS);
    if (!brokersWithOfflineReplicas.isEmpty()) {
      LOG.info("Skipping goal violation detection because there are dead brokers/disks in the cluster, flawed brokers: {}",
               brokersWithOfflineReplicas);
      return true;
    }
    return shouldSkipAnomalyDetection(_loadMonitor, _kafkaCruiseControl);
  }

  private Set<Integer> detectSlowBrokers(Map<BrokerEntity, ValuesAndExtrapolations> metricsHistoryByBroker,
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

    Set<Integer> detectedSlowBrokers = new HashSet<>();
    // Detect slow broker by comparing each broker's current derived metric value against historical value.
    detectSlowBrokersFromHistory(historyValueByBroker, currentValueByBroker, detectedSlowBrokers);
    // Detect slow broker by comparing each broker's derived metric value against its peers' value.
    detectSlowBrokersFromPeers(currentValueByBroker, detectedSlowBrokers);
    return detectedSlowBrokers;
  }

  private void detectSlowBrokersFromHistory(Map<BrokerEntity, List<Double>> historyValue,
                                            Map<BrokerEntity, Double> currentValue,
                                            Set<Integer> detectedSlowBrokers) {
    for (Map.Entry<BrokerEntity, Double> entry : currentValue.entrySet()) {
      BrokerEntity entity = entry.getKey();
      if (isDataSufficient(historyValue.size(), HISTORY_METRIC_PERCENTILE_THRESHOLD, HISTORY_METRIC_PERCENTILE_THRESHOLD)) {
        double [] data = historyValue.get(entity).stream().mapToDouble(i -> i).toArray();
        _percentile.setData(data);
        if (currentValue.get(entity) > _percentile.evaluate(HISTORY_METRIC_PERCENTILE_THRESHOLD) * HISTORY_METRIC_MARGIN) {
          detectedSlowBrokers.add(entity.brokerId());
        }
      }
    }
  }

  private void detectSlowBrokersFromPeers(Map<BrokerEntity, Double> currentValue, Set<Integer> detectedSlowBrokers) {
    if (isDataSufficient(currentValue.size(), PEER_METRIC_PERCENTILE_THRESHOLD, PEER_METRIC_PERCENTILE_THRESHOLD)) {
      double [] data = currentValue.values().stream().mapToDouble(i -> i).toArray();
      _percentile.setData(data);
      double base = _percentile.evaluate(PEER_METRIC_PERCENTILE_THRESHOLD);
      for (Map.Entry<BrokerEntity, Double> entry  : currentValue.entrySet()) {
        if (currentValue.get(entry.getKey()) > base * PEER_METRIC_MARGIN) {
          detectedSlowBrokers.add(entry.getKey().brokerId());
        }
      }
    }
  }

  private SlowBrokers createSlowBrokersAnomaly(Map<Integer, Long> detectedBrokers,
                                               boolean removeSlowBroker,
                                               boolean fixable) {
    Map<String, Object> parameterConfigOverrides = new HashMap<>(5);
    parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl);
    parameterConfigOverrides.put(SlowBrokerDetector.SLOW_BROKERS_OBJECT_CONFIG, detectedBrokers);
    parameterConfigOverrides.put(SlowBrokerDetector.REMOVE_SLOW_BROKERS_CONFIG, removeSlowBroker);
    parameterConfigOverrides.put(SlowBrokerDetector.SLOW_BROKERS_FIXABLE_CONFIG, fixable);
    parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_CONFIG, _time.milliseconds());
    return _kafkaCruiseControl.config().getConfiguredInstance(KafkaCruiseControlConfig.SLOW_BROKERS_CLASS_CONFIG,
                                                              SlowBrokers.class,
                                                              parameterConfigOverrides);

  }

  @Override
  public void run() {
    try {
      if (shouldSkipSlowBrokerDetection()) {
        return;
      }

      Map<BrokerEntity, ValuesAndExtrapolations> metricsHistoryByBroker = _loadMonitor.brokerMetrics().valuesAndExtrapolations();
      Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker = _loadMonitor.currentBrokerMetricValues();
      Set<Integer> detectedSlowBrokers = detectSlowBrokers(metricsHistoryByBroker, currentMetricsByBroker);

      Map<Integer, Long> brokersToDemote = new HashMap<>();
      Map<Integer, Long> brokersToRemove = new HashMap<>();
      for (Integer brokerId : detectedSlowBrokers) {
        // Update slow broker detection time and slowness score.
        Long currentTimeMs = _time.milliseconds();
        _detectedSlowBrokers.putIfAbsent(brokerId, currentTimeMs);
        Integer slownessScore = _brokerSlownessScore.compute(brokerId, (k, v) -> (v == null) ? 1 : v + 1);

        // Report anomaly if slowness score reaches threshold for broker decommission/demotion.
        if (slownessScore == SLOW_BROKER_DECOMMISSION_SCORE) {
          brokersToRemove.put(brokerId, _detectedSlowBrokers.get(brokerId));
          _detectedSlowBrokers.remove(brokerId);
          _brokerSlownessScore.remove(brokerId);
        } else if (slownessScore == SLOW_BROKER_DEMOTION_SCORE) {
          brokersToDemote.put(brokerId, _detectedSlowBrokers.get(brokerId));
        }
      }

      // If too many brokers in the cluster are detected as slow brokers, report anomaly as not fixable.
      // Otherwise report anomaly with which broker to be removed/demoted.
      if (brokersToDemote.size() + brokersToRemove.size() > currentMetricsByBroker.size() * SELF_HEALING_UNFIXABLE_RATIO) {
        brokersToRemove.forEach(brokersToDemote::put);
        _anomalies.add(createSlowBrokersAnomaly(brokersToDemote, false, false));
      } else {
        if (!brokersToDemote.isEmpty()) {
          _anomalies.add(createSlowBrokersAnomaly(brokersToDemote, false, true));
        }
        if (!brokersToRemove.isEmpty()) {
          _anomalies.add(createSlowBrokersAnomaly(brokersToRemove, true, true));
        }
      }

      // For brokers which are previously detected as slow brokers, decrease their slowness score if their metrics has
      // recovered back to normal range. If the score reaches zero, remove its suspicion.
      for (Map.Entry<Integer, Integer> entry : _brokerSlownessScore.entrySet()) {
        Integer brokerId = entry.getKey();
        if (!detectedSlowBrokers.contains(brokerId)) {
          Integer score = _brokerSlownessScore.get(brokerId);
          if (score != null && --score == 0) {
            _brokerSlownessScore.remove(brokerId);
            _detectedSlowBrokers.remove(brokerId);
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Slow broker detector encountered exception: ", e);
    } finally {
      LOG.debug("Slow broker detection finished.");
    }
  }
}
