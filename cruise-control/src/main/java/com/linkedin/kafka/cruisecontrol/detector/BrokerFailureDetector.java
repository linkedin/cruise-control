/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import static java.util.stream.Collectors.toSet;


/**
 * This class detects broker failures.
 *
 */
public class BrokerFailureDetector {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerFailureDetector.class);
  private static final long MAX_METADATA_WAIT_MS = 60000L;
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final String _failedBrokersZkPath;
  private final ZkClient _zkClient;
  private final ZkUtils _zkUtils;
  private final Map<Integer, Long> _failedBrokers;
  private final LoadMonitor _loadMonitor;
  private final Queue<Anomaly> _anomalies;
  private final Time _time;
  private final boolean _allowCapacityEstimation;

  public BrokerFailureDetector(KafkaCruiseControlConfig config,
                               LoadMonitor loadMonitor,
                               Queue<Anomaly> anomalies,
                               Time time,
                               KafkaCruiseControl kafkaCruiseControl) {
    String zkUrl = config.getString(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG);
    ZkConnection zkConnection = new ZkConnection(zkUrl, 30000);
    _zkClient = new ZkClient(zkConnection, 30000, new ZkStringSerializer());
    // Do not support secure ZK at this point.
    _zkUtils = new ZkUtils(_zkClient, zkConnection, false);
    _failedBrokers = new HashMap<>();
    _failedBrokersZkPath = config.getString(KafkaCruiseControlConfig.FAILED_BROKERS_ZK_PATH_CONFIG);
    _loadMonitor = loadMonitor;
    _anomalies = anomalies;
    _time = time;
    _kafkaCruiseControl = kafkaCruiseControl;
    _allowCapacityEstimation = config.getBoolean(KafkaCruiseControlConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
  }

  void startDetection() {
    try {
      _zkClient.createPersistent(_failedBrokersZkPath);
    } catch (ZkNodeExistsException znee) {
      // let it go.
    }
    // Load the failed broker information from zookeeper.
    loadPersistedFailedBrokerList();
    // Detect broker failures.
    detectBrokerFailures();
    _zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath(), new BrokerFailureListener());
  }

  synchronized void detectBrokerFailures() {
    // update the failed broker information based on the current state.
    updateFailedBrokerList(aliveBrokers());
    // persist the updated failed broker list.
    persistFailedBrokerList();
    // Report the failures to anomaly detector to handle.
    reportBrokerFailures();
  }

  Map<Integer, Long> failedBrokers() {
    return new HashMap<>(_failedBrokers);
  }

  void shutdown() {
    _zkClient.close();
  }

  private void persistFailedBrokerList() {
    _zkClient.writeData(_failedBrokersZkPath, failedBrokerString());
  }

  private void loadPersistedFailedBrokerList() {
    String failedBrokerListString = _zkClient.readData(_failedBrokersZkPath);
    parsePersistedFailedBrokers(failedBrokerListString);
  }

  private void updateFailedBrokerList(Set<Integer> aliveBrokers) {
    // We get the complete broker list from metadata. i.e. any broker that still has a partition assigned to it is
    // included in the broker list. If we cannot update metadata in 60 seconds, skip
    Set<Integer> currentFailedBrokers = _loadMonitor.brokersWithPartitions(MAX_METADATA_WAIT_MS);
    currentFailedBrokers.removeAll(aliveBrokers);
    LOG.debug("Alive brokers: {}, failed brokers: {}", aliveBrokers, currentFailedBrokers);
    // Remove broker that is no longer failed.
    _failedBrokers.entrySet().removeIf(entry -> !currentFailedBrokers.contains(entry.getKey()));
    // Add broker that has just failed.
    for (Integer brokerId : currentFailedBrokers) {
      _failedBrokers.putIfAbsent(brokerId, _time.milliseconds());
    }
  }

  private Set<Integer> aliveBrokers() {
    // We get the alive brokers from ZK directly.
    return JavaConversions.asJavaCollection(_zkUtils.getAllBrokersInCluster())
                          .stream().map(Broker::id).collect(toSet());
  }

  private String failedBrokerString() {
    StringBuilder sb = new StringBuilder();
    for (Iterator<Map.Entry<Integer, Long>> iter = _failedBrokers.entrySet().iterator(); iter.hasNext(); ) {
      Map.Entry<Integer, Long> entry = iter.next();
      sb.append(entry.getKey()).append("=").append(entry.getValue());
      if (iter.hasNext()) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  private void parsePersistedFailedBrokers(String failedBrokerListString) {
    if (failedBrokerListString != null && !failedBrokerListString.isEmpty()) {
      String[] entries = failedBrokerListString.split(",");
      for (String entry : entries) {
        String[] idAndTimestamp = entry.split("=");
        if (idAndTimestamp.length != 2) {
          throw new IllegalStateException(
              "The persisted failed broker string cannot be parsed. The string is " + failedBrokerListString);
        }
        _failedBrokers.putIfAbsent(Integer.parseInt(idAndTimestamp[0]), Long.parseLong(idAndTimestamp[1]));
      }
    }
  }

  private void reportBrokerFailures() {
    if (!_failedBrokers.isEmpty()) {
      Map<Integer, Long> failedBrokers = new HashMap<>(_failedBrokers);
      _anomalies.add(new BrokerFailures(_kafkaCruiseControl, failedBrokers, _allowCapacityEstimation));
    }
  }

  /**
   * The zookeeper listener for failure detection.
   */
  private class BrokerFailureListener implements IZkChildListener {

    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) {
      updateFailedBrokerList(currentChildren.stream().map(Integer::parseInt).collect(toSet()));
      persistFailedBrokerList();
      reportBrokerFailures();
    }
  }

  /**
   * Serde class for ZkClient.
   */
  private static class ZkStringSerializer implements ZkSerializer {
    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
      return ((String) data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

}
