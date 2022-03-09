/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.ZKConfigUtils;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import kafka.cluster.Broker;
import kafka.zk.BrokerIdsZNode;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Node;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.zookeeper.ZNodeChildChangeHandler;
import scala.jdk.javaapi.CollectionConverters;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.MAX_METADATA_WAIT_MS;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.io.FileUtils.readFileToString;
import static org.apache.commons.io.FileUtils.writeStringToFile;


/**
 * This class detects broker failures.
 */
public class BrokerFailureDetector extends AbstractAnomalyDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerFailureDetector.class);
  public static final String FAILED_BROKERS_OBJECT_CONFIG = "failed.brokers.object";
  // Config to indicate whether detected broker failures are fixable or not.
  public static final String BROKER_FAILURES_FIXABLE_CONFIG = "broker.failures.fixable.object";
  private static final String ZK_BROKER_FAILURE_METRIC_GROUP = "CruiseControlAnomaly";
  private static final String ZK_BROKER_FAILURE_METRIC_TYPE = "BrokerFailure";
  public static final long CLIENT_REQUEST_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
  private final boolean _useKafkaAPI;
  private final File _failedBrokersFile;
  private final Map<Integer, Long> _failedBrokers;
  private final short _fixableFailedBrokerCountThreshold;
  private final double _fixableFailedBrokerPercentageThreshold;
  private AdminClient _adminClient;
  private KafkaZkClient _kafkaZkClient;
  private Set<Integer> _aliveBrokers;
  private ExecutorService _detectionExecutor;

  public BrokerFailureDetector(Queue<Anomaly> anomalies, KafkaCruiseControl kafkaCruiseControl) {
    super(anomalies, kafkaCruiseControl);
    KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
    _useKafkaAPI = config.getBoolean(AnomalyDetectorConfig.KAFKA_BROKER_FAILURE_DETECTION_ENABLE_CONFIG);
    if (_useKafkaAPI) {
      _adminClient = kafkaCruiseControl.adminClient();
    } else {
      String zkUrl = config.getString(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG);
      boolean zkSecurityEnabled = config.getBoolean(ExecutorConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG);
      ZKClientConfig zkClientConfig = ZKConfigUtils.zkClientConfigFromKafkaConfig(config);
      _kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zkUrl, ZK_BROKER_FAILURE_METRIC_GROUP, ZK_BROKER_FAILURE_METRIC_TYPE,
              zkSecurityEnabled, zkClientConfig);
      _detectionExecutor = Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("BrokerFailureDetectorExecutor"));
    }
    _failedBrokers = new HashMap<>();
    _failedBrokersFile = new File(config.getString(AnomalyDetectorConfig.FAILED_BROKERS_FILE_PATH_CONFIG));
    _fixableFailedBrokerCountThreshold = config.getShort(AnomalyDetectorConfig.FIXABLE_FAILED_BROKER_COUNT_THRESHOLD_CONFIG);
    _fixableFailedBrokerPercentageThreshold = config.getDouble(AnomalyDetectorConfig.FIXABLE_FAILED_BROKER_PERCENTAGE_THRESHOLD_CONFIG);
    _aliveBrokers = new HashSet<>();
    // Load the failed broker information.
    String failedBrokerListString = loadPersistedFailedBrokerList();
    parsePersistedFailedBrokers(failedBrokerListString);
  }

  public boolean useKafkaAPI() {
    return _useKafkaAPI;
  }

  void startDetection() {
    // Detect broker failures.
    detectBrokerFailures(false);
    // Register ZNodeChildChangeHandler to ZK.
    _kafkaZkClient.registerZNodeChildChangeHandler(new BrokerFailureHandler());
    _kafkaZkClient.getChildren(BrokerIdsZNode.path());
  }

  @Override
  public void run() {
    detectBrokerFailures(false);
  }

  /**
   * Detect broker failures. Skip reporting if the failed brokers have not changed and skipReportingIfNotUpdated is true.
   *
   * @param skipReportingIfNotUpdated {@code true} if broker failure reporting will be skipped if failed brokers have not changed.
   */
  synchronized void detectBrokerFailures(boolean skipReportingIfNotUpdated) {
    try {
      _aliveBrokers = aliveBrokers();
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      LOG.error("Failed retrieving alive brokers", e);
      _aliveBrokers = Collections.emptySet();
    }
    // update the failed broker information based on the current state.
    boolean updated = updateFailedBrokers(_aliveBrokers);
    if (updated) {
      // persist the updated failed broker list.
      persistFailedBrokerList();
    }
    if (!skipReportingIfNotUpdated || updated) {
      // Report the failures to anomaly detector to handle.
      reportBrokerFailures();
    }
  }

  synchronized Map<Integer, Long> failedBrokers() {
    return new HashMap<>(_failedBrokers);
  }

  void shutdown() {
    if (!_useKafkaAPI) {
      _kafkaZkClient.unregisterZNodeChildChangeHandler(BrokerIdsZNode.path());
      _detectionExecutor.shutdown();
      try {
        _detectionExecutor.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Unable to shutdown BrokerFailureDetector normally, the active detection may be terminated.", e);
        if (!_detectionExecutor.isTerminated()) {
          _detectionExecutor.shutdownNow();
        }
      }
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(_kafkaZkClient);
    }
  }

  private void persistFailedBrokerList() {
    try {
      writeStringToFile(_failedBrokersFile, failedBrokerString(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      // let it go.
      LOG.error("Failed to persist the failed broker list.", e);
    }
  }

  /**
   * Package private for unit test.
   * @return Persisted failed broker list as a {@code String}, or an empty {@code String} if no previous failures have been persisted
   * in the file, or {@code null} if failed to load the failed broker list from the existing {@link #_failedBrokersFile}.
   */
  String loadPersistedFailedBrokerList() {
    String failedBrokerListString = null;
    try {
      failedBrokerListString = readFileToString(_failedBrokersFile, StandardCharsets.UTF_8);
    } catch (FileNotFoundException fnfe) {
      // This means no previous failures have ever been persisted in the file.
      failedBrokerListString = "";
    } catch (IOException ioe) {
      // let it go.
      LOG.error("Failed to load the failed broker list.", ioe);
    }
    return failedBrokerListString;
  }

  /**
   * If {@link #_failedBrokers} has changed, update it.
   *
   * @param aliveBrokers Alive brokers in the cluster.
   * @return {@code true} if {@link #_failedBrokers} has been updated, {@code false} otherwise.
   */
  private boolean updateFailedBrokers(Set<Integer> aliveBrokers) {
    // We get the complete broker list from metadata. i.e. any broker that still has a partition assigned to it is
    // included in the broker list. If we cannot update metadata in MAX_METADATA_WAIT_MS, skip
    Set<Integer> currentFailedBrokers = _kafkaCruiseControl.loadMonitor().brokersWithReplicas(MAX_METADATA_WAIT_MS);
    currentFailedBrokers.removeAll(aliveBrokers);
    LOG.debug("Brokers (alive: {}, failed: {}).", aliveBrokers, currentFailedBrokers);
    // Remove broker that is no longer failed.
    boolean updated = _failedBrokers.entrySet().removeIf(entry -> !currentFailedBrokers.contains(entry.getKey()));
    // Add broker that has just failed.
    for (Integer brokerId : currentFailedBrokers) {
      if (_failedBrokers.putIfAbsent(brokerId, _kafkaCruiseControl.timeMs()) == null) {
        updated = true;
      }
    }
    return updated;
  }

  private Set<Integer> aliveBrokers() throws ExecutionException, InterruptedException, TimeoutException {
    if (_useKafkaAPI) {
      return _adminClient.describeCluster().nodes().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)
              .stream().map(Node::id).collect(toSet());
    } else {
      return CollectionConverters.asJava(_kafkaZkClient.getAllBrokersInCluster())
              .stream().map(Broker::id).collect(toSet());
    }
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

  private boolean tooManyFailedBrokers(int failedBrokerCount, int aliveBrokerCount) {
    return failedBrokerCount > _fixableFailedBrokerCountThreshold
           || (double) failedBrokerCount / (failedBrokerCount + aliveBrokerCount) > _fixableFailedBrokerPercentageThreshold;
  }

  private void reportBrokerFailures() {
    if (!_failedBrokers.isEmpty()) {
      Map<String, Object> parameterConfigOverrides = new HashMap<>();
      parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl);
      Map<Integer, Long> failedBrokers = failedBrokers();
      parameterConfigOverrides.put(FAILED_BROKERS_OBJECT_CONFIG, failedBrokers);
      parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, _kafkaCruiseControl.timeMs());
      parameterConfigOverrides.put(BROKER_FAILURES_FIXABLE_CONFIG,
                                   !tooManyFailedBrokers(failedBrokers.size(), _aliveBrokers.size()));

      BrokerFailures brokerFailures = _kafkaCruiseControl.config().getConfiguredInstance(AnomalyDetectorConfig.BROKER_FAILURES_CLASS_CONFIG,
                                                                                         BrokerFailures.class,
                                                                                         parameterConfigOverrides);
      _anomalies.add(brokerFailures);
    }
  }

  /**
   * The zookeeper handler for failure detection.
   */
  private final class BrokerFailureHandler implements ZNodeChildChangeHandler {

    @Override
    public String path() {
      return BrokerIdsZNode.path();
    }

    @Override
    public void handleChildChange() {
      // Ensure that broker failures are not reported if there are no updates in already known failed brokers.
      // Anomaly Detector guarantees that a broker failure detection will not be lost. Skipping reporting if not updated
      // ensures that the broker failure detector will not report superfluous broker failures due to flaky zNode.
      _detectionExecutor.submit(() -> detectBrokerFailures(true));
    }
  }

}
