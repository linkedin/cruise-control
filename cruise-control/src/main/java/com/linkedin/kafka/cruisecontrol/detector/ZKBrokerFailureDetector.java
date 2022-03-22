/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.ZKConfigUtils;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import kafka.cluster.Broker;
import kafka.zk.BrokerIdsZNode;
import kafka.zk.KafkaZkClient;
import org.apache.zookeeper.client.ZKClientConfig;
import kafka.zookeeper.ZNodeChildChangeHandler;
import scala.jdk.javaapi.CollectionConverters;

import static java.util.stream.Collectors.toSet;


/**
 * This class detects broker failures via ZooKeeper
 */
public class ZKBrokerFailureDetector extends AbstractBrokerFailureDetector {
  private static final String ZK_BROKER_FAILURE_METRIC_GROUP = "CruiseControlAnomaly";
  private static final String ZK_BROKER_FAILURE_METRIC_TYPE = "BrokerFailure";

  private final KafkaZkClient _kafkaZkClient;
  private final ExecutorService _detectionExecutor;
  private boolean _started = false;

  public ZKBrokerFailureDetector(Queue<Anomaly> anomalies, KafkaCruiseControl kafkaCruiseControl) {
    super(anomalies, kafkaCruiseControl);
    KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
    String zkUrl = config.getString(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG);
    boolean zkSecurityEnabled = config.getBoolean(ExecutorConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG);
    ZKClientConfig zkClientConfig = ZKConfigUtils.zkClientConfigFromKafkaConfig(config);
    _kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zkUrl, ZK_BROKER_FAILURE_METRIC_GROUP, ZK_BROKER_FAILURE_METRIC_TYPE,
                                                                 zkSecurityEnabled, zkClientConfig);
    _detectionExecutor = Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("BrokerFailureDetectorExecutor"));
  }

  /**
   * On the first invocation, start broker failure detection. Load already persisted failed brokers and detect new broker failures. After
   * that register a ZNodeChildChangeHandler to be able to run detection automatically when ZK event happened.
   *
   * ZkClient needs the registerZNodeChildChangeHandler and getChildren methods to register
   * a ZNodeChildChangeHandler as the ZK docs said.
   */
  @Override
  public void run() {
    synchronized (this) {
      if (!_started) {
        // Load the failed broker information from zookeeper.
        String failedBrokerListString = loadPersistedFailedBrokerList();
        parsePersistedFailedBrokers(failedBrokerListString);
        // Detect broker failures.
        detectBrokerFailures(false);
        // Register ZNodeChildChangeHandler to ZK.
        _kafkaZkClient.registerZNodeChildChangeHandler(new BrokerFailureHandler());
        _kafkaZkClient.getChildren(BrokerIdsZNode.path());
        _started = true;
      }
    }
  }

  @Override
  public void shutdown() {
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

  @Override
  Set<Integer> aliveBrokers() {
    // We get the alive brokers from ZK directly.
    return CollectionConverters.asJava(_kafkaZkClient.getAllBrokersInCluster()).stream().map(Broker::id).collect(toSet());
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
