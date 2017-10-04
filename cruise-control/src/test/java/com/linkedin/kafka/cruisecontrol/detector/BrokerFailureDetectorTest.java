/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.clients.utils.tests.AbstractKafkaIntegrationTestHarness;
import com.linkedin.kafka.clients.utils.tests.EmbeddedBroker;
import com.linkedin.kafka.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import kafka.utils.MockTime;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.anyLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for broker failure detector.
 */
public class BrokerFailureDetectorTest extends AbstractKafkaIntegrationTestHarness {

  @Override
  public int clusterSize() {
    return 2;
  }

  @Before
  public void setUp() {
    super.setUp();
  }

  @After
  public void tearDown() {
    super.tearDown();
  }

  @Test
  public void testFailureDetection() {
    Time mockTime = new MockTime(100L);
    Queue<Anomaly> anomalies = new ConcurrentLinkedQueue<>();
    BrokerFailureDetector detector = createBrokerFailureDetector(anomalies, mockTime);

    try {
      // Start detection.
      detector.startDetection();
      assertTrue(anomalies.isEmpty());
      int brokerId = killRandomBroker();
      long start = System.currentTimeMillis();
      while (anomalies.isEmpty() && System.currentTimeMillis() < start + 30000) {
        // wait for the anomalies to be drained.
      }
      assertEquals("One broker failure should have been detected before timeout.", 1, anomalies.size());
      Anomaly anomaly = anomalies.remove();
      assertTrue("The anomaly should be BrokerFailure", anomaly instanceof BrokerFailures);
      BrokerFailures brokerFailures = (BrokerFailures) anomaly;
      assertEquals("The failed broker should be 0 and time should be 100L", Collections.singletonMap(brokerId, 100L),
                   brokerFailures.failedBrokers());

      // Bring the broker back
      System.out.println("Starting brokers.");
      restartDeadBrokers();
      detector.detectBrokerFailures();
      assertTrue(detector.failedBrokers().isEmpty());
    } finally {
      detector.shutdown();
    }
  }

  @Test
  public void testDetectorStartWithFailedBrokers() {
    Time mockTime = new MockTime(100L);
    Queue<Anomaly> anomalies = new ConcurrentLinkedQueue<>();
    BrokerFailureDetector detector = createBrokerFailureDetector(anomalies, mockTime);

    try {
      int brokerId = killRandomBroker();
      detector.startDetection();
      assertEquals(Collections.singletonMap(brokerId, 100L), detector.failedBrokers());
    } finally {
      detector.shutdown();
    }
  }

  @Test
  public void testLoadFailedBrokersFromZK() {
    Time mockTime = new MockTime(100L);
    Queue<Anomaly> anomalies = new ConcurrentLinkedQueue<>();
    BrokerFailureDetector detector = createBrokerFailureDetector(anomalies, mockTime);

    try {
      detector.startDetection();
      int brokerId = killRandomBroker();
      long start = System.currentTimeMillis();
      while (anomalies.isEmpty() && System.currentTimeMillis() < start + 30000) {
        // Wait for the anomalies to be drained.
      }
      assertEquals(Collections.singletonMap(brokerId, 100L), detector.failedBrokers());
      // shutdown, advance the clock and create a new detector.
      detector.shutdown();
      mockTime.sleep(100L);
      detector = createBrokerFailureDetector(anomalies, mockTime);
      // start the newly created detector and the broker down time should remain previous time.
      detector.startDetection();
      assertEquals(Collections.singletonMap(brokerId, 100L), detector.failedBrokers());
    } finally {
      detector.shutdown();
    }
  }

  private BrokerFailureDetector createBrokerFailureDetector(Queue<Anomaly> anomalies, Time time) {
    LoadMonitor mockLoadMonitor = EasyMock.mock(LoadMonitor.class);
    EasyMock.expect(mockLoadMonitor.brokersWithPartitions(anyLong())).andAnswer(() -> new HashSet<>(Arrays.asList(0, 1))).anyTimes();
    EasyMock.replay(mockLoadMonitor);
    Properties props = CruiseControlUnitTestUtils.getCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper().getConnectionString());
    KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(props);
    return new BrokerFailureDetector(kafkaCruiseControlConfig,
                                     mockLoadMonitor,
                                     anomalies,
                                     time);
  }

  private ZkUtils zkUtils() {
    ZkConnection zkConnection = new ZkConnection(zookeeper().getConnectionString());
    ZkClient zkClient = new ZkClient(zkConnection);
    return new ZkUtils(zkClient, zkConnection, false);
  }


  private void killBroker(int index) throws Exception {
    _brokers.get(Integer.toString(index)).close();
  }

  private void restartDeadBrokers(int index) {

  }
}
