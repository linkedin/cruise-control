/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaIntegrationTestHarness;
import com.linkedin.kafka.clients.utils.tests.EmbeddedBroker;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.MockTime;
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
  public void testFailureDetection() throws Exception {
    Time mockTime = getMockTime();
    Queue<Anomaly> anomalies = new ConcurrentLinkedQueue<>();
    BrokerFailureDetector detector = createBrokerFailureDetector(anomalies, mockTime);
    try {
      // Start detection.
      detector.startDetection();
      assertTrue(anomalies.isEmpty());
      int brokerId = 0;
      killBroker(brokerId);
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

      // Ensure that broker failure is detected as long as the broker is down.
      detector.detectBrokerFailures();
      assertEquals("One broker failure should have been detected before timeout.", 1, anomalies.size());
      // Bring the broker back
      restartDeadBroker(brokerId);
      detector.detectBrokerFailures();
      assertTrue(detector.failedBrokers().isEmpty());
    } finally {
      detector.shutdown();
    }
  }

  @Test
  public void testDetectorStartWithFailedBrokers() throws Exception {
    Time mockTime = getMockTime();
    Queue<Anomaly> anomalies = new ConcurrentLinkedQueue<>();
    BrokerFailureDetector detector = createBrokerFailureDetector(anomalies, mockTime);

    try {
      int brokerId = 0;
      killBroker(brokerId);
      detector.startDetection();
      assertEquals(Collections.singletonMap(brokerId, 100L), detector.failedBrokers());
    } finally {
      detector.shutdown();
    }
  }

  @Test
  public void testLoadFailedBrokersFromZK() throws Exception {
    Time mockTime = getMockTime();
    Queue<Anomaly> anomalies = new ConcurrentLinkedQueue<>();
    BrokerFailureDetector detector = createBrokerFailureDetector(anomalies, mockTime);

    try {
      detector.startDetection();
      int brokerId = 0;
      killBroker(brokerId);
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
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    EasyMock.expect(mockLoadMonitor.brokersWithReplicas(anyLong())).andAnswer(() -> new HashSet<>(Arrays.asList(0, 1))).anyTimes();
    EasyMock.replay(mockLoadMonitor);
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper().getConnectionString());
    props.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG, "false");
    KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(props);
    return new BrokerFailureDetector(kafkaCruiseControlConfig,
                                     mockLoadMonitor,
                                     anomalies,
                                     time,
                                     mockKafkaCruiseControl,
                                     Collections.emptyList());
  }

  private void killBroker(int index) throws Exception {
    EmbeddedBroker broker = _brokers.get(index);
    broker.shutdown();
    broker.awaitShutdown();
  }

  private void restartDeadBroker(int index) throws Exception {
    _brokers.get(index).startup();
  }

  private MockTime getMockTime() {
    return new MockTime(0, 100L, TimeUnit.NANOSECONDS.convert(100L, TimeUnit.MILLISECONDS));
  }
}
