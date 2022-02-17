/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCEmbeddedBroker;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.anomalyComparator;
import static org.easymock.EasyMock.anyLong;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.ANOMALY_DETECTOR_INITIAL_QUEUE_SIZE;


/**
 * Unit test for broker failure detector.
 */
@RunWith(Parameterized.class)
public class BrokerFailureDetectorTest extends CCKafkaIntegrationTestHarness {

  private final boolean _useKafkaApi;

  public BrokerFailureDetectorTest(boolean useKafkaApi) {
    this._useKafkaApi = useKafkaApi;
  }

  @Parameterized.Parameters
  public static Collection<Boolean> data() {
    return Arrays.asList(false, true);
  }

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
    Queue<Anomaly> anomalies = new PriorityBlockingQueue<>(ANOMALY_DETECTOR_INITIAL_QUEUE_SIZE, anomalyComparator());
    File failedBrokersFile = File.createTempFile("testFailureDetection", ".txt");
    BrokerFailureDetector detector = createBrokerFailureDetector(anomalies, mockTime, failedBrokersFile.getPath());
    try {
      String failedBrokerListString = detector.loadPersistedFailedBrokerList();
      assertTrue(failedBrokerListString.isEmpty());
      // Start detection.
      start(detector);
      assertTrue(anomalies.isEmpty());
      // Kill a broker and ensure an anomaly is detected
      int brokerId = 0;
      long anomalyTime = mockTime.milliseconds();
      killBroker(brokerId);
      long start = System.currentTimeMillis();
      detect(detector, anomalies, start, 0);
      assertEquals("One broker failure should have been detected before timeout.", 1, anomalies.size());
      failedBrokerListString = detector.loadPersistedFailedBrokerList();
      assertEquals(String.format("%d=%d", brokerId, anomalyTime), failedBrokerListString);
      Anomaly anomaly = anomalies.remove();
      assertThat(anomaly, instanceOf(BrokerFailures.class));
      BrokerFailures brokerFailures = (BrokerFailures) anomaly;
      assertEquals("The failed broker should be 0 and time should be 100L", Collections.singletonMap(brokerId, 100L),
                   brokerFailures.failedBrokers());
      assertFalse(brokerFailures.fixable());

      // Ensure that broker failure is detected as long as the broker is down.
      detector.detectBrokerFailures(false);
      assertEquals("One broker failure should have been detected before timeout.", 1, anomalies.size());
      // Bring the broker back
      restartDeadBroker(brokerId);
      detector.detectBrokerFailures(true);
      assertTrue(detector.failedBrokers().isEmpty());
      failedBrokerListString = detector.loadPersistedFailedBrokerList();
      assertTrue(failedBrokerListString.isEmpty());
      // Kill the 2nd broker
      killBroker(1);
      detect(detector, anomalies, start, 1);
      assertEquals("Two broker failure should have been detected before timeout.", 2, anomalies.size());
    } finally {
      detector.shutdown();
      Files.delete(failedBrokersFile.toPath());
    }
  }

  @Test
  public void testDetectorStartWithFailedBrokers() throws Exception {
    Time mockTime = getMockTime();
    Queue<Anomaly> anomalies = new PriorityBlockingQueue<>(ANOMALY_DETECTOR_INITIAL_QUEUE_SIZE, anomalyComparator());
    File failedBrokersFile = File.createTempFile("testDetectorStartWithFailedBrokers", ".txt");
    BrokerFailureDetector detector = createBrokerFailureDetector(anomalies, mockTime, failedBrokersFile.getPath());
    try {
      String failedBrokerListString = detector.loadPersistedFailedBrokerList();
      assertTrue(failedBrokerListString.isEmpty());
      int brokerId = 0;
      long anomalyTime = mockTime.milliseconds();
      killBroker(brokerId);
      // Start detection.
      start(detector);
      assertEquals(Collections.singletonMap(brokerId, 100L), detector.failedBrokers());
      failedBrokerListString = detector.loadPersistedFailedBrokerList();
      assertEquals(String.format("%d=%d", brokerId, anomalyTime), failedBrokerListString);
    } finally {
      detector.shutdown();
      Files.delete(failedBrokersFile.toPath());
    }
  }

  @Test
  public void testLoadFailedBrokersFromFile() throws Exception {
    Time mockTime = getMockTime();
    Queue<Anomaly> anomalies = new PriorityBlockingQueue<>(ANOMALY_DETECTOR_INITIAL_QUEUE_SIZE, anomalyComparator());
    File failedBrokersFile = File.createTempFile("testLoadFailedBrokersFromFile", ".txt");
    BrokerFailureDetector detector = createBrokerFailureDetector(anomalies, mockTime, failedBrokersFile.getPath());
    try {
      String failedBrokerListString = detector.loadPersistedFailedBrokerList();
      assertTrue(failedBrokerListString.isEmpty());
      // Start detection.
      start(detector);
      int brokerId = 0;
      long anomalyTime = mockTime.milliseconds();
      // kill a broker and ensure the anomaly is detected
      killBroker(brokerId);
      long start = System.currentTimeMillis();
      detect(detector, anomalies, start, 0);
      assertEquals(Collections.singletonMap(brokerId, anomalyTime), detector.failedBrokers());
      // shutdown, advance the clock and create a new detector.
      detector.shutdown();
      mockTime.sleep(100L);
      detector = createBrokerFailureDetector(anomalies, mockTime, failedBrokersFile.getPath());
      failedBrokerListString = detector.loadPersistedFailedBrokerList();
      assertEquals(String.format("%d=%d", brokerId, anomalyTime), failedBrokerListString);
      // start the newly created detector and the broker down time should remain previous time.
      detector.run();
      assertEquals(Collections.singletonMap(brokerId, anomalyTime), detector.failedBrokers());
      failedBrokerListString = detector.loadPersistedFailedBrokerList();
      assertEquals(String.format("%d=%d", brokerId, anomalyTime), failedBrokerListString);
    } finally {
      detector.shutdown();
      Files.delete(failedBrokersFile.toPath());
    }
  }

  private BrokerFailureDetector createBrokerFailureDetector(Queue<Anomaly> anomalies, Time time, String failedBrokersFilePath) {
    LoadMonitor mockLoadMonitor = EasyMock.mock(LoadMonitor.class);
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    EasyMock.expect(mockLoadMonitor.brokersWithReplicas(anyLong())).andAnswer(() -> new HashSet<>(Arrays.asList(0, 1))).anyTimes();
    EasyMock.replay(mockLoadMonitor);
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    if (_useKafkaApi) {
      props.setProperty(AnomalyDetectorConfig.KAFKA_BROKER_FAILURE_DETECTION_ENABLE_CONFIG, "true");
    } else {
      String s = zookeeper().connectionString();
      props.setProperty(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG, s);
      props.setProperty(ExecutorConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG, "false");
    }
    props.setProperty(AnomalyDetectorConfig.FAILED_BROKERS_FILE_PATH_CONFIG, failedBrokersFilePath);
    KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(props);
    if (_useKafkaApi) {
      Map<String, Object> adminClientConfigs = KafkaCruiseControlUtils.parseAdminClientConfigs(kafkaCruiseControlConfig);
      adminClientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
      AdminClient mockAdminClient = KafkaCruiseControlUtils.createAdminClient(adminClientConfigs);
      EasyMock.expect(mockKafkaCruiseControl.adminClient()).andReturn(mockAdminClient);
    }
    EasyMock.expect(mockKafkaCruiseControl.config()).andReturn(kafkaCruiseControlConfig).atLeastOnce();
    EasyMock.expect(mockKafkaCruiseControl.loadMonitor()).andReturn(mockLoadMonitor).atLeastOnce();
    EasyMock.expect(mockKafkaCruiseControl.timeMs()).andReturn(time.milliseconds()).atLeastOnce();
    EasyMock.replay(mockKafkaCruiseControl);
    return new BrokerFailureDetector(anomalies, mockKafkaCruiseControl);
  }

  private void killBroker(int index) {
    CCEmbeddedBroker broker = _brokers.get(index);
    broker.shutdown();
    broker.awaitShutdown();
  }

  private void restartDeadBroker(int index) {
    _brokers.get(index).startup();
  }

  private MockTime getMockTime() {
    return new MockTime(0, 100L, TimeUnit.NANOSECONDS.convert(100L, TimeUnit.MILLISECONDS));
  }

  private void start(BrokerFailureDetector detector) {
    if (_useKafkaApi) {
      detector.run();
    } else {
      detector.startDetection();
    }
  }

  private void detect(BrokerFailureDetector detector, Queue<Anomaly> anomalies, long start, int count) {
    if (_useKafkaApi) {
      detector.run();
    } else {
      while (anomalies.size() == count && System.currentTimeMillis() < start + 30000) {
        // wait for the anomalies to be drained.
      }
    }
  }
}
