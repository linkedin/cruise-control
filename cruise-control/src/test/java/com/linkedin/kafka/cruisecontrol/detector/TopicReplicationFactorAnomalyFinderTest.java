/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder.DESCRIBE_TOPIC_CONFIG_TIMEOUT_MS;
import static org.apache.kafka.common.config.TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TopicReplicationFactorAnomalyFinderTest {
  public static final short TARGET_TOPIC_REPLICATION_FACTOR = 2;
  public static final String TOPIC = "topic";
  public static final String HOST = "localhost";
  public static final String CLUSTER_ID = "cluster_id";

  @Test
  public void testAnomalyDetection() throws InterruptedException, ExecutionException, TimeoutException {
    KafkaCruiseControl mockKafkaCruiseControl = mockKafkaCruiseControl();
    AdminClient mockAdminClient = mockAdminClient((short) 1);
    TopicReplicationFactorAnomalyFinder anomalyFinder = new TopicReplicationFactorAnomalyFinder(mockKafkaCruiseControl,
                                                                                                TARGET_TOPIC_REPLICATION_FACTOR,
                                                                                                mockAdminClient);
    Set<TopicAnomaly> topicAnomalies = anomalyFinder.topicAnomalies();
    assertEquals(1, topicAnomalies.size());
    EasyMock.verify(mockKafkaCruiseControl, mockAdminClient);
  }

  @Test
  public void testSkipTopicWithLargeMinISR() throws InterruptedException, ExecutionException, TimeoutException {
    KafkaCruiseControl mockKafkaCruiseControl = mockKafkaCruiseControl();
    AdminClient mockAdminClient = mockAdminClient((short) 2);
    TopicReplicationFactorAnomalyFinder anomalyFinder = new TopicReplicationFactorAnomalyFinder(mockKafkaCruiseControl,
                                                                                                TARGET_TOPIC_REPLICATION_FACTOR,
                                                                                                mockAdminClient);
    Set<TopicAnomaly> topicAnomalies = anomalyFinder.topicAnomalies();
    assertTrue(topicAnomalies.isEmpty());
    EasyMock.verify(mockKafkaCruiseControl, mockAdminClient);
  }

  private AdminClient mockAdminClient(short expectedMinISR) throws InterruptedException, ExecutionException, TimeoutException {
    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);
    DescribeConfigsResult mockDescribeConfigsResult = EasyMock.mock(DescribeConfigsResult.class);
    KafkaFuture<Config> mockKafkaFuture = EasyMock.mock(KafkaFuture.class);
    Config config = new Config(Collections.singleton(new ConfigEntry(MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(expectedMinISR))));
    EasyMock.expect(mockAdminClient.describeConfigs(EasyMock.anyObject())).andReturn(mockDescribeConfigsResult);
    EasyMock.expect(mockDescribeConfigsResult.values())
            .andReturn(Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC), mockKafkaFuture));
    EasyMock.expect(mockKafkaFuture.get(EasyMock.eq(DESCRIBE_TOPIC_CONFIG_TIMEOUT_MS), EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(config);
    EasyMock.replay(mockAdminClient);
    EasyMock.replay(mockDescribeConfigsResult);
    EasyMock.replay(mockKafkaFuture);
    return mockAdminClient;
  }

  private KafkaCruiseControl mockKafkaCruiseControl() {
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    Cluster cluster = generateCluster();
    EasyMock.expect(mockKafkaCruiseControl.kafkaCluster()).andReturn(cluster).anyTimes();
    EasyMock.expect(mockKafkaCruiseControl.timeMs()).andReturn(System.currentTimeMillis()).anyTimes();
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(properties);
    EasyMock.expect(mockKafkaCruiseControl.config()).andReturn(config).anyTimes();
    EasyMock.replay(mockKafkaCruiseControl);
    return mockKafkaCruiseControl;
  }

  private Cluster generateCluster() {
    Node [] allNodes = new Node [3];
    IntStream.rangeClosed(0, 2).forEach(i -> allNodes[i] = new Node(i, HOST, 0));
    Set<PartitionInfo> partitionInfo = new HashSet<>(1);
    partitionInfo.add(new PartitionInfo(TOPIC, 0, allNodes[0], allNodes, allNodes));
    return new Cluster(CLUSTER_ID, Arrays.asList(allNodes), partitionInfo, Collections.emptySet(), Collections.emptySet());
  }
}
