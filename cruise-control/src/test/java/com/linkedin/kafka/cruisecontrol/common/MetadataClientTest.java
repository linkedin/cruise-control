/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.ComparisonCriteria;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MetadataClientTest {
  private final Node _controllerNode = new Node(0, "host0", 100);
  private final Node _simpleNode = new Node(1, "host1", 100);
  private final List<Node> _nodes = Arrays.asList(_controllerNode, _simpleNode);
  private final String _clusterId = "cluster-1";
  private final int _p0 = 0;
  private final int _p1 = 1;
  private final int _p2 = 2;
  private final TopicPartition _t0P0 = new TopicPartition(TOPIC0, _p0);
  private final TopicPartition _t0P1 = new TopicPartition(TOPIC0, _p1);
  private final TopicPartition _t0P2 = new TopicPartition(TOPIC0, _p2);

  // t0p2 has offline replicas
  private final TopicPartitionInfo _t0P2TopicPartitionInfo = new TopicPartitionInfo(
      _t0P2.partition(), _controllerNode, _nodes, Collections.singletonList(_controllerNode));

  private final TopicDescription _topic0TopicDescription =
      new TopicDescription(TOPIC0, false, Arrays.asList(
          defaultTopicPartitionInfo(_t0P0),
          defaultTopicPartitionInfo(_t0P1),
          _t0P2TopicPartitionInfo
      ));
  private final PartitionInfo _t0P0PartitionInfo = partitionInfo(_t0P0, defaultTopicPartitionInfo(_t0P0));
  private final PartitionInfo _t0P1PartitionInfo = partitionInfo(_t0P1, defaultTopicPartitionInfo(_t0P1));
  private final PartitionInfo _t0P2PartitionInfo = partitionInfo(_t0P2,
      new TopicPartitionInfo(_t0P2.partition(), _controllerNode, _nodes,
          Collections.singletonList(_controllerNode)));

  private final KafkaCruiseControlConfig _cruiseControlConfig = new KafkaCruiseControlConfig(
      KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()
    );
  private final long _metadataTTL = 100L;

  private Time _mockTime = null;
  private AdminClient _mockAdminClient = null;
  private MetadataClient _metadataClient = null;

  /**
   * Setup the unit test
   */
  @Before
  public void setUp() {
    _mockTime = mockTime();
    _mockAdminClient = EasyMock.mock(AdminClient.class);
    _metadataClient = new MetadataClient(_cruiseControlConfig, _metadataTTL, _mockTime, _mockAdminClient);

    assertEquals(0, _metadataClient._version);
    assertEquals(0, _metadataClient.clusterAndGeneration().generation());
    assertEquals(Cluster.empty(), _metadataClient.clusterAndGeneration().cluster());
  }

  private TopicPartitionInfo defaultTopicPartitionInfo(TopicPartition tp) {
    return new TopicPartitionInfo(tp.partition(), _controllerNode, _nodes, _nodes);
  }

  private PartitionInfo partitionInfo(TopicPartition tp, TopicPartitionInfo tpInfo) {
    return new PartitionInfo(tp.topic(), tpInfo.partition(),
        tpInfo.leader(), tpInfo.replicas().toArray(new Node[0]),
        tpInfo.isr().toArray(new Node[0]),
        tpInfo.replicas()
              .stream()
              .filter(r -> !tpInfo.isr().contains(r))
              .toArray(Node[]::new));
  }

  @Test
  public void testUpdatesMetadataAfterExpectedTime() throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
    int metadataTimeoutMs = 150;
    // Arrange mocks
    Collection<String> topics = Collections.singletonList(TOPIC0);
    Map<String, TopicDescription> topicDescriptions = new HashMap<>();
    topicDescriptions.put(TOPIC0, _topic0TopicDescription);
    replayMockAdminClient(_mockAdminClient, topics, topicDescriptions, metadataTimeoutMs);

    _mockTime.sleep(_metadataTTL - 1);
    _metadataClient.refreshMetadata(metadataTimeoutMs);
    // Should not have updated yet as metadataTTL hasn't passed
    assertEquals(0, _metadataClient._version);
    assertEquals(0, _metadataClient.clusterAndGeneration().generation());
    assertEquals(Cluster.empty(), _metadataClient.clusterAndGeneration().cluster());

    _mockTime.sleep(1);
    _metadataClient.refreshMetadata(metadataTimeoutMs);
    // Should have updated as metadataTTL has passed
    assertEquals(1, _metadataClient._version);
    assertEquals(1, _metadataClient.clusterAndGeneration().generation());
    Cluster receivedCluster = _metadataClient.clusterAndGeneration().cluster();
    assertReceivedCluster(receivedCluster);
  }

  @Test
  public void testDoesNotUpdateMetadataGenerationIfSameCluster() throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
    int metadataTimeoutMs = 150;

    // Arrange mocks
    Collection<String> topics = Collections.singletonList(TOPIC0);
    TopicDescription partialTopic0TopicDescription =
        new TopicDescription(TOPIC0, false, Collections.singletonList(
            defaultTopicPartitionInfo(_t0P0)
            // missing T0P1, T0P2
        ));
    Map<String, TopicDescription> topicDescriptions = new HashMap<>();
    topicDescriptions.put(TOPIC0, partialTopic0TopicDescription);
    replayMockAdminClient(_mockAdminClient, topics, topicDescriptions, metadataTimeoutMs);

    _mockTime.sleep(_metadataTTL);
    _metadataClient.refreshMetadata(metadataTimeoutMs);
    // Should have updated as metadataTTL has passed
    assertEquals(1, _metadataClient._version);
    assertEquals(1, _metadataClient.clusterAndGeneration().generation());
    Cluster receivedCluster = _metadataClient.clusterAndGeneration().cluster();
    assertReceivedCluster(receivedCluster, Collections.singletonList(_t0P0PartitionInfo));

    replayMockAdminClient(_mockAdminClient, topics, topicDescriptions, metadataTimeoutMs);
    _mockTime.sleep(_metadataTTL);
    _metadataClient.refreshMetadata(metadataTimeoutMs);
    // Should have updated as metadataTTL has passed but should not have bumped generation
    assertEquals(2, _metadataClient._version);
    // should not have bumped generation as cluster is the same
    assertEquals(1, _metadataClient.clusterAndGeneration().generation());
    receivedCluster = _metadataClient.clusterAndGeneration().cluster();
    assertReceivedCluster(receivedCluster, Collections.singletonList(_t0P0PartitionInfo));

    // put full topic 0 description
    topicDescriptions.put(TOPIC0, _topic0TopicDescription);
    replayMockAdminClient(_mockAdminClient, topics, topicDescriptions, metadataTimeoutMs);
    _mockTime.sleep(_metadataTTL);
    _metadataClient.refreshMetadata(metadataTimeoutMs);
    assertEquals(3, _metadataClient._version);
    // should have bumped generation as cluster is different
    assertEquals(2, _metadataClient.clusterAndGeneration().generation());
    receivedCluster = _metadataClient.clusterAndGeneration().cluster();
    assertReceivedCluster(receivedCluster);
  }

  private void assertReceivedCluster(Cluster receivedCluster) {
    assertReceivedCluster(receivedCluster, Arrays.asList(_t0P0PartitionInfo, _t0P1PartitionInfo, _t0P2PartitionInfo));
  }

  private void assertReceivedCluster(Cluster receivedCluster, List<PartitionInfo> expectedPartitionInfo) {
    assertEquals(_clusterId, receivedCluster.clusterResource().clusterId());
    assertEquals(_controllerNode, receivedCluster.controller());
    List<Node> receivedNodes = new ArrayList<>(receivedCluster.nodes());
    receivedNodes.sort(Comparator.comparingInt(Node::id)); // Cluster shuffles the node list
    assertArrayEquals(_nodes.toArray(new Node[0]), receivedNodes.toArray(new Node[0]));
    assertEquals(_t0P0PartitionInfo.leader(), receivedCluster.leaderFor(_t0P0));
    assertPartitionInfoListEquals(expectedPartitionInfo, receivedCluster.availablePartitionsForTopic(TOPIC0));
    assertEquals(Collections.EMPTY_LIST, receivedCluster.availablePartitionsForTopic(TOPIC1));
  }

  @Test
  public void testDoesNotUpdateMetadataOnTimeout() {
    // Arrange mocks
    EasyMock.expect(_mockAdminClient.listTopics(EasyMock.anyObject())).andThrow(new TimeoutException("timeout!"));
    EasyMock.expect(_mockAdminClient.describeCluster(EasyMock.anyObject())).andThrow(new TimeoutException("timeout!"));
    EasyMock.expect(_mockAdminClient.describeTopics(EasyMock.anyObject())).andThrow(new TimeoutException("timeout!"));
    EasyMock.replay(_mockAdminClient);

    // Act
    _metadataClient.refreshMetadata();

    // Assert -- nothing should have been updated
    assertEquals(0, _metadataClient._version);
    assertEquals(0, _metadataClient.clusterAndGeneration().generation());
    assertEquals(Cluster.empty(), _metadataClient.clusterAndGeneration().cluster());
  }

  /**
   * Sets up the AdminClient mock for the MetadataClient#doRefreshMetadata method
   */
  private void replayMockAdminClient(AdminClient adminClient, Collection<String> topics,
                                     Map<String, TopicDescription> topicDescriptions, int metadataTimeoutMs)
      throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
    EasyMock.reset(adminClient);
    mockListTopics(adminClient, topics, metadataTimeoutMs);
    mockDescribeCluster(adminClient, metadataTimeoutMs);
    mockDescribeTopics(adminClient, topics, topicDescriptions, metadataTimeoutMs);
    EasyMock.replay(adminClient);
  }

  private void mockDescribeTopics(AdminClient mockAdminClient, Collection<String> expectedTopicsToDescribe,
                                  Map<String, TopicDescription> topicDescriptions, int expectedTimeoutMs)
      throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
    DescribeTopicsResult mockDescribeTopicsResult = EasyMock.mock(DescribeTopicsResult.class);
    KafkaFuture mockKafkaFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockKafkaFuture.get(expectedTimeoutMs, TimeUnit.MILLISECONDS))
        .andReturn(topicDescriptions);
    EasyMock.expect(mockDescribeTopicsResult.all()).andReturn(mockKafkaFuture);

    EasyMock.expect(mockAdminClient.describeTopics(expectedTopicsToDescribe)).andReturn(mockDescribeTopicsResult);
    // cannot mock properly due to 1. EasyMock requiring both matchers to be concise at once
    // and 2. DescribeTopicsOptions being compared by reference, resulting in an invalid expectation
    EasyMock.expect(mockAdminClient
        .describeTopics(EasyMock.anyObject(Collection.class),
            EasyMock.anyObject(DescribeTopicsOptions.class)))
        .andReturn(mockDescribeTopicsResult);
    EasyMock.replay(mockDescribeTopicsResult, mockKafkaFuture);
  }

  private void mockDescribeCluster(AdminClient mockAdminClient, int expectedTimeoutMs)
      throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
    DescribeClusterResult mockDescribeClusterResult = EasyMock.mock(DescribeClusterResult.class);

    KafkaFuture mockClusterIdFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockClusterIdFuture.get(expectedTimeoutMs, TimeUnit.MILLISECONDS))
        .andReturn("cluster-1");
    EasyMock.expect(mockDescribeClusterResult.clusterId()).andReturn(mockClusterIdFuture);
    KafkaFuture mockControllerFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockControllerFuture.get(expectedTimeoutMs, TimeUnit.MILLISECONDS))
        .andReturn(new Node(0, "host0", 100));
    EasyMock.expect(mockDescribeClusterResult.controller()).andReturn(mockControllerFuture);

    KafkaFuture mockNodesFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockNodesFuture.get(expectedTimeoutMs, TimeUnit.MILLISECONDS))
        .andReturn(_nodes);
    EasyMock.expect(mockDescribeClusterResult.nodes()).andReturn(mockNodesFuture);

    EasyMock.expect(mockAdminClient.describeCluster()).andReturn(mockDescribeClusterResult);
    EasyMock.expect(mockAdminClient.describeCluster(EasyMock.anyObject())).andReturn(mockDescribeClusterResult);
    EasyMock.replay(mockDescribeClusterResult, mockClusterIdFuture,
        mockControllerFuture, mockNodesFuture);
  }

  private void mockListTopics(AdminClient mockAdminClient, Collection<String> topicNames,
                              int expectedTimeoutMs) throws InterruptedException, ExecutionException,
      java.util.concurrent.TimeoutException {
    ListTopicsResult mockListTopicsResult = EasyMock.mock(ListTopicsResult.class);
    KafkaFuture mockKafkaFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockKafkaFuture.get(expectedTimeoutMs, TimeUnit.MILLISECONDS))
        .andReturn(new HashSet<>(topicNames));
    EasyMock.expect(mockListTopicsResult.names()).andReturn(mockKafkaFuture);

    EasyMock.expect(mockAdminClient.listTopics(EasyMock.anyObject())).andReturn(mockListTopicsResult);
    EasyMock.expect(mockAdminClient.listTopics()).andReturn(mockListTopicsResult);
    EasyMock.replay(mockListTopicsResult, mockKafkaFuture);
  }

  private MockTime mockTime() {
    return new MockTime(0, 0L,
        TimeUnit.NANOSECONDS.convert(0L, TimeUnit.MILLISECONDS));
  }

  private static class PartitionInfoComparisonCriteria extends ComparisonCriteria {
    @Override
    protected void assertElementsEqual(Object expected, Object actual) {
      PartitionInfo expectedInfo = (PartitionInfo) expected;
      PartitionInfo actualInfo = (PartitionInfo) actual;
      Assert.assertEquals(expectedInfo.topic(), actualInfo.topic());
      Assert.assertEquals(expectedInfo.partition(), actualInfo.partition());
      Assert.assertEquals(expectedInfo.leader(), actualInfo.leader());
      Assert.assertArrayEquals(expectedInfo.replicas(), actualInfo.replicas());
      Assert.assertArrayEquals(expectedInfo.inSyncReplicas(), actualInfo.inSyncReplicas());
      Assert.assertArrayEquals(expectedInfo.offlineReplicas(), actualInfo.offlineReplicas());
    }
  }

  private void assertPartitionInfoListEquals(List<PartitionInfo> expected, List<PartitionInfo> actual) {
    new PartitionInfoComparisonCriteria().arrayEquals("", expected.toArray(), actual.toArray());
  }
}
