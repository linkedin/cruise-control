/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import kafka.server.ConfigType;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static org.junit.Assert.assertEquals;

public class ReplicationThrottleHelperTest extends CCKafkaIntegrationTestHarness {

  @Override
  public int clusterSize() {
    return 4;
  }

  @Before
  public void setUp() {
    super.setUp();
  }

  @After
  public void tearDown() {
    super.tearDown();
  }

  private void createTopics() {
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
    try {
      adminClient.createTopics(Arrays.asList(
          new NewTopic(TOPIC0, 2, (short) 2),
          new NewTopic(TOPIC1, 2, (short) 2)
      ));
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }
  }

  @Test
  public void isNoOpWhenThrottleIsNull() {
    KafkaZkClient mockKafkaZkClient = EasyMock.strictMock(KafkaZkClient.class);
    EasyMock.replay(mockKafkaZkClient);

    // Test would fail on any unexpected interactions with the kafkaZkClient
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockKafkaZkClient, null);
    ExecutionProposal proposal = new ExecutionProposal(
        new TopicPartition("topic", 0),
        100,
        new ReplicaPlacementInfo(0),
        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));
    throttleHelper.setThrottles(Collections.singletonList(proposal));
    throttleHelper.clearThrottles();
  }

  private void assertExpectedThrottledRateForBroker(KafkaZkClient kafkaZkClient, int broker, Long expectedRate) {
    Properties brokerConfig = kafkaZkClient.getEntityConfigs(ConfigType.Broker(), String.valueOf(broker));
    String expectedString = expectedRate == null ? null : String.valueOf(expectedRate);
    assertEquals(expectedString, brokerConfig.getProperty(ReplicationThrottleHelper.LEADER_THROTTLED_RATE));
    assertEquals(expectedString, brokerConfig.getProperty(ReplicationThrottleHelper.FOLLOWER_THROTTLED_RATE));
  }

  private void assertExpectedThrottledReplicas(KafkaZkClient kafkaZkClient, String topic, String expectedReplicas) {
    Properties topicConfig = kafkaZkClient.getEntityConfigs(ConfigType.Topic(), topic);
    assertEquals(expectedReplicas, topicConfig.getProperty(ReplicationThrottleHelper.LEADER_THROTTLED_REPLICAS));
    assertEquals(expectedReplicas, topicConfig.getProperty(ReplicationThrottleHelper.FOLLOWER_THROTTLED_REPLICAS));
  }

  @Test
  public void addingThrottlesWithNoPreExistingThrottles() {
    createTopics();
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
        "ReplicationThrottleHelperTestMetricGroup",
        "AddingThrottlesWithNoPreExistingThrottles");

    final long throttleRate = 100L;

    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(kafkaZkClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(
        new TopicPartition(TOPIC0, 0),
        100,
         new ReplicaPlacementInfo(0),
         Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
         Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    throttleHelper.setThrottles(Collections.singletonList(proposal));

    assertExpectedThrottledRateForBroker(kafkaZkClient, 0, throttleRate);
    assertExpectedThrottledRateForBroker(kafkaZkClient, 1, throttleRate);
    assertExpectedThrottledRateForBroker(kafkaZkClient, 2, throttleRate);
    // No throttle on broker 3 because it's not involved in any of the execution proposals:
    assertExpectedThrottledRateForBroker(kafkaZkClient, 3, null);
    assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, "0:0,0:1,0:2");

    // We expect all throttles to be cleaned up
    throttleHelper.clearThrottles();

    Arrays.asList(0, 1, 2, 3).forEach((i) -> assertExpectedThrottledRateForBroker(kafkaZkClient, i, null));
    assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, null);
  }

  @Test
  public void addingThrottlesWithPreExistingThrottles() throws InterruptedException {
    createTopics();
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
        "ReplicationThrottleHelperTestMetricGroup",
        "AddingThrottlesWithNoPreExistingThrottles");

    final long throttleRate = 100L;

    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(kafkaZkClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(
        new TopicPartition(TOPIC0, 0),
        100,
        new ReplicaPlacementInfo(0),
        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    // Broker 0 has an existing leader and follower throttle; we expect these to be preserved.
    Properties broker0Config = new Properties();
    long preExistingBroker0ThrottleRate = 200L;
    broker0Config.setProperty(ReplicationThrottleHelper.LEADER_THROTTLED_RATE, String.valueOf(preExistingBroker0ThrottleRate));
    broker0Config.setProperty(ReplicationThrottleHelper.FOLLOWER_THROTTLED_RATE, String.valueOf(preExistingBroker0ThrottleRate));
    ExecutorUtils.changeBrokerConfig(new AdminZkClient(kafkaZkClient), 0, broker0Config);

    // Partition 1 (which is not involved in any execution proposal) has pre-existing throttled
    // replicas (on both leaders and followers); we expect these configurations to be merged
    // with our new throttled replicas.
    Properties topic0Config = kafkaZkClient.getEntityConfigs(ConfigType.Topic(), TOPIC0);
    topic0Config.setProperty(ReplicationThrottleHelper.LEADER_THROTTLED_REPLICAS, "1:0,1:1");
    topic0Config.setProperty(ReplicationThrottleHelper.FOLLOWER_THROTTLED_REPLICAS, "1:0,1:1");
    ExecutorUtils.changeTopicConfig(new AdminZkClient(kafkaZkClient), TOPIC0, topic0Config);

    // Topic 1 is not involved in any execution proposal. It has pre-existing throttled replicas.
    Properties topic1Config = kafkaZkClient.getEntityConfigs(ConfigType.Topic(), TOPIC1);
    topic1Config.setProperty(ReplicationThrottleHelper.LEADER_THROTTLED_REPLICAS, "1:1");
    topic1Config.setProperty(ReplicationThrottleHelper.FOLLOWER_THROTTLED_REPLICAS, "1:1");
    ExecutorUtils.changeTopicConfig(new AdminZkClient(kafkaZkClient), TOPIC1, topic1Config);

    throttleHelper.setThrottles(Collections.singletonList(proposal));

    assertExpectedThrottledRateForBroker(kafkaZkClient, 0, preExistingBroker0ThrottleRate);
    assertExpectedThrottledRateForBroker(kafkaZkClient, 1, throttleRate);
    assertExpectedThrottledRateForBroker(kafkaZkClient, 2, throttleRate);
    // No throttle on broker 3 because it's not involved in any of the execution proposals:
    assertExpectedThrottledRateForBroker(kafkaZkClient, 3, null);
    // Existing throttled replicas are merged with new throttled replicas for topic 0:
    assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, "0:0,0:1,0:2,1:0,1:1");
    // Existing throttled replicas are unchanged for topic 1:
    assertExpectedThrottledReplicas(kafkaZkClient, TOPIC1, "1:1");

    throttleHelper.clearThrottles();

    // We expect ALL throttles to be cleaned up, including pre-existing throttles and throttles
    // for topics which were not involved in the execution proposal
    throttleHelper.clearThrottles();
    Arrays.asList(0, 1, 2, 3).forEach((i) -> assertExpectedThrottledRateForBroker(kafkaZkClient, i, null));
    assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, null);
    assertExpectedThrottledReplicas(kafkaZkClient, TOPIC1, null);
  }

  @Test
  public void removeReplicasFromConfigTest() {
    Set<String> replicas = new LinkedHashSet<>();
    replicas.add("foo");
    replicas.add("bar");
    replicas.add("baz");
    String throttleConfig = "foo,bar,qux,qaz,baz";
    String result = ReplicationThrottleHelper.removeReplicasFromConfig(throttleConfig, replicas);
    assertEquals(result, "qux,qaz");
  }
}
