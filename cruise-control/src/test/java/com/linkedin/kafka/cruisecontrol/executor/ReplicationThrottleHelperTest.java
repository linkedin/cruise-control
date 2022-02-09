/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ReplicationThrottleHelperTest extends CCKafkaIntegrationTestHarness {
  private static final long TASK_EXECUTION_ALERTING_THRESHOLD_MS = 100L;
  private static final Config EMPTY_CONFIG = new Config(Collections.emptyList());

  /**
   * The admin client
   */
  private AdminClient _adminClient;

  @Override
  public int clusterSize() {
    return 4;
  }

  /**
   * Setup the test.
   */
  @Before
  public void setUp() {
    super.setUp();
    _adminClient = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
  }

  /**
   * Teardown the test.
   */
  @After
  public void tearDown() {
    super.tearDown();
    if (_adminClient != null) {
      _adminClient.close(Duration.ofMillis(1000L));
    }
  }

  private void createTopics() throws ExecutionException, InterruptedException {
    _adminClient.createTopics(Arrays.asList(
        new NewTopic(TOPIC0, 2, (short) 2),
        new NewTopic(TOPIC1, 2, (short) 2)
    )).all().get();
  }

  private static void setWildcardThrottleReplicaForTopic(ReplicationThrottleHelper helper, String topicName) throws Exception {
    for (String replicaThrottleProp : Arrays.asList(ReplicationThrottleHelper.LEADER_THROTTLED_REPLICAS,
                                                    ReplicationThrottleHelper.FOLLOWER_THROTTLED_REPLICAS)) {
      Collection<AlterConfigOp> configs = Collections.singletonList(
              new AlterConfigOp(new ConfigEntry(replicaThrottleProp, ReplicationThrottleHelper.WILDCARD_ASTERISK), AlterConfigOp.OpType.SET)
      );
      helper.changeTopicConfigs(topicName, configs);
    }
  }

  private ExecutionTask inProgressTaskForProposal(long id, ExecutionProposal proposal) {
    ExecutionTask task = new ExecutionTask(id, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, TASK_EXECUTION_ALERTING_THRESHOLD_MS);
    task.inProgress(0);
    return task;
  }

  private ExecutionTask completedTaskForProposal(long id, ExecutionProposal proposal) {
    ExecutionTask task = inProgressTaskForProposal(id, proposal);
    task.completed(1);
    return task;
  }

  @Test
  public void testIsNoOpWhenThrottleIsNull() throws Exception {
    AdminClient mockAdminClient = EasyMock.strictMock(AdminClient.class);
    EasyMock.replay(mockAdminClient);

    // Test would fail on any unexpected interactions with the kafkaZkClient
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, null);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition("topic", 0),
                                           100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = completedTaskForProposal(0, proposal);

    throttleHelper.setThrottles(Collections.singletonList(proposal));
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testClearThrottleOnNonExistentTopic() throws Exception {
    final long throttleRate = 100L;
    final int brokerId0 = 0;
    final int brokerId1 = 1;
    final int brokerId2 = 2;
    final List<Integer> brokers = Arrays.asList(brokerId0, brokerId1, brokerId2);
    final int partitionId = 0;
    // A proposal to move a partition with 2 replicas from broker 0 and 1 to broker 0 and 2
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, partitionId),
                                                       100,
                                                       new ReplicaPlacementInfo(brokerId0),
                                                       Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId2)));

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, throttleRate);

    // Case 1: a situation where Topic0 does not exist. Hence no property is returned upon read.
    expectDescribeBrokerConfigs(mockAdminClient, brokers);
    expectIncrementalBrokerConfigs(mockAdminClient, brokers);
    expectDescribeBrokerConfigs(mockAdminClient, brokers, EMPTY_CONFIG);
    expectDescribeTopicConfigs(mockAdminClient, TOPIC0, EMPTY_CONFIG, false);
    expectListTopics(mockAdminClient, Collections.emptySet());
    ExecutionTask mockCompleteTask = prepareMockCompleteTask(proposal);
    EasyMock.replay(mockAdminClient);

    throttleHelper.clearThrottles(Collections.singletonList(mockCompleteTask), Collections.emptyList());
    EasyMock.verify(mockAdminClient, mockCompleteTask);

    // Case 2: a situation where Topic0 gets deleted after its configs were read.
    EasyMock.reset(mockAdminClient);
    expectDescribeBrokerConfigs(mockAdminClient, brokers);
    expectIncrementalBrokerConfigs(mockAdminClient, brokers);
    expectDescribeBrokerConfigs(mockAdminClient, brokers, EMPTY_CONFIG);
    String throttledReplicas = brokerId0 + "," + brokerId1;
    Config topicConfigProps = new Config(Arrays.asList(
            new ConfigEntry(ReplicationThrottleHelper.LEADER_THROTTLED_REPLICAS, throttledReplicas),
            new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_THROTTLED_REPLICAS, throttledReplicas)));
    expectDescribeTopicConfigs(mockAdminClient, TOPIC0, topicConfigProps, true);
    expectIncrementalTopicConfigs(mockAdminClient, TOPIC0, false);
    expectListTopics(mockAdminClient, Collections.emptySet());
    mockCompleteTask = prepareMockCompleteTask(proposal);
    EasyMock.replay(mockAdminClient);

    // Expect no exception
    throttleHelper.clearThrottles(Collections.singletonList(mockCompleteTask), Collections.emptyList());
    EasyMock.verify(mockAdminClient, mockCompleteTask);
  }

  @Test
  public void testSetThrottleOnNonExistentTopic() throws Exception {
    final long throttleRate = 100L;
    final int brokerId0 = 0;
    final int brokerId1 = 1;
    final int brokerId2 = 2;
    final List<Integer> brokers = Arrays.asList(brokerId0, brokerId1, brokerId2);
    final int partitionId = 0;
    // A proposal to move a partition with 2 replicas from broker 0 and 1 to broker 0 and 2
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, partitionId),
                                                       100,
                                                       new ReplicaPlacementInfo(brokerId0),
                                                       Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId2)));

    AdminClient mockAdminClient = EasyMock.strictMock(AdminClient.class);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, throttleRate);

    // Case 1: a situation where Topic0 does not exist. Hence no property is returned upon read.
    expectDescribeBrokerConfigs(mockAdminClient, brokers);
    expectDescribeTopicConfigs(mockAdminClient, TOPIC0, EMPTY_CONFIG, false);
    expectListTopics(mockAdminClient, Collections.emptySet());
    expectIncrementalTopicConfigs(mockAdminClient, TOPIC0, false);
    expectListTopics(mockAdminClient, Collections.emptySet());
    EasyMock.replay(mockAdminClient);
    // Expect no exception
    throttleHelper.setThrottles(Collections.singletonList(proposal));
    EasyMock.verify(mockAdminClient);

    // Case 2: a situation where Topic0 gets deleted after its configs were read. Change configs should not fail.
    EasyMock.reset(mockAdminClient);
    expectDescribeBrokerConfigs(mockAdminClient, brokers);
    String throttledReplicas = brokerId0 + "," + brokerId1;
    Config topicConfigs = new Config(Arrays.asList(
      new ConfigEntry(ReplicationThrottleHelper.LEADER_THROTTLED_REPLICAS, throttledReplicas),
      new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_THROTTLED_REPLICAS, throttledReplicas)));
    expectDescribeTopicConfigs(mockAdminClient, TOPIC0, topicConfigs, true);
    expectIncrementalTopicConfigs(mockAdminClient, TOPIC0, false);
    expectListTopics(mockAdminClient, Collections.emptySet());
    EasyMock.replay(mockAdminClient);
    // Expect no exception
    throttleHelper.setThrottles(Collections.singletonList(proposal));
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testAddingThrottlesWithNoPreExistingThrottles() throws Exception {
    createTopics();

    final long throttleRate = 100L;
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
                                           100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = completedTaskForProposal(0, proposal);

    throttleHelper.setThrottles(Collections.singletonList(proposal));

    assertExpectedThrottledRateForBroker(0, throttleRate);
    assertExpectedThrottledRateForBroker(1, throttleRate);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // No throttle on broker 3 because it's not involved in any of the execution proposals:
    assertExpectedThrottledRateForBroker(3, null);
    assertExpectedThrottledReplicas(TOPIC0, "0:0,0:1,0:2");

    // We expect all throttles to be cleaned up
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());

    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    assertExpectedThrottledReplicas(TOPIC0, "");
  }

  @Test
  public void testAddingThrottlesWithPreExistingThrottles() throws Exception {
    createTopics();

    final long throttleRate = 100L;
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(
        new TopicPartition(TOPIC0, 0),
        100,
        new ReplicaPlacementInfo(0),
        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = completedTaskForProposal(0, proposal);

    // Broker 0 has an existing leader and follower throttle; we expect these to be preserved.
    long preExistingBroker0ThrottleRate = 200L;
    List<AlterConfigOp> broker0Configs = Arrays.asList(
      new AlterConfigOp(
              new ConfigEntry(ReplicationThrottleHelper.LEADER_THROTTLED_RATE, String.valueOf(preExistingBroker0ThrottleRate)),
              AlterConfigOp.OpType.SET),
      new AlterConfigOp(
              new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_THROTTLED_RATE, String.valueOf(preExistingBroker0ThrottleRate)),
              AlterConfigOp.OpType.SET)
    );
    throttleHelper.changeBrokerConfigs(0, broker0Configs);

    // Partition 1 (which is not involved in any execution proposal) has pre-existing throttled
    // replicas (on both leaders and followers); we expect these configurations to be merged
    // with our new throttled replicas.
    List<AlterConfigOp> topic0Configs = Arrays.asList(
      new AlterConfigOp(new ConfigEntry(ReplicationThrottleHelper.LEADER_THROTTLED_REPLICAS, "1:0,1:1"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_THROTTLED_REPLICAS, "1:0,1:1"), AlterConfigOp.OpType.SET));
    throttleHelper.changeTopicConfigs(TOPIC0, topic0Configs);

    // Topic 1 is not involved in any execution proposal. It has pre-existing throttled replicas.
    List<AlterConfigOp> topic1Config = Arrays.asList(
      new AlterConfigOp(new ConfigEntry(ReplicationThrottleHelper.LEADER_THROTTLED_REPLICAS, "1:1"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_THROTTLED_REPLICAS, "1:1"), AlterConfigOp.OpType.SET));
    throttleHelper.changeTopicConfigs(TOPIC1, topic1Config);

    throttleHelper.setThrottles(Collections.singletonList(proposal));

    assertExpectedThrottledRateForBroker(0, preExistingBroker0ThrottleRate);
    assertExpectedThrottledRateForBroker(1, throttleRate);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // No throttle on broker 3 because it's not involved in any of the execution proposals:
    assertExpectedThrottledRateForBroker(3, null);
    // Existing throttled replicas are merged with new throttled replicas for topic 0:
    assertExpectedThrottledReplicas(TOPIC0, "0:0,0:1,0:2,1:0,1:1");
    // Existing throttled replicas are unchanged for topic 1:
    assertExpectedThrottledReplicas(TOPIC1, "1:1");

    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());

    // We expect all throttles related to replica movement to be removed. Specifically,
    // any throttles related to partitions which were not moved will remain.
    // However, we do expect the broker throttles to be removed.
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());
    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    assertExpectedThrottledReplicas(TOPIC0, "1:0,1:1");
    assertExpectedThrottledReplicas(TOPIC1, "1:1");
  }

  @Test
  public void testDoNotModifyExistingWildcardReplicaThrottles() throws Exception {
    createTopics();

    final long throttleRate = 100L;

    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate);

    // Set replica throttle config values for both topics
    setWildcardThrottleReplicaForTopic(throttleHelper, TOPIC0);
    setWildcardThrottleReplicaForTopic(throttleHelper, TOPIC1);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
                                           100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionProposal proposal2 = new ExecutionProposal(new TopicPartition(TOPIC0, 1),
                                            100,
                                                        new ReplicaPlacementInfo(0),
                                                        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(3)),
                                                        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    throttleHelper.setThrottles(Arrays.asList(proposal, proposal2));

    ExecutionTask completedTask = completedTaskForProposal(0, proposal);
    ExecutionTask inProgressTask = inProgressTaskForProposal(1, proposal2);

    assertExpectedThrottledRateForBroker(0, throttleRate);
    assertExpectedThrottledRateForBroker(1, throttleRate);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    assertExpectedThrottledRateForBroker(3, throttleRate);
    // Topic-level throttled replica config value should remain as "*"
    assertExpectedThrottledReplicas(TOPIC0, ReplicationThrottleHelper.WILDCARD_ASTERISK);
    assertExpectedThrottledReplicas(TOPIC1, ReplicationThrottleHelper.WILDCARD_ASTERISK);

    throttleHelper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    assertExpectedThrottledRateForBroker(0, throttleRate);
    // we expect broker 1 to be null since all replica movement related to it has completed.
    assertExpectedThrottledRateForBroker(1, null);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // We expect broker 3 to have a throttle on it because there is an in-progress replica being moved
    assertExpectedThrottledRateForBroker(3, throttleRate);
    // Topic-level throttled replica config value should remain as "*"
    assertExpectedThrottledReplicas(TOPIC0, ReplicationThrottleHelper.WILDCARD_ASTERISK);
    assertExpectedThrottledReplicas(TOPIC1, ReplicationThrottleHelper.WILDCARD_ASTERISK);

    // passing an inProgress task that is not complete should have no effect.
    throttleHelper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    assertExpectedThrottledRateForBroker(0, throttleRate);
    // we expect broker 1 to be null since all replica movement related to it has completed.
    assertExpectedThrottledRateForBroker(1, null);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // We expect broker 3 to have a throttle on it because there is an in-progress replica being moved
    assertExpectedThrottledRateForBroker(3, throttleRate);
    // Topic-level throttled replica config value should remain as "*"
    assertExpectedThrottledReplicas(TOPIC0, ReplicationThrottleHelper.WILDCARD_ASTERISK);
    assertExpectedThrottledReplicas(TOPIC1, ReplicationThrottleHelper.WILDCARD_ASTERISK);

    // Completing the in-progress task and the "*" should not be cleaned up.
    inProgressTask.completed(3);
    throttleHelper.clearThrottles(Arrays.asList(completedTask, inProgressTask), Collections.emptyList());

    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    // Topic-level throttled replica config value should remain as "*"
    assertExpectedThrottledReplicas(TOPIC0, ReplicationThrottleHelper.WILDCARD_ASTERISK);
    assertExpectedThrottledReplicas(TOPIC1, ReplicationThrottleHelper.WILDCARD_ASTERISK);
  }

  @Test
  public void testDoNotRemoveThrottlesForInProgressTasks() throws Exception {
    createTopics();

    final long throttleRate = 100L;

    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
                                           100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionProposal proposal2 = new ExecutionProposal(new TopicPartition(TOPIC0, 1),
                                            100,
                                                        new ReplicaPlacementInfo(0),
                                                        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(3)),
                                                        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    throttleHelper.setThrottles(Arrays.asList(proposal, proposal2));

    ExecutionTask completedTask = completedTaskForProposal(0, proposal);
    ExecutionTask inProgressTask = inProgressTaskForProposal(1, proposal2);

    assertExpectedThrottledRateForBroker(0, throttleRate);
    assertExpectedThrottledRateForBroker(1, throttleRate);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    assertExpectedThrottledRateForBroker(3, throttleRate);
    assertExpectedThrottledReplicas(TOPIC0, "0:0,0:1,0:2,1:0,1:2,1:3");

    throttleHelper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    assertExpectedThrottledRateForBroker(0, throttleRate);
    // we expect broker 1 to be null since all replica movement related to it has completed.
    assertExpectedThrottledRateForBroker(1, null);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // We expect broker 3 to have a throttle on it because there is an in-progress replica being moved
    assertExpectedThrottledRateForBroker(3, throttleRate);
    assertExpectedThrottledReplicas(TOPIC0, "1:0,1:2,1:3");

    // passing an inProgress task that is not complete should have no effect.
    throttleHelper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    assertExpectedThrottledRateForBroker(0, throttleRate);
    // we expect broker 1 to be null since all replica movement related to it has completed.
    assertExpectedThrottledRateForBroker(1, null);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // We expect broker 3 to have a throttle on it because there is an in-progress replica being moved
    assertExpectedThrottledRateForBroker(3, throttleRate);
    assertExpectedThrottledReplicas(TOPIC0, "1:0,1:2,1:3");

    // Completing the in-progress task and clearing the throttles should clean everything up.
    inProgressTask.completed(3);
    throttleHelper.clearThrottles(Arrays.asList(completedTask, inProgressTask), Collections.emptyList());

    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    assertExpectedThrottledReplicas(TOPIC0, "");
  }

  @Test
  public void testRemoveReplicasFromConfigTest() {
    Set<String> replicas = new LinkedHashSet<>();
    replicas.add("foo");
    replicas.add("bar");
    replicas.add("baz");
    String throttleConfig = "foo,bar,qux,qaz,baz";
    String result = ReplicationThrottleHelper.removeReplicasFromConfig(throttleConfig, replicas);
    assertEquals(result, "qux,qaz");
  }

  @Test
  public void testWaitForConfigs() throws Exception {
    AdminClient mockAdminClient = EasyMock.strictMock(AdminClient.class);
    int retries = 3;
    // Case 1: queue more responses than RETRIES and expect checkConfigs to throw
    for (int i = 0; i < retries + 1; i++) {
      expectDescribeTopicConfigs(mockAdminClient, TOPIC0, EMPTY_CONFIG, true);
    }
    EasyMock.replay(mockAdminClient);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, 100L, retries);
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC0);
    assertThrows(IllegalStateException.class, () -> throttleHelper.waitForConfigs(cf, Collections.singletonList(
            new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOp.OpType.SET)
    )));

    // Case 2: queue a single result and call checkConfigs with matching configs, so it succeeds
    EasyMock.reset(mockAdminClient);
    expectDescribeTopicConfigs(mockAdminClient, TOPIC0, EMPTY_CONFIG, true);
    EasyMock.replay(mockAdminClient);
    throttleHelper.waitForConfigs(cf, Collections.emptyList());
  }

  @Test
  public void testConfigsEqual() {
    Map<String, String> expectedConfigs = new HashMap<>();
    List<ConfigEntry> entries = new ArrayList<>();

    assertTrue(ReplicationThrottleHelper.configsEqual(EMPTY_CONFIG, expectedConfigs));
    expectedConfigs.put("name1", "value1");
    assertFalse(ReplicationThrottleHelper.configsEqual(EMPTY_CONFIG, expectedConfigs));
    entries.add(new ConfigEntry("name1", "value1"));
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    expectedConfigs.put("name2", null);
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    entries.add(new ConfigEntry("name2", ""));
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    expectedConfigs.put("name3", null);
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    entries.add(new ConfigEntry("name3", null));
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    expectedConfigs.put("name4", "value4");
    assertFalse(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    entries.add(new ConfigEntry("name4", "other-value"));
    assertFalse(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    expectedConfigs.put("name4", "other-value");
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
  }

  private ExecutionTask prepareMockCompleteTask(ExecutionProposal proposal) {
    ExecutionTask mockCompleteTask = EasyMock.mock(ExecutionTask.class);
    EasyMock.expect(mockCompleteTask.state()).andReturn(ExecutionTaskState.COMPLETED).times(2);
    EasyMock.expect(mockCompleteTask.type()).andReturn(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION).once();
    EasyMock.expect(mockCompleteTask.proposal()).andReturn(proposal).once();
    EasyMock.replay(mockCompleteTask);
    return mockCompleteTask;
  }

  private void expectDescribeTopicConfigs(AdminClient adminClient, String topic, Config topicConfig, boolean topicExists)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    Map<ConfigResource, Config> topicConfigs = Collections.singletonMap(cf, topicConfig);
    DescribeConfigsResult mockDescribeConfigsResult = EasyMock.mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> mockFuture = EasyMock.mock(KafkaFuture.class);
    if (topicExists) {
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(topicConfigs);
    } else {
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject()))
              .andThrow(new ExecutionException(new UnknownTopicOrPartitionException()));
    }
    EasyMock.expect(mockDescribeConfigsResult.all()).andReturn(mockFuture);
    EasyMock.expect(adminClient.describeConfigs(Collections.singletonList(cf))).andReturn(mockDescribeConfigsResult);
    EasyMock.replay(mockDescribeConfigsResult, mockFuture);
  }

  private void expectIncrementalTopicConfigs(AdminClient adminClient, String topic, boolean topicExists)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    AlterConfigsResult mockAlterConfigsResult = EasyMock.mock(AlterConfigsResult.class);
    KafkaFuture<Void> mockFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockAlterConfigsResult.all()).andReturn(mockFuture);
    if (topicExists) {
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(null);
    } else {
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject()))
              .andThrow(new ExecutionException(new UnknownTopicOrPartitionException()));
    }
    EasyMock.expect(adminClient.incrementalAlterConfigs(Collections.singletonMap(cf, EasyMock.anyObject()))).andReturn(mockAlterConfigsResult);
    EasyMock.replay(mockAlterConfigsResult, mockFuture);
  }

  private void expectListTopics(AdminClient adminClient, Set<String> topics)
  throws ExecutionException, InterruptedException, TimeoutException {
    ListTopicsResult mockListTopicsResult = EasyMock.mock(ListTopicsResult.class);
    KafkaFuture<Set<String>> mockFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockListTopicsResult.names()).andReturn(mockFuture);
    EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(topics);
    EasyMock.expect(adminClient.listTopics()).andReturn(mockListTopicsResult);
    EasyMock.replay(mockListTopicsResult, mockFuture);
  }

  private void expectDescribeBrokerConfigs(AdminClient adminClient, List<Integer> brokers)
  throws ExecutionException, InterruptedException, TimeoutException {
    // All participating brokers have throttled rate set already
    Config brokerConfig = new Config(Arrays.asList(
      new ConfigEntry(ReplicationThrottleHelper.LEADER_THROTTLED_RATE, "100"),
      new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_THROTTLED_RATE, "100")));
    expectDescribeBrokerConfigs(adminClient, brokers, brokerConfig);
  }

  private void expectDescribeBrokerConfigs(AdminClient adminClient, List<Integer> brokers, Config brokerConfig)
  throws ExecutionException, InterruptedException, TimeoutException {
    for (int i : brokers) {
      ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(i));
      Map<ConfigResource, Config> brokerConfigs = Collections.singletonMap(cf, brokerConfig);
      DescribeConfigsResult mockDescribeConfigsResult = EasyMock.mock(DescribeConfigsResult.class);
      KafkaFuture<Map<ConfigResource, Config>> mockFuture = EasyMock.mock(KafkaFuture.class);
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(brokerConfigs);
      EasyMock.expect(mockDescribeConfigsResult.all()).andReturn(mockFuture);
      EasyMock.expect(adminClient.describeConfigs(Collections.singletonList(cf))).andReturn(mockDescribeConfigsResult);
      EasyMock.replay(mockDescribeConfigsResult, mockFuture);
    }
  }

  private void expectIncrementalBrokerConfigs(AdminClient adminClient, List<Integer> brokers)
  throws ExecutionException, InterruptedException, TimeoutException {
    for (int brokerId : brokers) {
      ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
      AlterConfigsResult mockAlterConfigsResult = EasyMock.mock(AlterConfigsResult.class);
      KafkaFuture<Void> mockFuture = EasyMock.mock(KafkaFuture.class);
      EasyMock.expect(mockAlterConfigsResult.all()).andReturn(mockFuture);
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(null);
      EasyMock.expect(adminClient.incrementalAlterConfigs(Collections.singletonMap(cf, EasyMock.anyObject()))).andReturn(mockAlterConfigsResult);
      EasyMock.replay(mockAlterConfigsResult, mockFuture);
    }
  }

  private void assertExpectedThrottledRateForBroker(int brokerId, Long expectedRate) throws ExecutionException, InterruptedException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    Map<ConfigResource, Config> brokerConfig = _adminClient.describeConfigs(Collections.singletonList(cf)).all().get();
    String expectedString = expectedRate == null ? null : String.valueOf(expectedRate);
    assertNotNull(brokerConfig.get(cf));
    if (expectedRate == null) {
      assertNull(brokerConfig.get(cf).get(ReplicationThrottleHelper.LEADER_THROTTLED_RATE));
      assertNull(brokerConfig.get(cf).get(ReplicationThrottleHelper.FOLLOWER_THROTTLED_RATE));
    } else {
      assertEquals(expectedString, brokerConfig.get(cf).get(ReplicationThrottleHelper.LEADER_THROTTLED_RATE).value());
      assertEquals(expectedString, brokerConfig.get(cf).get(ReplicationThrottleHelper.FOLLOWER_THROTTLED_RATE).value());
    }
  }

  private void assertExpectedThrottledReplicas(String topic, String expectedReplicas) throws ExecutionException, InterruptedException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    Map<ConfigResource, Config> topicConfig = _adminClient.describeConfigs(Collections.singletonList(cf)).all().get();
    assertNotNull(topicConfig.get(cf));
    assertEquals(expectedReplicas, topicConfig.get(cf).get(ReplicationThrottleHelper.LEADER_THROTTLED_REPLICAS).value());
    assertEquals(expectedReplicas, topicConfig.get(cf).get(ReplicationThrottleHelper.FOLLOWER_THROTTLED_REPLICAS).value());
  }
}
