/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetector;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;
import com.linkedin.kafka.cruisecontrol.servlet.EndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC2;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC3;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorNotification.ActionAgent.USER;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorNotification.ActionAgent.EXECUTION_COMPLETION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorNotification.ActionAgent.CRUISE_CONTROL;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

public class ExecutorTest extends CCKafkaIntegrationTestHarness {
  private static final int PARTITION = 0;
  private static final TopicPartition TP0 = new TopicPartition(TOPIC0, PARTITION);
  private static final TopicPartition TP1 = new TopicPartition(TOPIC1, PARTITION);
  private static final TopicPartition TP2 = new TopicPartition(TOPIC2, PARTITION);
  private static final TopicPartition TP3 = new TopicPartition(TOPIC3, PARTITION);
  private static final String RANDOM_UUID = "random_uuid";


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
  public void testBasicBalanceMovement() throws InterruptedException {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
                                                                              "ExecutorTestMetricGroup",
                                                                              "BasicBalanceMovement",
                                                                              false);
    try {
      Collection<ExecutionProposal> proposals = getBasicProposals();
      executeAndVerifyProposals(kafkaZkClient, proposals, proposals);
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Test
  public void testMoveNonExistingPartition() throws InterruptedException {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
                                                                              "ExecutorTestMetricGroup",
                                                                              "MoveNonExistingPartition",
                                                                              false);
    try {
      Map<String, TopicDescription> topicDescriptions = createTopics();
      int initialLeader0 = topicDescriptions.get(TOPIC0).partitions().get(0).leader().id();
      int initialLeader1 = topicDescriptions.get(TOPIC1).partitions().get(0).leader().id();

      ExecutionProposal proposal0 =
          new ExecutionProposal(TP0, 0, new ReplicaPlacementInfo(initialLeader0),
                                Collections.singletonList(new ReplicaPlacementInfo(initialLeader0)),
                                Collections.singletonList(initialLeader0 == 0 ? new ReplicaPlacementInfo(1) :
                                                                                new ReplicaPlacementInfo(0)));
      ExecutionProposal proposal1 =
          new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(initialLeader1),
                                Arrays.asList(new ReplicaPlacementInfo(initialLeader1),
                                              initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                                                                    new ReplicaPlacementInfo(0)),
                                Arrays.asList(initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                                                                    new ReplicaPlacementInfo(0),
                                              new ReplicaPlacementInfo(initialLeader1)));
      ExecutionProposal proposal2 =
          new ExecutionProposal(TP2, 0, new ReplicaPlacementInfo(initialLeader0),
                                Collections.singletonList(new ReplicaPlacementInfo(initialLeader0)),
                                Collections.singletonList(initialLeader0 == 0 ? new ReplicaPlacementInfo(1) :
                                                                                new ReplicaPlacementInfo(0)));
      ExecutionProposal proposal3 =
          new ExecutionProposal(TP3, 0, new ReplicaPlacementInfo(initialLeader1),
                                Arrays.asList(new ReplicaPlacementInfo(initialLeader1),
                                              initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                                                                    new ReplicaPlacementInfo(0)),
                                Arrays.asList(initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                                                                    new ReplicaPlacementInfo(0),
                                              new ReplicaPlacementInfo(initialLeader1)));

      Collection<ExecutionProposal> proposalsToExecute = Arrays.asList(proposal0, proposal1, proposal2, proposal3);
      Collection<ExecutionProposal> proposalsToCheck = Arrays.asList(proposal0, proposal1);
      executeAndVerifyProposals(kafkaZkClient, proposalsToExecute, proposalsToCheck);
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Test
  public void testBrokerDiesWhenMovePartitions() throws Exception {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
                                                                              "ExecutorTestMetricGroup",
                                                                              "BrokerDiesWhenMovePartitions",
                                                                              false);
    try {
      Map<String, TopicDescription> topicDescriptions = createTopics();
      int initialLeader0 = topicDescriptions.get(TOPIC0).partitions().get(0).leader().id();
      int initialLeader1 = topicDescriptions.get(TOPIC1).partitions().get(0).leader().id();

      _brokers.get(initialLeader0 == 0 ? 1 : 0).shutdown();
      ExecutionProposal proposal0 =
          new ExecutionProposal(TP0, 0, new ReplicaPlacementInfo(initialLeader0),
                                Collections.singletonList(new ReplicaPlacementInfo(initialLeader0)),
                                Collections.singletonList(initialLeader0 == 0 ? new ReplicaPlacementInfo(1) :
                                                                                new ReplicaPlacementInfo(0)));
      ExecutionProposal proposal1 =
          new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(initialLeader1),
                                Arrays.asList(new ReplicaPlacementInfo(initialLeader1),
                                              initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                                                                    new ReplicaPlacementInfo(0)),
                                Arrays.asList(initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                                                                    new ReplicaPlacementInfo(0),
                                              new ReplicaPlacementInfo(initialLeader1)));

      Collection<ExecutionProposal> proposalsToExecute = Arrays.asList(proposal0, proposal1);
      executeAndVerifyProposals(kafkaZkClient, proposalsToExecute, Collections.emptyList());

      // We are not doing the rollback.
      assertEquals(Collections.singletonList(initialLeader0 == 0 ? 1 : 0),
                   ExecutorUtils.newAssignmentForPartition(kafkaZkClient, TP0));
      assertEquals(initialLeader0, kafkaZkClient.getLeaderForPartition(new TopicPartition(TOPIC1, PARTITION)).get());
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Test
  public void testTimeoutLeaderActions() throws InterruptedException {
    createTopics();
    // The proposal tries to move the leader. We fake the replica list to be unchanged so there is no replica
    // movement, but only leader movement.
    ExecutionProposal proposal =
        new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(1),
                              Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                              Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)));

    KafkaCruiseControlConfig configs = new KafkaCruiseControlConfig(getExecutorProperties());
    Time time = new MockTime();
    MetadataClient mockMetadataClient = EasyMock.createMock(MetadataClient.class);
    // Fake the metadata to never change so the leader movement will timeout.
    Node node0 = new Node(0, "host0", 100);
    Node node1 = new Node(1, "host1", 100);
    Node[] replicas = new Node[2];
    replicas[0] = node0;
    replicas[1] = node1;
    PartitionInfo partitionInfo = new PartitionInfo(TP1.topic(), TP1.partition(), node1, replicas, replicas);
    Cluster cluster = new Cluster("id", Arrays.asList(node0, node1), Collections.singleton(partitionInfo),
                                  Collections.emptySet(), Collections.emptySet());
    MetadataClient.ClusterAndGeneration clusterAndGeneration = new MetadataClient.ClusterAndGeneration(cluster, 0);
    EasyMock.expect(mockMetadataClient.refreshMetadata()).andReturn(clusterAndGeneration).anyTimes();
    EasyMock.expect(mockMetadataClient.cluster()).andReturn(clusterAndGeneration.cluster()).anyTimes();
    EasyMock.replay(mockMetadataClient);

    Collection<ExecutionProposal> proposalsToExecute = Collections.singletonList(proposal);
    Executor executor = new Executor(configs, time, new MetricRegistry(), mockMetadataClient, 86400000L,
                                     43200000L, null, getMockUserTaskManager(RANDOM_UUID),
                                     getMockAnomalyDetector(RANDOM_UUID));
    executor.setExecutionMode(false);
    executor.executeProposals(proposalsToExecute,
                              Collections.emptySet(),
                              null,
                              EasyMock.mock(LoadMonitor.class),
                              null,
                              null,
                              null,
                              null,
                              null,
                              RANDOM_UUID);
    // Wait until the execution to start so the task timestamp is set to time.milliseconds.
    while (executor.state().state() != ExecutorState.State.LEADER_MOVEMENT_TASK_IN_PROGRESS) {
      Thread.sleep(10);
    }
    // Sleep over 180000 (the hard coded timeout) with some margin for inter-thread synchronization.
    time.sleep(200000);
    // The execution should finish.
    waitUntilExecutionFinishes(executor);
  }

  @Test
  public void testExecutorSendNotificationForUserTask() throws InterruptedException {
    Collection<ExecutionProposal> proposals = getBasicProposals();
    String uuid = "user-task-uuid";
    executeAndVerifyNotification(proposals, uuid, USER, EXECUTION_COMPLETION, true);
  }

  @Test
  public void testExecutorSendNotificationForSelfHealing() throws InterruptedException {
    Collection<ExecutionProposal> proposals = getBasicProposals();
    String uuid = AnomalyType.GOAL_VIOLATION.toString() + "-uuid";
    executeAndVerifyNotification(proposals, uuid, CRUISE_CONTROL, EXECUTION_COMPLETION, false);
  }

  private void executeAndVerifyNotification(Collection<ExecutionProposal> proposalsToExecute,
                                            String uuid,
                                            ExecutorNotification.ActionAgent startedBy,
                                            ExecutorNotification.ActionAgent endedBy,
                                            boolean expectUserTaskInfo) {

    KafkaCruiseControlConfig configs = new KafkaCruiseControlConfig(getExecutorProperties());
    UserTaskManager.UserTaskInfo mockUserTaskInfo = EasyMock.mock(UserTaskManager.UserTaskInfo.class);
    UserTaskManager mockUserTaskManager = EasyMock.mock(UserTaskManager.class);
    ExecutorNotifier mockExecutorNotifier = EasyMock.mock(ExecutorNotifier.class);
    Capture<ExecutorNotification> captureNotification = Capture.newInstance(CaptureType.FIRST);

    EasyMock.expect(mockUserTaskInfo.endPoint()).andReturn(EndPoint.REBALANCE).once();
    EasyMock.expect(mockUserTaskManager.markTaskExecutionBegan(uuid))
        .andReturn(expectUserTaskInfo ? mockUserTaskInfo : null).once();
    mockUserTaskManager.markTaskExecutionFinished(uuid);
    mockExecutorNotifier.sendNotification(EasyMock.capture(captureNotification));
    EasyMock.expectLastCall();

    EasyMock.replay(mockUserTaskInfo);
    EasyMock.replay(mockUserTaskManager);
    EasyMock.replay(mockExecutorNotifier);

    Executor executor = new Executor(configs, new SystemTime(), new MetricRegistry(), null, 86400000L,
                                     43200000L, mockExecutorNotifier, mockUserTaskManager, getMockAnomalyDetector(uuid));
    executor.setExecutionMode(false);
    executor.executeProposals(proposalsToExecute, Collections.emptySet(), null, EasyMock.mock(LoadMonitor.class), null,
                              null, null, null, null, uuid);
    waitUntilExecutionFinishes(executor);

    ExecutorNotification notification = captureNotification.getValue();
    assertEquals(notification.startedBy(), startedBy);
    assertEquals(notification.endedBy(), endedBy);
    assertEquals(notification.actionUuid(), uuid);
    if (expectUserTaskInfo) {
      assertNotNull(notification.userTaskInfo());
    } else {
      assertNull(notification.userTaskInfo());
    }
  }

  private Collection<ExecutionProposal> getBasicProposals() throws InterruptedException {
    Map<String, TopicDescription> topicDescriptions = createTopics();
    int initialLeader0 = topicDescriptions.get(TOPIC0).partitions().get(0).leader().id();
    int initialLeader1 = topicDescriptions.get(TOPIC1).partitions().get(0).leader().id();

    ExecutionProposal proposal0 =
        new ExecutionProposal(TP0, 0, new ReplicaPlacementInfo(initialLeader0),
                              Collections.singletonList(new ReplicaPlacementInfo(initialLeader0)),
                              Collections.singletonList(initialLeader0 == 0 ? new ReplicaPlacementInfo(1) :
                                                                              new ReplicaPlacementInfo(0)));
    ExecutionProposal proposal1 =
        new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(initialLeader1),
                              Arrays.asList(new ReplicaPlacementInfo(initialLeader1),
                                            initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                                                                  new ReplicaPlacementInfo(0)),
                              Arrays.asList(initialLeader1 == 0 ? new ReplicaPlacementInfo(1) :
                                                                  new ReplicaPlacementInfo(0),
                              new ReplicaPlacementInfo(initialLeader1)));
    return Arrays.asList(proposal0, proposal1);
  }

  private Map<String, TopicDescription> createTopics() throws InterruptedException {
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
                              AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
    try {
      adminClient.createTopics(Arrays.asList(new NewTopic(TOPIC0, 1, (short) 1),
                                             new NewTopic(TOPIC1, 1, (short) 2)));
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }

    // We need to use the admin clients to query the metadata from two different brokers to make sure that
    // both brokers have the latest metadata. Otherwise the Executor may get confused when it does not
    // see expected topics in the metadata.
    Map<String, TopicDescription> topicDescriptions0 = null;
    Map<String, TopicDescription> topicDescriptions1 = null;
    do {
      AdminClient adminClient0 = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
                                 AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
      AdminClient adminClient1 = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
                                 AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(1).plaintextAddr()));
      try {
        topicDescriptions0 = adminClient0.describeTopics(Arrays.asList(TOPIC0, TOPIC1)).all().get();
        topicDescriptions1 = adminClient1.describeTopics(Arrays.asList(TOPIC0, TOPIC1)).all().get();
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } catch (ExecutionException ee) {
        // Let it go.
      } finally {
        KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient0);
        KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient1);
      }
    } while (topicDescriptions0 == null || topicDescriptions0.size() < 2
        || topicDescriptions1 == null || topicDescriptions1.size() < 2);

    return topicDescriptions0;
  }

  private UserTaskManager getMockUserTaskManager(String uuid) {
    UserTaskManager mockUserTaskManager = EasyMock.mock(UserTaskManager.class);
    mockUserTaskManager.markTaskExecutionFinished(uuid);
    EasyMock.expect(mockUserTaskManager.markTaskExecutionBegan(uuid)).andReturn(null).anyTimes();
    EasyMock.replay(mockUserTaskManager);
    return mockUserTaskManager;
  }

  private AnomalyDetector getMockAnomalyDetector(String anomalyId) {
    AnomalyDetector mockAnomalyDetector = EasyMock.mock(AnomalyDetector.class);
    mockAnomalyDetector.markSelfHealingFinished(anomalyId);
    EasyMock.replay(mockAnomalyDetector);
    return mockAnomalyDetector;
  }

  private void executeAndVerifyProposals(KafkaZkClient kafkaZkClient,
                                         Collection<ExecutionProposal> proposalsToExecute,
                                         Collection<ExecutionProposal> proposalsToCheck) {
    KafkaCruiseControlConfig configs = new KafkaCruiseControlConfig(getExecutorProperties());

    Executor executor = new Executor(configs, new SystemTime(), new MetricRegistry(), null, 86400000L,
                                     43200000L, null, getMockUserTaskManager(RANDOM_UUID),
                                     getMockAnomalyDetector(RANDOM_UUID));
    executor.setExecutionMode(false);
    executor.executeProposals(proposalsToExecute, Collections.emptySet(), null, EasyMock.mock(LoadMonitor.class), null,
                              null, null, null, null, RANDOM_UUID);

    Map<TopicPartition, Integer> replicationFactors = new HashMap<>();
    for (ExecutionProposal proposal : proposalsToCheck) {
      TopicPartition tp = new TopicPartition(proposal.topic(), proposal.partitionId());
      int replicationFactor = kafkaZkClient.getReplicasForPartition(tp).size();
      replicationFactors.put(tp, replicationFactor);
    }

    waitUntilExecutionFinishes(executor);

    for (ExecutionProposal proposal : proposalsToCheck) {
      TopicPartition tp = new TopicPartition(proposal.topic(), proposal.partitionId());
      int expectedReplicationFactor = replicationFactors.get(tp);
      assertEquals("Replication factor for partition " + tp + " should be " + expectedReplicationFactor,
                   expectedReplicationFactor, kafkaZkClient.getReplicasForPartition(tp).size());

      if (proposal.hasReplicaAction()) {
        for (ReplicaPlacementInfo r : proposal.newReplicas()) {
          assertTrue("The partition should have moved for " + tp,
                     kafkaZkClient.getReplicasForPartition(tp).contains(r.brokerId()));
        }
      }
      assertEquals("The leader should have moved for " + tp,
                   proposal.newLeader().brokerId(), kafkaZkClient.getLeaderForPartition(tp).get());

    }
  }

  private void waitUntilExecutionFinishes(Executor executor) {
    long now = System.currentTimeMillis();
    while ((executor.hasOngoingExecution() || executor.state().state() != ExecutorState.State.NO_TASK_IN_PROGRESS)
        && System.currentTimeMillis() < now + 30000) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    if (executor.state().state() != ExecutorState.State.NO_TASK_IN_PROGRESS) {
      fail("The execution did not finish in 30 seconds.");
    }
  }

  private Properties getExecutorProperties() {
    Properties props = new Properties();
    String capacityConfigFile = this.getClass().getClassLoader().getResource("DefaultCapacityConfig.json").getFile();
    props.setProperty(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE, capacityConfigFile);
    props.setProperty(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.setProperty(KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG, NoopSampler.class.getName());
    props.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper().connectionString());
    props.setProperty(KafkaCruiseControlConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG, "10");
    props.setProperty(KafkaCruiseControlConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, "1000");
    props.setProperty(
        KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal");
    return props;
  }
}
