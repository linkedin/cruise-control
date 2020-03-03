/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.exception.OngoingExecutionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetector;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ExecutorTest extends CCKafkaIntegrationTestHarness {
  private static final int PARTITION = 0;
  private static final TopicPartition TP0 = new TopicPartition(TOPIC0, PARTITION);
  private static final TopicPartition TP1 = new TopicPartition(TOPIC1, PARTITION);
  private static final TopicPartition TP2 = new TopicPartition(TOPIC2, PARTITION);
  private static final TopicPartition TP3 = new TopicPartition(TOPIC3, PARTITION);
  private static final String RANDOM_UUID = "random_uuid";
  private static final long REMOVAL_HISTORY_RETENTION_TIME_MS = 43200000L;
  private static final long DEMOTION_HISTORY_RETENTION_TIME_MS = 86400000L;

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
  public void testBalanceMovement() throws InterruptedException, OngoingExecutionException {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
                                                                              "ExecutorTestMetricGroup",
                                                                              "BasicBalanceMovement",
                                                                              false);
    try {
      List<ExecutionProposal> proposalsToExecute = new ArrayList<>();
      List<ExecutionProposal> proposalsToCheck = new ArrayList<>();
      populateProposals(proposalsToExecute, proposalsToCheck);
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
  public void testTimeoutAndForceExecutionStop() throws InterruptedException, OngoingExecutionException {
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
    LoadMonitor mockLoadMonitor = EasyMock.mock(LoadMonitor.class);
    EasyMock.expect(mockLoadMonitor.taskRunnerState())
            .andReturn(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING)
            .anyTimes();
    mockLoadMonitor.pauseMetricSampling(isA(String.class));
    expectLastCall().anyTimes();
    mockLoadMonitor.resumeMetricSampling(isA(String.class));
    expectLastCall().anyTimes();
    EasyMock.replay(mockLoadMonitor);

    Collection<ExecutionProposal> proposalsToExecute = Collections.singletonList(proposal);
    Executor executor = new Executor(configs, time, new MetricRegistry(), mockMetadataClient, DEMOTION_HISTORY_RETENTION_TIME_MS,
                                     REMOVAL_HISTORY_RETENTION_TIME_MS, null, getMockUserTaskManager(RANDOM_UUID),
                                     getMockAnomalyDetector(RANDOM_UUID));
    executor.setExecutionMode(false);
    executor.executeProposals(proposalsToExecute,
                              Collections.emptySet(),
                              null,
                              mockLoadMonitor,
                              null,
                              null,
                              null,
                              null,
                              null,
                              null,
                              true,
                              RANDOM_UUID,
                              () -> "");
    // Wait until the execution to start so the task timestamp is set to time.milliseconds.
    while (executor.state().state() != ExecutorState.State.LEADER_MOVEMENT_TASK_IN_PROGRESS) {
      Thread.sleep(10);
    }
    // Sleep over 180000 (the hard coded timeout) with some margin for inter-thread synchronization.
    time.sleep(200000);
    // The execution should finish.
    waitUntilExecutionFinishes(executor);

    // The proposal tries to move replicas.
    proposal = new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(1),
                                     Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                     Arrays.asList(new ReplicaPlacementInfo(1), new ReplicaPlacementInfo(0)));
    proposalsToExecute = Collections.singletonList(proposal);
    executor.executeProposals(proposalsToExecute,
                              Collections.emptySet(),
                              null,
                              mockLoadMonitor,
                              null,
                              null,
                              null,
                              null,
                              null,
                              null,
                              true,
                              RANDOM_UUID,
                              () -> "");
    // Wait until the inter-broker replica movement task hang.
    while (executor.state().state() != ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS) {
      Thread.sleep(10);
    }
    // Force execution to stop.
    executor.userTriggeredStopExecution(true);
    // The execution should finish.
    waitUntilExecutionFinishes(executor);
  }

  private void populateProposals(List<ExecutionProposal> proposalToExecute,
                                 List<ExecutionProposal> proposalToVerify) throws InterruptedException {
    Map<String, TopicDescription> topicDescriptions = createTopics();
    int initialLeader0 = topicDescriptions.get(TOPIC0).partitions().get(0).leader().id();
    int initialLeader1 = topicDescriptions.get(TOPIC1).partitions().get(0).leader().id();
    // Valid proposals
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
    // Invalid proposals, the targeting topics of these proposals does not exist.
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

    proposalToExecute.addAll(Arrays.asList(proposal0, proposal1, proposal2, proposal3));
    proposalToVerify.addAll(Arrays.asList(proposal0, proposal1));
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
    mockUserTaskManager.markTaskExecutionFinished(uuid, false);
    EasyMock.expect(mockUserTaskManager.markTaskExecutionBegan(uuid)).andReturn(null).anyTimes();
    EasyMock.replay(mockUserTaskManager);
    return mockUserTaskManager;
  }

  private AnomalyDetector getMockAnomalyDetector(String anomalyId) {
    AnomalyDetector mockAnomalyDetector = EasyMock.mock(AnomalyDetector.class);
    mockAnomalyDetector.maybeClearOngoingAnomalyDetectionTimeMs();
    expectLastCall().anyTimes();
    mockAnomalyDetector.resetHasUnfixableGoals();
    expectLastCall().anyTimes();
    mockAnomalyDetector.markSelfHealingFinished(anomalyId);
    expectLastCall().anyTimes();
    EasyMock.replay(mockAnomalyDetector);
    return mockAnomalyDetector;
  }

  private void executeAndVerifyProposals(KafkaZkClient kafkaZkClient,
                                         Collection<ExecutionProposal> proposalsToExecute,
                                         Collection<ExecutionProposal> proposalsToCheck)
      throws OngoingExecutionException {
    KafkaCruiseControlConfig configs = new KafkaCruiseControlConfig(getExecutorProperties());
    UserTaskManager.UserTaskInfo mockUserTaskInfo = EasyMock.mock(UserTaskManager.UserTaskInfo.class);
    UserTaskManager mockUserTaskManager = EasyMock.mock(UserTaskManager.class);
    ExecutorNotifier mockExecutorNotifier = EasyMock.mock(ExecutorNotifier.class);
    Capture<String> captureNotification = Capture.newInstance(CaptureType.FIRST);

    EasyMock.expect(mockUserTaskInfo.endPoint()).andReturn(CruiseControlEndPoint.REBALANCE).once();
    EasyMock.expect(mockUserTaskManager.markTaskExecutionBegan(RANDOM_UUID)).andReturn(null).once();
    mockUserTaskManager.markTaskExecutionFinished(RANDOM_UUID, false);
    mockExecutorNotifier.sendNotification(EasyMock.capture(captureNotification));
    mockExecutorNotifier.sendAlert(EasyMock.capture(captureNotification));

    EasyMock.replay(mockUserTaskInfo);
    EasyMock.replay(mockUserTaskManager);
    EasyMock.replay(mockExecutorNotifier);

    Executor executor = new Executor(configs, new SystemTime(), new MetricRegistry(), null, DEMOTION_HISTORY_RETENTION_TIME_MS,
                                     REMOVAL_HISTORY_RETENTION_TIME_MS, mockExecutorNotifier, mockUserTaskManager,
                                     getMockAnomalyDetector(RANDOM_UUID));
    executor.setExecutionMode(false);
    LoadMonitor mockLoadMonitor = EasyMock.mock(LoadMonitor.class);
    EasyMock.expect(mockLoadMonitor.taskRunnerState())
            .andReturn(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING)
            .anyTimes();
    mockLoadMonitor.pauseMetricSampling(isA(String.class));
    expectLastCall().anyTimes();
    mockLoadMonitor.resumeMetricSampling(isA(String.class));
    expectLastCall().anyTimes();
    EasyMock.replay(mockLoadMonitor);

    executor.executeProposals(proposalsToExecute, Collections.emptySet(), null, mockLoadMonitor, null,
                              null, null, null,
                              null, null, true, RANDOM_UUID, () -> "");

    Map<TopicPartition, Integer> replicationFactors = new HashMap<>();
    for (ExecutionProposal proposal : proposalsToCheck) {
      TopicPartition tp = new TopicPartition(proposal.topic(), proposal.partitionId());
      int replicationFactor = kafkaZkClient.getReplicasForPartition(tp).size();
      replicationFactors.put(tp, replicationFactor);
    }

    waitUntilExecutionFinishes(executor);

    // Check notification is sent after execution has finished.
    String notification = captureNotification.getValue();
    assertTrue(notification.contains(RANDOM_UUID));

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
    props.setProperty(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.setProperty(MonitorConfig.METRIC_SAMPLER_CLASS_CONFIG, NoopSampler.class.getName());
    props.setProperty(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper().connectionString());
    props.setProperty(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG, "10");
    props.setProperty(ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, "1000");
    props.setProperty(
        AnalyzerConfig.DEFAULT_GOALS_CONFIG,
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
