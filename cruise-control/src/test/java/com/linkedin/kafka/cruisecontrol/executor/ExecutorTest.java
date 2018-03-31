/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaIntegrationTestHarness;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class ExecutorTest extends AbstractKafkaIntegrationTestHarness {
  private static final String TOPIC_0 = "testPartitionMovement0";
  private static final String TOPIC_1 = "testPartitionMovement1";
  private static final String TOPIC_2 = "testPartitionMovement2";
  private static final String TOPIC_3 = "testPartitionMovement3";
  private static final int PARTITION = 0;
  private static final TopicPartition TP0 = new TopicPartition(TOPIC_0, PARTITION);
  private static final TopicPartition TP1 = new TopicPartition(TOPIC_1, PARTITION);
  private static final TopicPartition TP2 = new TopicPartition(TOPIC_2, PARTITION);
  private static final TopicPartition TP3 = new TopicPartition(TOPIC_3, PARTITION);


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
  public void testBasicBalanceMovement() {
    ZkUtils zkUtils = KafkaCruiseControlUnitTestUtils.zkUtils(zookeeper().getConnectionString());
    String topic0 = "testPartitionMovement0";
    String topic1 = "testPartitionMovement1";
    int partition = 0;
    TopicPartition tp0 = new TopicPartition(topic0, partition);
    TopicPartition tp1 = new TopicPartition(topic1, partition);
    AdminUtils.createTopic(zkUtils, topic0, 1, 1, new Properties(), RackAwareMode.Safe$.MODULE$);
    AdminUtils.createTopic(zkUtils, topic1, 1, 2, new Properties(), RackAwareMode.Safe$.MODULE$);
    while (zkUtils.getLeaderForPartition(topic0, partition).isEmpty()
        || zkUtils.getLeaderForPartition(topic1, partition).isEmpty()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    int initialLeader0 = (Integer) zkUtils.getLeaderForPartition(topic0, partition).get();
    int initialLeader1 = (Integer) zkUtils.getLeaderForPartition(topic1, partition).get();
    ExecutionProposal proposal0 =
        new ExecutionProposal(tp0, 0, initialLeader0,
                              Collections.singletonList(initialLeader0),
                              Collections.singletonList(initialLeader0 == 0 ? 1 : 0));
    ExecutionProposal proposal1 =
        new ExecutionProposal(tp1, 0, initialLeader1,
                              Arrays.asList(initialLeader1, initialLeader1 == 0 ? 1 : 0),
                              Arrays.asList(initialLeader1 == 0 ? 1 : 0, initialLeader1));

    Collection<ExecutionProposal> proposals = Arrays.asList(proposal0, proposal1);
    executeAndVerifyProposals(zkUtils, proposals, proposals);
  }

  @Test
  public void testMoveNonExistingPartition() throws InterruptedException {
    ZkUtils zkUtils = KafkaCruiseControlUnitTestUtils.zkUtils(zookeeper().getConnectionString());

    AdminClient adminClient = getAdminClient(broker(0).getPlaintextAddr());

    adminClient.createTopics(Arrays.asList(new NewTopic(TOPIC_0, 1, (short) 1),
                                           new NewTopic(TOPIC_1, 1, (short) 2)));

    Map<String, TopicDescription> topicDescriptions = createTopics();
    int initialLeader0 = topicDescriptions.get(TOPIC_0).partitions().get(0).leader().id();
    int initialLeader1 = topicDescriptions.get(TOPIC_1).partitions().get(0).leader().id();

    ExecutionProposal proposal0 =
        new ExecutionProposal(TP0, 0, initialLeader0,
                              Collections.singletonList(initialLeader0),
                              Collections.singletonList(initialLeader0 == 0 ? 1 : 0));
    ExecutionProposal proposal1 =
        new ExecutionProposal(TP1, 0, initialLeader1,
                              Arrays.asList(initialLeader1, initialLeader1 == 0 ? 1 : 0),
                              Arrays.asList(initialLeader1 == 0 ? 1 : 0, initialLeader1));
    ExecutionProposal proposal2 =
        new ExecutionProposal(TP2, 0, initialLeader0,
                              Collections.singletonList(initialLeader0),
                              Collections.singletonList(initialLeader0 == 0 ? 1 : 0));
    ExecutionProposal proposal3 =
        new ExecutionProposal(TP3, 0, initialLeader1,
                              Arrays.asList(initialLeader1, initialLeader1 == 0 ? 1 : 0),
                              Arrays.asList(initialLeader1 == 0 ? 1 : 0, initialLeader1));

    Collection<ExecutionProposal> proposalsToExecute = Arrays.asList(proposal0, proposal1, proposal2, proposal3);
    Collection<ExecutionProposal> proposalsToCheck = Arrays.asList(proposal0, proposal1);
    executeAndVerifyProposals(zkUtils, proposalsToExecute, proposalsToCheck);
  }

  @Test
  public void testBrokerDiesWhenMovePartitions() throws Exception {
    ZkUtils zkUtils = KafkaCruiseControlUnitTestUtils.zkUtils(zookeeper().getConnectionString());

    Map<String, TopicDescription> topicDescriptions = createTopics();
    int initialLeader0 = topicDescriptions.get(TOPIC_0).partitions().get(0).leader().id();
    int initialLeader1 = topicDescriptions.get(TOPIC_1).partitions().get(0).leader().id();

    _brokers.get(initialLeader0 == 0 ? 1 : 0).shutdown();
    ExecutionProposal proposal0 =
        new ExecutionProposal(TP0, 0, initialLeader0,
                              Collections.singletonList(initialLeader0),
                              Collections.singletonList(initialLeader0 == 0 ? 1 : 0));
    ExecutionProposal proposal1 =
        new ExecutionProposal(TP1, 0, initialLeader1,
                              Arrays.asList(initialLeader1, initialLeader1 == 0 ? 1 : 0),
                              Arrays.asList(initialLeader1 == 0 ? 1 : 0, initialLeader1));

    Collection<ExecutionProposal> proposalsToExecute = Arrays.asList(proposal0, proposal1);
    executeAndVerifyProposals(zkUtils, proposalsToExecute, Collections.emptyList());

    // We are not doing the rollback.
    assertEquals(Collections.singletonList(initialLeader0 == 0 ? 1 : 0),
                 ExecutorUtils.newAssignmentForPartition(zkUtils, TP0));
    assertEquals(initialLeader0, zkUtils.getLeaderForPartition(TOPIC_1, PARTITION).get());
  }

  private Map<String, TopicDescription> createTopics() throws InterruptedException {
    AdminClient adminClient = getAdminClient(broker(0).getPlaintextAddr());

    adminClient.createTopics(Arrays.asList(new NewTopic(TOPIC_0, 1, (short) 1),
                                           new NewTopic(TOPIC_1, 1, (short) 2)));

    // We need to use the admin clients to query the metadata from two different brokers to make sure that
    // both brokers have the latest metadata. Otherwise the Executor may get confused when it does not
    // see expected topics in the metadata.
    Map<String, TopicDescription> topicDescriptions0 = null;
    Map<String, TopicDescription> topicDescriptions1 = null;
    do {
      try (AdminClient adminClient0 = getAdminClient(broker(0).getPlaintextAddr());
          AdminClient adminClient1 = getAdminClient(broker(1).getPlaintextAddr())) {
        topicDescriptions0 = adminClient0.describeTopics(Arrays.asList(TOPIC_0, TOPIC_1)).all().get();
        topicDescriptions1 = adminClient1.describeTopics(Arrays.asList(TOPIC_0, TOPIC_1)).all().get();
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } catch (ExecutionException ee) {
        // Let it go.
      }
    } while (topicDescriptions0 == null || topicDescriptions0.size() < 2
        || topicDescriptions1 == null || topicDescriptions1.size() < 2);

    return topicDescriptions0;
  }

  private void executeAndVerifyProposals(ZkUtils zkUtils,
                                         Collection<ExecutionProposal> proposalsToExecute,
                                         Collection<ExecutionProposal> proposalsToCheck) {
    KafkaCruiseControlConfig configs = new KafkaCruiseControlConfig(getExecutorProperties());
    Executor executor = new Executor(configs, new SystemTime(), new MetricRegistry());
    executor.executeProposals(proposalsToExecute, Collections.emptySet(), EasyMock.mock(LoadMonitor.class));

    Map<TopicPartition, Integer> replicationFactors = new HashMap<>();
    for (ExecutionProposal proposal : proposalsToCheck) {
      int replicationFactor = zkUtils.getReplicasForPartition(proposal.topic(), proposal.partitionId()).size();
      replicationFactors.put(new TopicPartition(proposal.topic(), proposal.partitionId()), replicationFactor);
    }

    long now = System.currentTimeMillis();
    while (executor.state().state() != ExecutorState.State.NO_TASK_IN_PROGRESS
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

    for (ExecutionProposal proposal : proposalsToCheck) {
      TopicPartition tp = new TopicPartition(proposal.topic(), proposal.partitionId());
      int expectedReplicationFactor = replicationFactors.get(tp);
      assertEquals("Replication factor for partition " + tp + " should be " + expectedReplicationFactor,
                   expectedReplicationFactor, zkUtils.getReplicasForPartition(tp.topic(), tp.partition()).size());

      if (proposal.hasReplicaAction()) {
        for (int brokerId : proposal.newReplicas()) {
          assertTrue("The partition should have moved for " + tp,
                     zkUtils.getReplicasForPartition(tp.topic(), tp.partition()).contains(brokerId));
        }
      }
      assertEquals("The leader should have moved for " + tp,
                   proposal.newLeader(), zkUtils.getLeaderForPartition(tp.topic(), tp.partition()).get());

    }
  }

  private AdminClient getAdminClient(String bootstrapServer) {
    Properties props = new Properties();
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    return AdminClient.create(props);
  }

  private Properties getExecutorProperties() {
    Properties props = new Properties();
    String capacityConfigFile = this.getClass().getClassLoader().getResource("DefaultCapacityConfig.json").getFile();
    props.setProperty(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE, capacityConfigFile);
    props.setProperty(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.setProperty(KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG, NoopSampler.class.getName());
    props.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper().getConnectionString());
    props.setProperty(KafkaCruiseControlConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG, "10");
    props.setProperty(KafkaCruiseControlConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, "1000");
    return props;
  }
}
