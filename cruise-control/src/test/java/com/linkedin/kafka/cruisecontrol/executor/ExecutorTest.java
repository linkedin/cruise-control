/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.CruiseControlUnitTestUtils;
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
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
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
    ZkUtils zkUtils = CruiseControlUnitTestUtils.zkUtils(zookeeper().getConnectionString());
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
  public void testMoveNonExistingPartition() {
    ZkUtils zkUtils = CruiseControlUnitTestUtils.zkUtils(zookeeper().getConnectionString());
    String topic0 = "testPartitionMovement0";
    String topic1 = "testPartitionMovement1";
    String topic2 = "testPartitionMovement2";
    String topic3 = "testPartitionMovement3";
    int partition = 0;
    TopicPartition tp0 = new TopicPartition(topic0, partition);
    TopicPartition tp1 = new TopicPartition(topic1, partition);
    TopicPartition tp2 = new TopicPartition(topic2, partition);
    TopicPartition tp3 = new TopicPartition(topic3, partition);
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
    ExecutionProposal proposal2 =
        new ExecutionProposal(tp2, 0, initialLeader0,
                              Collections.singletonList(initialLeader0),
                              Collections.singletonList(initialLeader0 == 0 ? 1 : 0));
    ExecutionProposal proposal3 =
        new ExecutionProposal(tp3, 0, initialLeader1,
                              Arrays.asList(initialLeader1, initialLeader1 == 0 ? 1 : 0),
                              Arrays.asList(initialLeader1 == 0 ? 1 : 0, initialLeader1));

    Collection<ExecutionProposal> proposalsToExecute = Arrays.asList(proposal0, proposal1, proposal2, proposal3);
    Collection<ExecutionProposal> proposalsToCheck = Arrays.asList(proposal0, proposal1);
    executeAndVerifyProposals(zkUtils, proposalsToExecute, proposalsToCheck);
  }

  @Test
  public void testBrokerDiesWhenMovePartitions() throws Exception {
    ZkUtils zkUtils = CruiseControlUnitTestUtils.zkUtils(zookeeper().getConnectionString());
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
    _brokers.get(initialLeader0 == 0 ? 1 : 0).shutdown();
    ExecutionProposal proposal0 =
        new ExecutionProposal(tp0, 0, initialLeader0,
                              Collections.singletonList(initialLeader0),
                              Collections.singletonList(initialLeader0 == 0 ? 1 : 0));
    ExecutionProposal proposal1 =
        new ExecutionProposal(tp1, 0, initialLeader1,
                              Arrays.asList(initialLeader1, initialLeader1 == 0 ? 1 : 0),
                              Arrays.asList(initialLeader1 == 0 ? 1 : 0, initialLeader1));

    Collection<ExecutionProposal> proposalsToExecute = Arrays.asList(proposal0, proposal1);
    executeAndVerifyProposals(zkUtils, proposalsToExecute, Collections.emptyList());

    // We are not doing the rollback.
    assertEquals(Collections.singletonList(initialLeader0 == 0 ? 1 : 0),
                 ExecutorUtils.newAssignmentForPartition(zkUtils, tp0));
    assertEquals(initialLeader0, zkUtils.getLeaderForPartition(topic1, partition).get());
  }

  private void executeAndVerifyProposals(ZkUtils zkUtils,
                                         Collection<ExecutionProposal> proposalsToExecute,
                                         Collection<ExecutionProposal> proposalsToCheck) {
    KafkaCruiseControlConfig configs = new KafkaCruiseControlConfig(getExecutorProperties());
    Executor executor = new Executor(configs, new SystemTime(), new MetricRegistry());
    executor.addExecutionProposals(proposalsToExecute, Collections.emptySet());
    executor.startExecution(EasyMock.mock(LoadMonitor.class));

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
      fail("The execution did not finish in 5 seconds.");
    }

    for (ExecutionProposal proposal : proposalsToCheck) {
      TopicPartition tp = new TopicPartition(proposal.topic(), proposal.partitionId());
      int expectedReplicationFector = replicationFactors.get(tp);
      assertEquals("Replication factor for partition " + tp + " should be " + expectedReplicationFector,
                   expectedReplicationFector, zkUtils.getReplicasForPartition(tp.topic(), tp.partition()).size());

      if (proposal.hasReplicaAction()) {
        for (int brokerId : proposal.newReplicas()) {
          assertTrue("The partition should have moved for " + tp,
                     zkUtils.getReplicasForPartition(tp.topic(), tp.partition()).contains(brokerId));
        }
      }
      assertEquals("The leader should have moved for " + tp,
                 proposal.newReplicas().get(0), zkUtils.getLeaderForPartition(tp.topic(), tp.partition()).get());

    }
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
