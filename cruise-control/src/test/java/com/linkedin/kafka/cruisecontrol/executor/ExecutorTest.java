/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
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
    BalancingProposal proposal0 =
        new BalancingProposal(tp0, initialLeader0, initialLeader0 == 0 ? 1 : 0, BalancingAction.REPLICA_MOVEMENT);
    BalancingProposal proposal1 =
        new BalancingProposal(tp1, initialLeader1, initialLeader1 == 0 ? 1 : 0, BalancingAction.LEADERSHIP_MOVEMENT);

    Collection<BalancingProposal> proposals = Arrays.asList(proposal0, proposal1);
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
    BalancingProposal proposal0 =
        new BalancingProposal(tp0, initialLeader0, initialLeader0 == 0 ? 1 : 0, BalancingAction.REPLICA_MOVEMENT);
    BalancingProposal proposal1 =
        new BalancingProposal(tp1, initialLeader1, initialLeader1 == 0 ? 1 : 0, BalancingAction.LEADERSHIP_MOVEMENT);
    BalancingProposal proposal2 =
        new BalancingProposal(tp2, 0, 1, BalancingAction.REPLICA_MOVEMENT);
    BalancingProposal proposal3 =
        new BalancingProposal(tp3, 0, 1, BalancingAction.LEADERSHIP_MOVEMENT);

    Collection<BalancingProposal> proposalsToExecute = Arrays.asList(proposal0, proposal1, proposal2, proposal3);
    Collection<BalancingProposal> proposalsToCheck = Arrays.asList(proposal0, proposal1);
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
    BalancingProposal proposal0 =
        new BalancingProposal(tp0, initialLeader0, initialLeader0 == 0 ? 1 : 0, BalancingAction.REPLICA_MOVEMENT);
    BalancingProposal proposal1 =
        new BalancingProposal(tp1, initialLeader1, initialLeader1 == 0 ? 1 : 0, BalancingAction.LEADERSHIP_MOVEMENT);

    Collection<BalancingProposal> proposalsToExecute = Arrays.asList(proposal0, proposal1);
    executeAndVerifyProposals(zkUtils, proposalsToExecute, Collections.emptyList());

    assertEquals(Collections.singletonList(initialLeader0), ExecutorUtils.newAssignmentForPartition(zkUtils, tp0));
  }

  private void executeAndVerifyProposals(ZkUtils zkUtils,
                                         Collection<BalancingProposal> proposalsToExecute,
                                         Collection<BalancingProposal> proposalsToCheck) {
    KafkaCruiseControlConfig configs = new KafkaCruiseControlConfig(getExecutorProperties());
    Executor executor = new Executor(configs, new SystemTime(), new MetricRegistry());
    executor.addBalancingProposals(proposalsToExecute, Collections.emptySet());
    executor.startExecution(null);

    Map<TopicPartition, Integer> replicationFactors = new HashMap<>();
    for (BalancingProposal proposal : proposalsToCheck) {
      int replicationFactor = zkUtils.getReplicasForPartition(proposal.topic(), proposal.partitionId()).size();
      replicationFactors.put(new TopicPartition(proposal.topic(), proposal.partitionId()), replicationFactor);
    }

    long now = System.currentTimeMillis();
    while (executor.state().state() != ExecutorState.State.NO_TASK_IN_PROGRESS
        && System.currentTimeMillis() < now + 5000) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    if (executor.state().state() != ExecutorState.State.NO_TASK_IN_PROGRESS) {
      fail("The execution did not finish in 5 seconds.");
    }

    for (BalancingProposal proposal : proposalsToCheck) {
      TopicPartition tp = new TopicPartition(proposal.topic(), proposal.partitionId());
      int expectedReplicationFector = replicationFactors.get(tp);
      assertEquals("Replication factor for partition " + tp + " should be " + expectedReplicationFector,
                   expectedReplicationFector, zkUtils.getReplicasForPartition(tp.topic(), tp.partition()).size());

      if (proposal.balancingAction() == BalancingAction.REPLICA_MOVEMENT) {
        assertTrue("The partition should have moved for " + tp,
                   zkUtils.getReplicasForPartition(tp.topic(), tp.partition()).contains(proposal.destinationBrokerId()));
      } else {
        assertEquals("The leader should have moved for " + tp,
                   proposal.destinationBrokerId(), zkUtils.getLeaderForPartition(tp.topic(), tp.partition()).get());
      }
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
