/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;
import com.linkedin.kafka.clients.utils.tests.AbstractKafkaIntegrationTestHarness;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

    KafkaCruiseControlConfig configs = new KafkaCruiseControlConfig(getExecutorProperties());
    Executor executor = new Executor(configs, new SystemTime());
    executor.addBalancingProposals(Arrays.asList(proposal0, proposal1), Collections.emptySet());
    executor.startExecution(null);
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
    assertEquals("Replication factor of topic 0 should be 1", zkUtils.getReplicasForPartition(topic0, partition).size(), 1);
    assertEquals("The partition should have moved.", zkUtils.getReplicasForPartition(topic0, partition).apply(0),
                 initialLeader0 == 0 ? 1 : 0);
    assertEquals("The leader should have moved.", zkUtils.getLeaderForPartition(topic1, partition).get(),
                 initialLeader1 == 0 ? 1 : 0);
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
