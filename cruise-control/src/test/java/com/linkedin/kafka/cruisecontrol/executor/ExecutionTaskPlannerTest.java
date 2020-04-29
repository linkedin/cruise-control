/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.BaseReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeLargeReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeSmallReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartitionReplica;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.apache.kafka.common.KafkaFuture.completedFuture;
import static org.easymock.EasyMock.anyObject;
import static org.junit.Assert.assertEquals;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC2;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;

/**
 * Unit test class for execution task planner
 */
public class ExecutionTaskPlannerTest {
  private ReplicaPlacementInfo _r0 = new ReplicaPlacementInfo(0);
  private ReplicaPlacementInfo _r1 = new ReplicaPlacementInfo(1);
  private ReplicaPlacementInfo _r2 = new ReplicaPlacementInfo(2);
  private ReplicaPlacementInfo _r3 = new ReplicaPlacementInfo(3);

  private final ExecutionProposal _leaderMovement1 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 0), 0, _r1, Arrays.asList(_r1, _r0), Arrays.asList(_r0, _r1));
  private final ExecutionProposal _leaderMovement2 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 1), 0, _r1, Arrays.asList(_r1, _r0), Arrays.asList(_r0, _r1));
  private final ExecutionProposal _leaderMovement3 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 2), 0, _r1, Arrays.asList(_r1, _r2), Arrays.asList(_r2, _r1));
  private final ExecutionProposal _leaderMovement4 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 3), 0, _r3, Arrays.asList(_r3, _r2), Arrays.asList(_r2, _r3));

  private final ExecutionProposal _partitionMovement1 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 0), 1, _r0, Arrays.asList(_r0, _r2), Arrays.asList(_r2, _r1));
  private final ExecutionProposal _partitionMovement2 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 1), 3, _r1, Arrays.asList(_r1, _r3), Arrays.asList(_r3, _r2));
  private final ExecutionProposal _partitionMovement3 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 2), 2, _r2, Arrays.asList(_r2, _r1), Arrays.asList(_r1, _r3));
  private final ExecutionProposal _partitionMovement4 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 3), 1, _r3, Arrays.asList(_r3, _r2), Arrays.asList(_r2, _r0));

  private final List<Node> _expectedNodes = Arrays.asList(new Node(0, "null", -1),
                                                          new Node(1, "null", -1),
                                                          new Node(2, "null", -1),
                                                          new Node(3, "null", -1));

  @Test
  public void testGetLeaderMovementTasks() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(_leaderMovement1);
    proposals.add(_leaderMovement2);
    proposals.add(_leaderMovement3);
    proposals.add(_leaderMovement4);
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG, "");
    ExecutionTaskPlanner planner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(props));

    Set<PartitionInfo> partitions = new HashSet<>();
    for (ExecutionProposal proposal : proposals) {
      partitions.add(generatePartitionInfo(proposal, false));
    }

    Cluster expectedCluster = new Cluster(null,
                                          _expectedNodes,
                                          partitions,
                                          Collections.<String>emptySet(),
                                          Collections.<String>emptySet());

    planner.addExecutionProposals(proposals, expectedCluster, null);
    List<ExecutionTask> leaderMovementTasks = planner.getLeadershipMovementTasks(2);
    assertEquals("2 of the leader movements should return in one batch", 2, leaderMovementTasks.size());
    assertEquals(4, leaderMovementTasks.get(0).executionId());
    assertEquals(leaderMovementTasks.get(0).proposal(), _leaderMovement1);
    assertEquals(5, leaderMovementTasks.get(1).executionId());
    assertEquals(leaderMovementTasks.get(1).proposal(), _leaderMovement2);
    leaderMovementTasks = planner.getLeadershipMovementTasks(2);
    assertEquals("2 of the leader movements should return in one batch", 2, leaderMovementTasks.size());
    assertEquals(6, leaderMovementTasks.get(0).executionId());
    assertEquals(leaderMovementTasks.get(0).proposal(), _leaderMovement3);
    assertEquals(7, leaderMovementTasks.get(1).executionId());
    assertEquals(leaderMovementTasks.get(1).proposal(), _leaderMovement4);
  }

  @Test
  public void testGetInterBrokerPartitionMovementTasks() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(_partitionMovement1);
    proposals.add(_partitionMovement2);
    proposals.add(_partitionMovement3);
    proposals.add(_partitionMovement4);
    // Test different execution strategies.
    ExecutionTaskPlanner basePlanner = new ExecutionTaskPlanner(
        null,
        new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));

    Properties postponeUrpProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    postponeUrpProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                 PostponeUrpReplicaMovementStrategy.class.getName().toString());
    ExecutionTaskPlanner postponeUrpPlanner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(postponeUrpProps));

    Properties prioritizeLargeMovementProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    prioritizeLargeMovementProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                             String.format("%s,%s", PrioritizeLargeReplicaMovementStrategy.class.getName(),
                                                           BaseReplicaMovementStrategy.class.getName()));
    ExecutionTaskPlanner prioritizeLargeMovementPlanner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(prioritizeLargeMovementProps));

    Properties prioritizeSmallMovementProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    prioritizeSmallMovementProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                             String.format("%s,%s", PrioritizeSmallReplicaMovementStrategy.class.getName(),
                                                           BaseReplicaMovementStrategy.class.getName()));
    ExecutionTaskPlanner prioritizeSmallMovementPlanner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(prioritizeSmallMovementProps));

    Set<PartitionInfo> partitions = new HashSet<>();
    partitions.add(generatePartitionInfo(_partitionMovement1, true));
    partitions.add(generatePartitionInfo(_partitionMovement2, false));
    partitions.add(generatePartitionInfo(_partitionMovement3, true));
    partitions.add(generatePartitionInfo(_partitionMovement4, false));

    Cluster expectedCluster = new Cluster(null, _expectedNodes, partitions, Collections.<String>emptySet(), Collections.<String>emptySet());

    Map<Integer, Integer> readyBrokers = new HashMap<>();
    readyBrokers.put(0, 8);
    readyBrokers.put(1, 8);
    readyBrokers.put(2, 8);
    readyBrokers.put(3, 8);
    basePlanner.addExecutionProposals(proposals, expectedCluster, null);
    List<ExecutionTask> partitionMovementTasks = basePlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement1", _partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement3", _partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement4", _partitionMovement4, partitionMovementTasks.get(2).proposal());

    postponeUrpPlanner.addExecutionProposals(proposals, expectedCluster, null);
    partitionMovementTasks = postponeUrpPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement4", _partitionMovement4, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement2", _partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement1", _partitionMovement1, partitionMovementTasks.get(2).proposal());

    prioritizeLargeMovementPlanner.addExecutionProposals(proposals, expectedCluster, null);
    partitionMovementTasks = prioritizeLargeMovementPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement1", _partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement3", _partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement4", _partitionMovement4, partitionMovementTasks.get(2).proposal());

    prioritizeSmallMovementPlanner.addExecutionProposals(proposals, expectedCluster, null);
    partitionMovementTasks = prioritizeSmallMovementPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement4", _partitionMovement4, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement2", _partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement1", _partitionMovement1, partitionMovementTasks.get(2).proposal());
  }

  @Test
  public void testDynamicConfigReplicaMovementStrategy() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(_partitionMovement1);
    proposals.add(_partitionMovement2);
    proposals.add(_partitionMovement3);
    proposals.add(_partitionMovement4);
    ExecutionTaskPlanner planner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));

    Set<PartitionInfo> partitions = new HashSet<>();
    partitions.add(generatePartitionInfo(_partitionMovement1, true));
    partitions.add(generatePartitionInfo(_partitionMovement2, false));
    partitions.add(generatePartitionInfo(_partitionMovement3, true));
    partitions.add(generatePartitionInfo(_partitionMovement4, false));

    Cluster expectedCluster = new Cluster(null, _expectedNodes, partitions, Collections.<String>emptySet(), Collections.<String>emptySet());

    Map<Integer, Integer> readyBrokers = new HashMap<>();
    readyBrokers.put(0, 8);
    readyBrokers.put(1, 8);
    readyBrokers.put(2, 8);
    readyBrokers.put(3, 8);
    planner.addExecutionProposals(proposals, expectedCluster, null);
    List<ExecutionTask> partitionMovementTasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement1", _partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement3", _partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement4", _partitionMovement4, partitionMovementTasks.get(2).proposal());

    planner.addExecutionProposals(proposals, expectedCluster, new PostponeUrpReplicaMovementStrategy());
    partitionMovementTasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement4", _partitionMovement4, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement2", _partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement1", _partitionMovement1, partitionMovementTasks.get(2).proposal());

    planner.addExecutionProposals(proposals, expectedCluster, new PrioritizeLargeReplicaMovementStrategy());
    partitionMovementTasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement1", _partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement3", _partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement4", _partitionMovement4, partitionMovementTasks.get(2).proposal());

    planner.addExecutionProposals(proposals, expectedCluster, new PrioritizeSmallReplicaMovementStrategy());
    partitionMovementTasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement4", _partitionMovement4, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement2", _partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement1", _partitionMovement1, partitionMovementTasks.get(2).proposal());

  }

  @Test
  public void testGetIntraBrokerPartitionMovementTasks() {
    ReplicaPlacementInfo r0d0 = new ReplicaPlacementInfo(0, "d0");
    ReplicaPlacementInfo r0d1 = new ReplicaPlacementInfo(0, "d1");
    ReplicaPlacementInfo r1d0 = new ReplicaPlacementInfo(1, "d0");
    ReplicaPlacementInfo r1d1 = new ReplicaPlacementInfo(1, "d1");

    List<ExecutionProposal> proposals = Collections.singletonList(new ExecutionProposal(new TopicPartition(TOPIC2, 0),
                                                                                        4,
                                                                                        r0d0,
                                                                                        Arrays.asList(r0d0, r1d1),
                                                                                        Arrays.asList(r1d0, r0d1)));

    TopicPartitionReplica tpr0 = new TopicPartitionReplica(TOPIC2, 0, 0);
    TopicPartitionReplica tpr1 = new TopicPartitionReplica(TOPIC2, 0, 1);

    //Mock adminClient
    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);
    try {
      // Reflectively set constructors from package private to public.
      Constructor<DescribeReplicaLogDirsResult> constructor1 = DescribeReplicaLogDirsResult.class.getDeclaredConstructor(Map.class);
      constructor1.setAccessible(true);
      Constructor<ReplicaLogDirInfo> constructor2 =
          ReplicaLogDirInfo.class.getDeclaredConstructor(String.class, long.class, String.class, long.class);
      constructor2.setAccessible(true);

      Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> futureByReplica = new HashMap<>();
      futureByReplica.put(tpr0, completedFuture(constructor2.newInstance("d0", 0L, null, -1L)));
      futureByReplica.put(tpr1, completedFuture(constructor2.newInstance("d1", 0L, null, -1L)));

      EasyMock.expect(mockAdminClient.describeReplicaLogDirs(anyObject()))
              .andReturn(constructor1.newInstance(futureByReplica))
              .anyTimes();
      EasyMock.replay(mockAdminClient);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      // Let it go.
    }

    Set<PartitionInfo> partitions = new HashSet<>();
    Node[] isrArray = generateExpectedReplicas(proposals.get(0));
    partitions.add(new PartitionInfo(proposals.get(0).topicPartition().topic(),
                                     proposals.get(0).topicPartition().partition(),
                                     isrArray[0], isrArray, isrArray));

    Cluster expectedCluster = new Cluster(null,
                                          _expectedNodes,
                                          partitions,
                                          Collections.<String>emptySet(),
                                          Collections.<String>emptySet());


    ExecutionTaskPlanner planner =
        new ExecutionTaskPlanner(mockAdminClient, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));
    planner.addExecutionProposals(proposals, expectedCluster, null);
    assertEquals(1, planner.remainingLeadershipMovements().size());
    assertEquals(2, planner.remainingIntraBrokerReplicaMovements().size());
    planner.clear();
    assertEquals(0, planner.remainingLeadershipMovements().size());
    assertEquals(0, planner.remainingIntraBrokerReplicaMovements().size());
  }

  @Test
  public void testClear() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(_leaderMovement1);
    proposals.add(_partitionMovement1);
    ExecutionTaskPlanner planner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));

    Set<PartitionInfo> partitions = new HashSet<>();

    partitions.add(generatePartitionInfo(_leaderMovement1, false));
    partitions.add(generatePartitionInfo(_partitionMovement1, false));

    Cluster expectedCluster = new Cluster(null,
                                          _expectedNodes,
                                          partitions,
                                          Collections.<String>emptySet(),
                                          Collections.<String>emptySet());

    planner.addExecutionProposals(proposals, expectedCluster, null);
    assertEquals(2, planner.remainingLeadershipMovements().size());
    assertEquals(2, planner.remainingInterBrokerReplicaMovements().size());
    planner.clear();
    assertEquals(0, planner.remainingLeadershipMovements().size());
    assertEquals(0, planner.remainingInterBrokerReplicaMovements().size());
  }

  private Node[] generateExpectedReplicas(ExecutionProposal proposal) {
    int i = 0;
    Node[] expectedProposalReplicas = new Node[proposal.oldReplicas().size()];
    for (ReplicaPlacementInfo oldId: proposal.oldReplicas()) {
      expectedProposalReplicas[i++] = new Node(oldId.brokerId(), "null", -1);
    }
    return expectedProposalReplicas;
  }

  private PartitionInfo generatePartitionInfo(ExecutionProposal proposal, boolean isPartitionURP) {
    Node[] isrArray = generateExpectedReplicas(proposal);
    return new PartitionInfo(proposal.topicPartition().topic(),
                             proposal.topicPartition().partition(), isrArray[0], isrArray,
                             isPartitionURP ? Arrays.copyOf(isrArray, 1) : isrArray);
  }
}
