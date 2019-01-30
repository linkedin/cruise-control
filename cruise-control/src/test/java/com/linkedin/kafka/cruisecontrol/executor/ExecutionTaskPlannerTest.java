/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.executor.strategy.BaseReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeLargeReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeSmallReplicaMovementStrategy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC2;

/**
 * Unit test class for execution task planner
 */
public class ExecutionTaskPlannerTest {
  private final ExecutionProposal leaderMovement1 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 0), 0, 1, Arrays.asList(1, 0), Arrays.asList(0, 1));
  private final ExecutionProposal leaderMovement2 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 1), 0, 1, Arrays.asList(1, 0), Arrays.asList(0, 1));
  private final ExecutionProposal leaderMovement3 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 2), 0, 1, Arrays.asList(1, 2), Arrays.asList(2, 1));
  private final ExecutionProposal leaderMovement4 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 3), 0, 3, Arrays.asList(3, 2), Arrays.asList(2, 3));

  private final ExecutionProposal partitionMovement1 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 0), 4, 0, Arrays.asList(0, 2), Arrays.asList(2, 1));
  private final ExecutionProposal partitionMovement2 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 1), 3, 1, Arrays.asList(1, 3), Arrays.asList(3, 2));
  private final ExecutionProposal partitionMovement3 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 2), 2, 2, Arrays.asList(2, 1), Arrays.asList(1, 3));
  private final ExecutionProposal partitionMovement4 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 3), 1, 3, Arrays.asList(3, 2), Arrays.asList(2, 0));

  private final List<Node> _expectedNodes = new ArrayList<>(Arrays.asList(
      new Node(0, "null", -1),
      new Node(1, "null", -1),
      new Node(2, "null", -1),
      new Node(3, "null", -1)));

  @Test
  public void testGetLeaderMovementTasks() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(leaderMovement1);
    proposals.add(leaderMovement2);
    proposals.add(leaderMovement3);
    proposals.add(leaderMovement4);
    ExecutionTaskPlanner planner = new ExecutionTaskPlanner(Collections.emptyList());

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
    assertEquals(leaderMovementTasks.get(0).proposal(), leaderMovement1);
    assertEquals(5, leaderMovementTasks.get(1).executionId());
    assertEquals(leaderMovementTasks.get(1).proposal(), leaderMovement2);
    leaderMovementTasks = planner.getLeadershipMovementTasks(2);
    assertEquals("2 of the leader movements should return in one batch", 2, leaderMovementTasks.size());
    assertEquals(6, leaderMovementTasks.get(0).executionId());
    assertEquals(leaderMovementTasks.get(0).proposal(), leaderMovement3);
    assertEquals(7, leaderMovementTasks.get(1).executionId());
    assertEquals(leaderMovementTasks.get(1).proposal(), leaderMovement4);
  }

  @Test
  public void testGetPartitionMovementTasks() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(partitionMovement1);
    proposals.add(partitionMovement2);
    proposals.add(partitionMovement3);
    proposals.add(partitionMovement4);
    // Test different execution strategies.
    ExecutionTaskPlanner basePlanner = new ExecutionTaskPlanner(null);
    ExecutionTaskPlanner postponeUrpPlanner = new ExecutionTaskPlanner(Collections.singletonList(PostponeUrpReplicaMovementStrategy.class.getName()));
    ExecutionTaskPlanner prioritizeLargeMovementPlanner = new ExecutionTaskPlanner(Arrays.asList(PrioritizeLargeReplicaMovementStrategy.class.getName(),
                                                                                                 BaseReplicaMovementStrategy.class.getName()));
    ExecutionTaskPlanner prioritizeSmallMovementPlanner = new ExecutionTaskPlanner(Arrays.asList(PrioritizeSmallReplicaMovementStrategy.class.getName(),
                                                                                                 BaseReplicaMovementStrategy.class.getName()));

    Set<PartitionInfo> partitions = new HashSet<>();
    partitions.add(generatePartitionInfo(partitionMovement1, true));
    partitions.add(generatePartitionInfo(partitionMovement2, false));
    partitions.add(generatePartitionInfo(partitionMovement3, true));
    partitions.add(generatePartitionInfo(partitionMovement4, false));

    Cluster expectedCluster = new Cluster(null,
                                          _expectedNodes,
                                          partitions,
                                          Collections.<String>emptySet(),
                                          Collections.<String>emptySet());

    Map<Integer, Integer> readyBrokers = new HashMap<>();
    readyBrokers.put(0, 8);
    readyBrokers.put(1, 8);
    readyBrokers.put(2, 8);
    readyBrokers.put(3, 8);
    basePlanner.addExecutionProposals(proposals, expectedCluster, null);
    List<ExecutionTask> partitionMovementTasks = basePlanner.getReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement1", partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement3", partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement4", partitionMovement4, partitionMovementTasks.get(2).proposal());

    postponeUrpPlanner.addExecutionProposals(proposals, expectedCluster, null);
    partitionMovementTasks = postponeUrpPlanner.getReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement4", partitionMovement4, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement2", partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement1", partitionMovement1, partitionMovementTasks.get(2).proposal());

    prioritizeLargeMovementPlanner.addExecutionProposals(proposals, expectedCluster, null);
    partitionMovementTasks = prioritizeLargeMovementPlanner.getReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement1", partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement3", partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement4", partitionMovement4, partitionMovementTasks.get(2).proposal());

    prioritizeSmallMovementPlanner.addExecutionProposals(proposals, expectedCluster, null);
    partitionMovementTasks = prioritizeSmallMovementPlanner.getReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement4", partitionMovement4, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement2", partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement1", partitionMovement1, partitionMovementTasks.get(2).proposal());
  }

  @Test
  public void testDynamicConfigReplicaMovementStrategy() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(partitionMovement1);
    proposals.add(partitionMovement2);
    proposals.add(partitionMovement3);
    proposals.add(partitionMovement4);
    ExecutionTaskPlanner planner = new ExecutionTaskPlanner(null);

    Set<PartitionInfo> partitions = new HashSet<>();
    partitions.add(generatePartitionInfo(partitionMovement1, true));
    partitions.add(generatePartitionInfo(partitionMovement2, false));
    partitions.add(generatePartitionInfo(partitionMovement3, true));
    partitions.add(generatePartitionInfo(partitionMovement4, false));

    Cluster expectedCluster = new Cluster(null,
                                          _expectedNodes,
                                          partitions,
                                          Collections.<String>emptySet(),
                                          Collections.<String>emptySet());

    Map<Integer, Integer> readyBrokers = new HashMap<>();
    readyBrokers.put(0, 8);
    readyBrokers.put(1, 8);
    readyBrokers.put(2, 8);
    readyBrokers.put(3, 8);
    planner.addExecutionProposals(proposals, expectedCluster, null);
    List<ExecutionTask> partitionMovementTasks = planner.getReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement1", partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement3", partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement4", partitionMovement4, partitionMovementTasks.get(2).proposal());

    planner.addExecutionProposals(proposals, expectedCluster, new PostponeUrpReplicaMovementStrategy());
    partitionMovementTasks = planner.getReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement4", partitionMovement4, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement2", partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement1", partitionMovement1, partitionMovementTasks.get(2).proposal());

    planner.addExecutionProposals(proposals, expectedCluster, new PrioritizeLargeReplicaMovementStrategy());
    partitionMovementTasks = planner.getReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement1", partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement3", partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement4", partitionMovement4, partitionMovementTasks.get(2).proposal());

    planner.addExecutionProposals(proposals, expectedCluster, new PrioritizeSmallReplicaMovementStrategy());
    partitionMovementTasks = planner.getReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement4", partitionMovement4, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task should be partitionMovement2", partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task should be partitionMovement1", partitionMovement1, partitionMovementTasks.get(2).proposal());
  }

  @Test
  public void testClear() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(leaderMovement1);
    proposals.add(partitionMovement1);
    ExecutionTaskPlanner planner = new ExecutionTaskPlanner(Collections.emptyList());

    Set<PartitionInfo> partitions = new HashSet<>();

    partitions.add(generatePartitionInfo(leaderMovement1, false));
    partitions.add(generatePartitionInfo(partitionMovement1, false));

    Cluster expectedCluster = new Cluster(null,
                                          _expectedNodes,
                                          partitions,
                                          Collections.<String>emptySet(),
                                          Collections.<String>emptySet());

    planner.addExecutionProposals(proposals, expectedCluster, null);
    assertEquals(4, planner.remainingDataToMoveInMB());
    assertEquals(2, planner.remainingLeadershipMovements().size());
    assertEquals(2, planner.remainingReplicaMovements().size());
    planner.clear();
    assertEquals(0, planner.remainingDataToMoveInMB());
    assertEquals(0, planner.remainingLeadershipMovements().size());
    assertEquals(0, planner.remainingReplicaMovements().size());
  }

  private Node[] generateExpectedReplicas(ExecutionProposal proposal) {
    int i = 0;
    Node[] expectedProposalReplicas = new Node[proposal.oldReplicas().size()];
    for (Integer oldId: proposal.oldReplicas()) {
      expectedProposalReplicas[i++] = new Node(oldId, "null", -1);
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
