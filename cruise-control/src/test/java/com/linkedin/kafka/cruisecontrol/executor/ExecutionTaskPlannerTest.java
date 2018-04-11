/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

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

/**
 * Unit test class for execution task planner
 */
public class ExecutionTaskPlannerTest {
  private static final String TOPIC1 = "topic1";
  private static final String TOPIC2 = "topic2";

  private final ExecutionProposal leaderMovement1 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 0), 0, 1, Arrays.asList(1, 0), Arrays.asList(0, 1));
  private final ExecutionProposal leaderMovement2 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 1), 0, 1, Arrays.asList(1, 0), Arrays.asList(0, 1));
  private final ExecutionProposal leaderMovement3 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 2), 0, 1, Arrays.asList(1, 2), Arrays.asList(2, 1));
  private final ExecutionProposal leaderMovement4 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 3), 0, 3, Arrays.asList(3, 2), Arrays.asList(2, 3));

  private final ExecutionProposal partitionMovement1 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 0), 1, 0, Arrays.asList(0, 2), Arrays.asList(2, 1));
  private final ExecutionProposal partitionMovement2 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 1), 2, 0, Arrays.asList(2, 0), Arrays.asList(1, 2));
  private final ExecutionProposal partitionMovement3 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 2), 3, 2, Arrays.asList(2, 0), Arrays.asList(1, 0));
  private final ExecutionProposal partitionMovement4 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 2), 4, 3, Arrays.asList(3, 0), Arrays.asList(0, 2));

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
    ExecutionTaskPlanner planner = new ExecutionTaskPlanner();

    Set<PartitionInfo> partitions = new HashSet<>();

    Node[] isrArray = generateExpectedReplicas(leaderMovement1);
    partitions.add(new PartitionInfo(leaderMovement1.topicPartition().topic(),
                                     leaderMovement1.topicPartition().partition(),
                                     isrArray[0], isrArray, isrArray));

    isrArray = generateExpectedReplicas(leaderMovement2);
    partitions.add(new PartitionInfo(leaderMovement2.topicPartition().topic(),
                                     leaderMovement2.topicPartition().partition(),
                                     isrArray[0], isrArray, isrArray));

    isrArray = generateExpectedReplicas(leaderMovement3);
    partitions.add(new PartitionInfo(leaderMovement3.topicPartition().topic(),
                                     leaderMovement3.topicPartition().partition(),
                                     isrArray[0], isrArray, isrArray));

    isrArray = generateExpectedReplicas(leaderMovement4);
    partitions.add(new PartitionInfo(leaderMovement4.topicPartition().topic(),
                                     leaderMovement4.topicPartition().partition(),
                                     isrArray[0], isrArray, isrArray));

    Cluster expectedCluster = new Cluster(null,
                                          _expectedNodes,
                                          partitions,
                                          Collections.<String>emptySet(),
                                          Collections.<String>emptySet());

    planner.addExecutionProposals(proposals, expectedCluster);
    List<ExecutionTask> leaderMovementTasks = planner.getLeadershipMovementTasks(2);
    assertEquals("2 of the leader movements should return in one batch", 2, leaderMovementTasks.size());
    assertEquals(1, leaderMovementTasks.get(0).executionId());
    assertEquals(leaderMovementTasks.get(0).proposal(), leaderMovement1);
    assertEquals(3, leaderMovementTasks.get(1).executionId());
    assertEquals(leaderMovementTasks.get(1).proposal(), leaderMovement2);
    leaderMovementTasks = planner.getLeadershipMovementTasks(2);
    assertEquals("2 of the leader movements should return in one batch", 2, leaderMovementTasks.size());
    assertEquals(5, leaderMovementTasks.get(0).executionId());
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
    ExecutionTaskPlanner planner = new ExecutionTaskPlanner();

    Set<PartitionInfo> partitions = new HashSet<>();

    Node[] isrArray = generateExpectedReplicas(partitionMovement1);
    partitions.add(new PartitionInfo(partitionMovement1.topicPartition().topic(),
                                     partitionMovement1.topicPartition().partition(),
                                     isrArray[0], isrArray, isrArray));

    isrArray = generateExpectedReplicas(partitionMovement2);
    partitions.add(new PartitionInfo(partitionMovement2.topicPartition().topic(),
                                     partitionMovement2.topicPartition().partition(),
                                     isrArray[1], isrArray, isrArray));

    isrArray = generateExpectedReplicas(partitionMovement3);
    partitions.add(new PartitionInfo(partitionMovement3.topicPartition().topic(),
                                     partitionMovement3.topicPartition().partition(),
                                     isrArray[0], isrArray, isrArray));

    isrArray = generateExpectedReplicas(partitionMovement4);
    partitions.add(new PartitionInfo(partitionMovement4.topicPartition().topic(),
                                     partitionMovement4.topicPartition().partition(),
                                     isrArray[0], isrArray, isrArray));

    Cluster expectedCluster = new Cluster(null,
                                          _expectedNodes,
                                          partitions,
                                          Collections.<String>emptySet(),
                                          Collections.<String>emptySet());

    planner.addExecutionProposals(proposals, expectedCluster);
    Map<Integer, Integer> readyBrokers = new HashMap<>();
    readyBrokers.put(0, 2);
    readyBrokers.put(1, 2);
    readyBrokers.put(2, 1);
    readyBrokers.put(3, 1);
    List<ExecutionTask> partitionMovementTasks = planner.getReplicaMovementTasks(readyBrokers, Collections.emptySet());
    assertEquals("First task should be partitionMovement1", partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("First task should be partitionMovement4", partitionMovement4, partitionMovementTasks.get(1).proposal());
    assertEquals("First task should be partitionMovement2", partitionMovement2, partitionMovementTasks.get(2).proposal());
  }

  private Node[] generateExpectedReplicas(ExecutionProposal proposal) {
    int i = 0;
    Node[] expectedProposalReplicas = new Node[proposal.oldReplicas().size()];
    for (Integer oldId: proposal.oldReplicas()) {
      expectedProposalReplicas[i++] = new Node(oldId, "null", -1);
    }
    return expectedProposalReplicas;
  }

  private Cluster generateExpectedCluster(ExecutionProposal proposal, TopicPartition tp, boolean isLeaderMove) {
    List<Node> mockProposalReplicas = new ArrayList<>(proposal.oldReplicas().size());
    for (Integer oldId: proposal.oldReplicas()) {
      mockProposalReplicas.add(new Node(oldId, "null", -1));
    }

    Node[] isrArray = new Node[mockProposalReplicas.size()];
    isrArray = mockProposalReplicas.toArray(isrArray);

    Set<PartitionInfo> partitions = new HashSet<>();
    partitions.add(new PartitionInfo(tp.topic(), tp.partition(), mockProposalReplicas.get(isLeaderMove ? 1 : 0), isrArray, isrArray));

    return new Cluster(null, mockProposalReplicas, partitions, Collections.<String>emptySet(), Collections.<String>emptySet());
  }

  @Test
  public void testClear() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(leaderMovement1);
    proposals.add(partitionMovement1);
    ExecutionTaskPlanner planner = new ExecutionTaskPlanner();

    Set<PartitionInfo> partitions = new HashSet<>();

    Node[] isrArray = generateExpectedReplicas(leaderMovement1);
    partitions.add(new PartitionInfo(leaderMovement1.topicPartition().topic(),
                                     leaderMovement1.topicPartition().partition(),
                                     isrArray[0], isrArray, isrArray));

    isrArray = generateExpectedReplicas(partitionMovement1);
    partitions.add(new PartitionInfo(partitionMovement1.topicPartition().topic(),
                                     partitionMovement1.topicPartition().partition(),
                                     isrArray[0], isrArray, isrArray));

    Cluster expectedCluster = new Cluster(null,
                                          _expectedNodes,
                                          partitions,
                                          Collections.<String>emptySet(),
                                          Collections.<String>emptySet());

    planner.addExecutionProposals(proposals, expectedCluster);
    assertEquals(1, planner.remainingDataToMoveInMB());
    assertEquals(2, planner.remainingLeadershipMovements().size());
    assertEquals(2, planner.remainingReplicaMovements().size());
    planner.clear();
    assertEquals(0, planner.remainingDataToMoveInMB());
    assertEquals(0, planner.remainingLeadershipMovements().size());
    assertEquals(0, planner.remainingReplicaMovements().size());
  }
}
