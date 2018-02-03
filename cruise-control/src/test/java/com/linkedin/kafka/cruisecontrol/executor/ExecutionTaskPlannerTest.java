/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.Arrays;
import java.util.Collections;
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

  @Test
  public void testGetLeaderMovementTasks() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(leaderMovement1);
    proposals.add(leaderMovement2);
    proposals.add(leaderMovement3);
    proposals.add(leaderMovement4);
    ExecutionTaskPlanner planner = new ExecutionTaskPlanner();
    planner.addExecutionProposals(proposals);
    List<ExecutionTask> leaderMovementTasks = planner.getLeaderMovementTasks(2);
    assertEquals("2 of the leader movements should return in one batch", 2, leaderMovementTasks.size());
    assertEquals(1, leaderMovementTasks.get(0).executionId());
    assertEquals(leaderMovementTasks.get(0).proposal(), leaderMovement1);
    assertEquals(3, leaderMovementTasks.get(1).executionId());
    assertEquals(leaderMovementTasks.get(1).proposal(), leaderMovement2);
    leaderMovementTasks = planner.getLeaderMovementTasks(2);
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
    planner.addExecutionProposals(proposals);
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

  @Test
  public void testClear() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(leaderMovement1);
    proposals.add(partitionMovement1);
    ExecutionTaskPlanner planner = new ExecutionTaskPlanner();
    planner.addExecutionProposals(proposals);
    assertEquals(1, planner.remainingDataToMoveInMB());
    assertEquals(2, planner.remainingLeaderMovements().size());
    assertEquals(2, planner.remainingReplicaMovements().size());
    planner.clear();
    assertEquals(0, planner.remainingDataToMoveInMB());
    assertEquals(0, planner.remainingLeaderMovements().size());
    assertEquals(0, planner.remainingReplicaMovements().size());
  }
}
