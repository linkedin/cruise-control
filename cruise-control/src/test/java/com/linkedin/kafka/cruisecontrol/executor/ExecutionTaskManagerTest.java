/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.State.*;
import static org.junit.Assert.*;


public class ExecutionTaskManagerTest {

  private Cluster generateExpectedCluster(ExecutionProposal proposal, TopicPartition tp) {
    List<Node> expectedReplicas = new ArrayList<>(proposal.oldReplicas().size());
    expectedReplicas.add(new Node(0, "null", -1));
    expectedReplicas.add(new Node(2, "null", -1));

    Node[] isrArray = new Node[expectedReplicas.size()];
    isrArray = expectedReplicas.toArray(isrArray);

    Set<PartitionInfo> partitions = new HashSet<>();
    partitions.add(new PartitionInfo(tp.topic(), tp.partition(), expectedReplicas.get(1), isrArray, isrArray));

    return new Cluster(null, expectedReplicas, partitions, Collections.emptySet(), Collections.emptySet());
  }

  @Test
  public void testStateChangeSequences() {
    TopicPartition tp = new TopicPartition("topic", 0);
    ExecutionTaskManager taskManager = new ExecutionTaskManager(1, 1,
                                                                null, new MetricRegistry(), new SystemTime());

    List<List<ExecutionTask.State>> testSequences = new ArrayList<>();
    // Completed successfully.
    testSequences.add(Arrays.asList(IN_PROGRESS, COMPLETED));
    // Rollback succeeded.
    testSequences.add(Arrays.asList(IN_PROGRESS, ABORTING, ABORTED));
    // Rollback failed.
    testSequences.add(Arrays.asList(IN_PROGRESS, ABORTING, DEAD));
    // Cannot rollback.
    testSequences.add(Arrays.asList(IN_PROGRESS, DEAD));

    for (List<ExecutionTask.State> sequence : testSequences) {
      taskManager.clear();
      // Make sure the proposal does not involve leader movement.
      ExecutionProposal proposal =
          new ExecutionProposal(tp, 0, 2, Arrays.asList(0, 2), Arrays.asList(2, 1));

      taskManager.setExecutionModeForTaskTracker(false);
      taskManager.addExecutionProposals(Collections.singletonList(proposal),
                                        Collections.emptySet(),
                                        generateExpectedCluster(proposal, tp),
                                        null);
      taskManager.setRequestedPartitionMovementConcurrency(null);
      taskManager.setRequestedLeadershipMovementConcurrency(null);
      List<ExecutionTask> tasks = taskManager.getReplicaMovementTasks();
      assertEquals(1, tasks.size());
      ExecutionTask task  = tasks.get(0);
      verifyStateChangeSequence(sequence, task, taskManager);
    }
  }

  private void verifyStateChangeSequence(List<ExecutionTask.State> stateSequence,
                                         ExecutionTask task,
                                         ExecutionTaskManager taskManager) {
    stateSequence.forEach(s -> changeTaskState(s, task, taskManager));
  }

  private void changeTaskState(ExecutionTask.State state, ExecutionTask task, ExecutionTaskManager taskManager) {
    Map<ExecutionTask.State, Integer> taskStat;
    switch (state) {
      case IN_PROGRESS:
        taskManager.markTasksInProgress(Collections.singletonList(task));
        taskStat = taskManager.getExecutionTasksSummary(Collections.emptyList(), false).taskStat()
                              .get(ExecutionTask.TaskType.REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(PENDING));
        assertEquals(1, (int) taskStat.get(IN_PROGRESS));
        assertEquals(0, (int) taskStat.get(ABORTING));
        assertEquals(0, (int) taskStat.get(ABORTED));
        assertEquals(0, (int) taskStat.get(COMPLETED));
        assertEquals(0, (int) taskStat.get(DEAD));
        break;
      case ABORTING:
        taskManager.markTaskAborting(task);
        taskStat = taskManager.getExecutionTasksSummary(Collections.emptyList(), false).taskStat()
                              .get(ExecutionTask.TaskType.REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(PENDING));
        assertEquals(0, (int) taskStat.get(IN_PROGRESS));
        assertEquals(1, (int) taskStat.get(ABORTING));
        assertEquals(0, (int) taskStat.get(ABORTED));
        assertEquals(0, (int) taskStat.get(COMPLETED));
        assertEquals(0, (int) taskStat.get(DEAD));
        break;
      case DEAD:
        taskManager.markTaskDead(task);
        taskStat = taskManager.getExecutionTasksSummary(Collections.emptyList(), false).taskStat()
                              .get(ExecutionTask.TaskType.REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(PENDING));
        assertEquals(0, (int) taskStat.get(IN_PROGRESS));
        assertEquals(0, (int) taskStat.get(ABORTING));
        assertEquals(0, (int) taskStat.get(ABORTED));
        assertEquals(0, (int) taskStat.get(COMPLETED));
        assertEquals(1, (int) taskStat.get(DEAD));
        break;
      case ABORTED:
      case COMPLETED:
        ExecutionTask.State origState = task.state();
        taskManager.markTaskDone(task);
        taskStat = taskManager.getExecutionTasksSummary(Collections.emptyList(), false).taskStat()
                              .get(ExecutionTask.TaskType.REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(PENDING));
        assertEquals(0, (int) taskStat.get(IN_PROGRESS));
        assertEquals(0, (int) taskStat.get(ABORTING));
        assertEquals(origState == ExecutionTask.State.ABORTING ? 1 : 0, (int) taskStat.get(ABORTED));
        assertEquals(origState == ExecutionTask.State.ABORTING ? 0 : 1, (int) taskStat.get(COMPLETED));
        assertEquals(0, (int) taskStat.get(DEAD));
        break;
      default:
        throw new IllegalArgumentException("Invalid state " + state);
    }
    assertEquals(state, task.state());
  }
}
