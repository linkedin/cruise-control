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
    ExecutionTaskManager taskManager = new ExecutionTaskManager(1, 1, new MetricRegistry(), new SystemTime());

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
                                        generateExpectedCluster(proposal, tp));
      taskManager.setRequestedMovementConcurrency(null, null);
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
    switch (state) {
      case IN_PROGRESS:
        taskManager.markTasksInProgress(Collections.singletonList(task));
        assertEquals(0, taskManager.remainingPartitionMovements().size());
        assertEquals(0, taskManager.remainingLeadershipMovements().size());
        assertEquals(1, taskManager.inProgressTasks().size());
        assertEquals(0, taskManager.abortingTasks().size());
        assertEquals(0, taskManager.abortedTasks().size());
        assertEquals(0, taskManager.deadTasks().size());
        break;
      case ABORTING:
        taskManager.markTaskAborting(task);
        assertEquals(0, taskManager.remainingPartitionMovements().size());
        assertEquals(0, taskManager.remainingLeadershipMovements().size());
        assertEquals(0, taskManager.inProgressTasks().size());
        assertEquals(1, taskManager.abortingTasks().size());
        assertEquals(0, taskManager.abortedTasks().size());
        assertEquals(0, taskManager.deadTasks().size());
        break;
      case DEAD:
        taskManager.markTaskDead(task);
        assertEquals(0, taskManager.remainingPartitionMovements().size());
        assertEquals(0, taskManager.remainingLeadershipMovements().size());
        assertEquals(0, taskManager.inProgressTasks().size());
        assertEquals(0, taskManager.abortingTasks().size());
        assertEquals(0, taskManager.abortedTasks().size());
        assertEquals(1, taskManager.deadTasks().size());
        break;
      case ABORTED:
      case COMPLETED:
        ExecutionTask.State origState = task.state();
        taskManager.markTaskDone(task);
        assertEquals(0, taskManager.remainingPartitionMovements().size());
        assertEquals(0, taskManager.remainingLeadershipMovements().size());
        assertEquals(0, taskManager.inProgressTasks().size());
        assertEquals(0, taskManager.abortingTasks().size());
        assertEquals(origState == ExecutionTask.State.ABORTING ? 1 : 0, taskManager.abortedTasks().size());
        assertEquals(0, taskManager.deadTasks().size());
        break;
      default:
        throw new IllegalArgumentException("Invalid state " + state);
    }
    assertEquals(state, task.state());
  }
}
